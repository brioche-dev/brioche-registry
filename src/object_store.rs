use axum::response::IntoResponse as _;
use eyre::{Context as _, ContextCompat, OptionExt as _};
use futures::{StreamExt as _, TryStreamExt as _};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

#[derive(Debug, Clone)]
pub enum ObjectStore {
    S3 {
        client: aws_sdk_s3::Client,
        bucket: String,
        prefix: String,
    },
    Filesystem {
        path: std::path::PathBuf,
    },
}

impl ObjectStore {
    pub async fn from_url(url: &url::Url) -> eyre::Result<Self> {
        match url.scheme() {
            "s3" => {
                let bucket = url.host_str().wrap_err("no bucket specified in URL")?;
                let prefix_path = url.path().trim_start_matches('/').to_string();
                let prefix = if prefix_path.is_empty() {
                    "".to_string()
                } else {
                    format!("{}/", prefix_path)
                };
                let aws_config =
                    aws_config::load_defaults(aws_config::BehaviorVersion::v2023_11_09()).await;
                let client = aws_sdk_s3::Client::new(&aws_config);
                Ok(Self::S3 {
                    client,
                    bucket: bucket.to_string(),
                    prefix,
                })
            }
            "file" => {
                let path = url
                    .to_file_path()
                    .map_err(|_| eyre::eyre!("invalid file URL"))?;
                Ok(Self::Filesystem { path })
            }
            "relative-file" => {
                let relative_path = url.path();
                let relative_path = relative_path.strip_prefix('/').unwrap_or(relative_path);
                let abs_path = tokio::fs::canonicalize(std::path::Path::new(relative_path))
                    .await
                    .with_context(|| {
                        format!(
                            "failed to canonicalize relative object store path: {relative_path}"
                        )
                    })?;

                Ok(Self::Filesystem { path: abs_path })
            }
            scheme => {
                eyre::bail!("unsupported scheme {scheme} for object store URL {url}");
            }
        }
    }

    pub async fn exists(&self, key: &str) -> eyre::Result<bool> {
        match self {
            ObjectStore::S3 {
                client,
                bucket,
                prefix,
            } => {
                let object_key = format!("{prefix}{key}");
                let response = client
                    .head_object()
                    .bucket(bucket)
                    .key(&object_key)
                    .send()
                    .await;
                match response {
                    Ok(_) => Ok(true),
                    Err(aws_sdk_s3::error::SdkError::ServiceError(context))
                        if matches!(
                            context.err(),
                            aws_sdk_s3::operation::head_object::HeadObjectError::NotFound(_)
                        ) =>
                    {
                        Ok(false)
                    }
                    Err(error) => Err(error.into()),
                }
            }
            ObjectStore::Filesystem { path } => {
                let object_path = path.join(key);
                Ok(tokio::fs::try_exists(object_path).await?)
            }
        }
    }

    pub async fn get_as_http_response(&self, key: &str) -> eyre::Result<axum::response::Response> {
        match self {
            ObjectStore::S3 {
                client,
                bucket,
                prefix,
            } => {
                let object_key = format!("{prefix}{key}");

                let presigning_config = aws_sdk_s3::presigning::PresigningConfig::builder()
                    .expires_in(std::time::Duration::from_secs(60 * 60))
                    .build()?;
                let presigned_request = client
                    .get_object()
                    .bucket(bucket)
                    .key(&object_key)
                    .presigned(presigning_config)
                    .await?;

                eyre::ensure!(
                    presigned_request.method().to_ascii_lowercase() == "get",
                    "presigned URL has unexpected method {}",
                    presigned_request.method()
                );
                eyre::ensure!(
                    presigned_request.headers().count() == 0,
                    "presigned request has extra required headers",
                );

                let response = axum::response::Redirect::to(presigned_request.uri());
                Ok(response.into_response())
            }
            ObjectStore::Filesystem { path } => {
                let object_path = path.join(key);
                let file = tokio::fs::File::open(&object_path).await?;
                let file_stream = tokio_util::io::ReaderStream::new(file);
                Ok(axum::response::Response::new(
                    axum::body::Body::from_stream(file_stream),
                ))
            }
        }
    }

    pub async fn put_and_validate<E>(
        &self,
        key: &str,
        input: impl futures::Stream<Item = Result<bytes::Bytes, E>>,
        expected_hash: blake3::Hash,
    ) -> eyre::Result<()>
    where
        eyre::Error: From<E>,
    {
        match self {
            ObjectStore::S3 {
                client,
                bucket,
                prefix,
            } => {
                let object_key = format!("{prefix}{key}");

                let input = std::pin::pin!(input);
                let upload_type = s3_upload_type(input).await?;

                match upload_type {
                    S3UploadType::Single(bytes) => {
                        upload_s3_single_part(client, bucket, &object_key, bytes, expected_hash)
                            .await?;
                    }
                    S3UploadType::Multipart(stream) => {
                        upload_s3_multipart(client, bucket, &object_key, stream, expected_hash)
                            .await?;
                    }
                }

                Ok(())
            }
            ObjectStore::Filesystem { path } => {
                let object_path = path.join(key);
                let parent_path = object_path.parent().ok_or_eyre("no parent path")?;

                // Ensure the destination path exists
                tokio::fs::create_dir_all(&parent_path).await?;

                // Create a temporary file. It's created in the same directory
                // as the final file to ensure we can rename it without
                // copying across filesystems.
                let temp_path = parent_path.join(format!("._tmp_{}", ulid::Ulid::new()));

                let temp_file = tokio::fs::File::create(&temp_path).await?;
                let mut buf_writer = tokio::io::BufWriter::new(temp_file);

                let buf_writer = std::pin::pin!(&mut buf_writer);

                let result = write_file_and_validate_hash(buf_writer, input, expected_hash).await;

                match result {
                    Ok(()) => {
                        // Move the file to its final location if the file
                        // was written and validated
                        tokio::fs::rename(&temp_path, &object_path).await?;
                    }
                    Err(error) => {
                        // Remove the temp file if there was an error
                        let _ = tokio::fs::remove_file(&temp_path).await.inspect_err(|error| {
                            tracing::warn!(temp_path = %temp_path.display(), %error, "failed to remove temporary file");
                        });

                        return Err(error);
                    }
                }

                Ok(())
            }
        }
    }
}

pub async fn write_file_and_validate_hash<W, E>(
    mut writer: std::pin::Pin<&mut W>,
    input: impl futures::Stream<Item = Result<bytes::Bytes, E>>,
    expected_hash: blake3::Hash,
) -> eyre::Result<()>
where
    W: tokio::io::AsyncWrite + Send,
    eyre::Error: From<E>,
{
    let mut hasher = blake3::Hasher::new();

    // Hash and write each set of bytes from the input stream
    let mut input = std::pin::pin!(input);
    while let Some(bytes) = input.try_next().await? {
        hasher.update(&bytes[..]);
        writer.write_all(&bytes[..]).await?;
    }

    // Flush and shutdown the writer to ensure the bytes get written
    writer.flush().await?;
    writer.shutdown().await?;

    // Validate the hashes match
    let actual_hash = hasher.finalize();
    if actual_hash != expected_hash {
        eyre::bail!("hash mismatch: expected {expected_hash}, got {actual_hash}");
    }

    Ok(())
}

enum S3UploadType<S> {
    Single(bytes::Bytes),
    Multipart(S),
}

/// The minimum size for each part of a multipart upload. If the total
/// request size is small enough to fit within one part, we'll use a normal
/// upload instead of a multipart upload.
const MIN_UPLOAD_PART_SIZE: usize = 10 * 1024 * 1024;

async fn s3_upload_type<E>(
    mut input: impl futures::Stream<Item = Result<bytes::Bytes, E>> + Unpin,
) -> eyre::Result<S3UploadType<impl futures::Stream<Item = Result<bytes::Bytes, E>>>>
where
    eyre::Error: From<E>,
{
    let mut buffer = bytes::BytesMut::new();

    while let Some(chunk) = input.try_next().await? {
        buffer.extend(chunk);

        if buffer.len() > MIN_UPLOAD_PART_SIZE {
            break;
        }
    }

    if buffer.len() > MIN_UPLOAD_PART_SIZE {
        let stream = futures::stream::once(async move { Ok(buffer.freeze()) }).chain(input);
        Ok(S3UploadType::Multipart(stream))
    } else {
        Ok(S3UploadType::Single(buffer.freeze()))
    }
}

async fn upload_s3_single_part(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    bytes: bytes::Bytes,
    expected_hash: blake3::Hash,
) -> eyre::Result<()> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&bytes[..]);
    let actual_hash = hasher.finalize();
    if actual_hash != expected_hash {
        eyre::bail!("hash mismatch: expected {expected_hash}, got {actual_hash}");
    }

    let body = http_body_util::Full::new(bytes);
    let body = aws_sdk_s3::primitives::ByteStream::from_body_1_x(body);
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .send()
        .await?;

    Ok(())
}

async fn upload_s3_multipart<E>(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    input: impl futures::Stream<Item = Result<bytes::Bytes, E>>,
    expected_hash: blake3::Hash,
) -> eyre::Result<()>
where
    eyre::Error: From<E>,
{
    let multipart_response = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;
    let multipart_upload_id = multipart_response
        .upload_id
        .ok_or_eyre("no multipart upload ID returned")?;

    let result = upload_s3_parts_and_validate_hash(
        client,
        bucket,
        key,
        &multipart_upload_id,
        input,
        expected_hash,
    )
    .await;

    match result {
        Ok(parts) => {
            // Complete the multipart upload if it was successful
            client
                .complete_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(&multipart_upload_id)
                .multipart_upload(
                    aws_sdk_s3::types::CompletedMultipartUpload::builder()
                        .set_parts(Some(parts))
                        .build(),
                )
                .send()
                .await
                .wrap_err("failed to complete multipart upload")?;
        }
        Err(error) => {
            // Abort the multipart upload if there was an error
            let _ = client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(&multipart_upload_id)
                .send()
                .await
                .inspect_err(|error| tracing::warn!(%error, "failed to abort multipart upload"));

            return Err(error);
        }
    }

    Ok(())
}

async fn upload_s3_parts_and_validate_hash<E>(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    input: impl futures::Stream<Item = Result<bytes::Bytes, E>>,
    expected_hash: blake3::Hash,
) -> eyre::Result<Vec<aws_sdk_s3::types::CompletedPart>>
where
    eyre::Error: From<E>,
{
    // Create a reader for the input stream
    let input = input.map_err(|err| std::io::Error::other(eyre::Error::from(err)));
    let input_reader = tokio_util::io::StreamReader::new(input);
    let mut input_reader = std::pin::pin!(input_reader);

    let mut hasher = blake3::Hasher::new();
    let mut parts = vec![];

    let mut buffer = vec![0; MIN_UPLOAD_PART_SIZE];
    for part_number in 1.. {
        // Read a chunk from the receiver, up to the buffer size
        let chunk_len = read_chunk(&mut input_reader, &mut buffer).await?;
        let chunk_bytes = &buffer[..chunk_len];

        // A length of 0 means we've reached the end
        if chunk_len == 0 {
            break;
        }

        // Hash the chunk
        hasher.update(chunk_bytes);

        // Upload the chunk
        let chunk_body = http_body_util::Full::new(bytes::Bytes::copy_from_slice(chunk_bytes));
        let chunk_body = aws_sdk_s3::primitives::ByteStream::from_body_1_x(chunk_body);
        let response = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(chunk_body)
            .send()
            .await?;

        parts.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(part_number)
                .set_e_tag(response.e_tag)
                .build(),
        );
    }

    // Validate the hashes match
    let actual_hash = hasher.finalize();
    if actual_hash != expected_hash {
        eyre::bail!("hash mismatch: expected {expected_hash}, got {actual_hash}");
    }

    Ok(parts)
}

/// Repeatedly read from the reader to fill the buffer, returning the
/// number of bytes read. This function is like `.read_exact()`, except
/// reaching end-of-file will return `Ok` with the number of bytes successfully read.
async fn read_chunk(
    mut reader: impl tokio::io::AsyncRead + Unpin,
    mut buffer: &mut [u8],
) -> std::io::Result<usize> {
    let mut total_length = 0;

    loop {
        // If there's no more room in the buffer, return
        if buffer.is_empty() {
            return Ok(total_length);
        }

        // Read a part into the buffer
        let length = reader.read(buffer).await?;

        // If we read 0 bytes, then the reader finished and we can return
        if length == 0 {
            return Ok(total_length);
        }

        // Advance the buffer past the part we just read
        (_, buffer) = buffer.split_at_mut(length);
        total_length += length;
    }
}

#[cfg(test)]
mod tests {
    use futures::stream::TryStreamExt as _;
    use tokio::io::AsyncReadExt as _;

    #[tokio::test]
    async fn test_s3_upload_type_small() {
        let input = futures::stream::iter([eyre::Ok(bytes::Bytes::from("hello"))]);
        let input = std::pin::pin!(input);
        let upload_type = super::s3_upload_type(input).await.unwrap();

        let super::S3UploadType::Single(bytes) = upload_type else {
            panic!("expected single upload type");
        };

        assert_eq!(bytes, bytes::Bytes::from("hello"));
    }

    #[tokio::test]
    async fn test_s3_upload_type_small_with_chunks() {
        let input = futures::stream::iter([
            eyre::Ok(bytes::Bytes::from("hello")),
            eyre::Ok(bytes::Bytes::from("world")),
        ]);
        let input = std::pin::pin!(input);
        let upload_type = super::s3_upload_type(input).await.unwrap();

        let super::S3UploadType::Single(bytes) = upload_type else {
            panic!("expected single upload type");
        };

        assert_eq!(bytes, bytes::Bytes::from("helloworld"));
    }

    #[tokio::test]
    async fn test_s3_upload_type_large() {
        let content = "hello".repeat(1024 * 1024 * 5);
        let input = futures::stream::iter([eyre::Ok(bytes::Bytes::from(content.clone()))]);
        let input = std::pin::pin!(input);
        let upload_type = super::s3_upload_type(input).await.unwrap();

        let super::S3UploadType::Multipart(stream) = upload_type else {
            panic!("expected multipart upload type");
        };

        let stream = stream.map_err(std::io::Error::other);
        let reader = tokio_util::io::StreamReader::new(stream);
        let mut reader = std::pin::pin!(reader);
        let mut result = String::new();
        reader.read_to_string(&mut result).await.unwrap();

        assert_eq!(result, content);
    }

    #[tokio::test]
    async fn test_s3_upload_type_large_with_chunks() {
        let content = "hello".repeat(1024 * 1024 * 5);
        let input_chunks = std::iter::repeat(bytes::Bytes::from("hello"))
            .map(eyre::Ok)
            .take(1024 * 1024 * 5);
        let input = futures::stream::iter(input_chunks);
        let input = std::pin::pin!(input);
        let upload_type = super::s3_upload_type(input).await.unwrap();

        let super::S3UploadType::Multipart(stream) = upload_type else {
            panic!("expected multipart upload type");
        };

        let stream = stream.map_err(std::io::Error::other);
        let reader = tokio_util::io::StreamReader::new(stream);
        let mut reader = std::pin::pin!(reader);
        let mut result = String::new();
        reader.read_to_string(&mut result).await.unwrap();

        assert_eq!(result, content);
    }

    #[tokio::test]
    async fn test_read_chunk_small_input_large_buffer() {
        let input = futures::stream::iter([Ok::<_, std::io::Error>(bytes::Bytes::from("hello"))]);
        let input = std::pin::pin!(input);
        let mut buffer = [0; 1024];

        let reader = tokio_util::io::StreamReader::new(input);
        let mut reader = std::pin::pin!(reader);
        let length = super::read_chunk(&mut reader, &mut buffer).await.unwrap();
        let result = &buffer[..length];

        assert_eq!(result, b"hello");
    }

    #[tokio::test]
    async fn test_read_chunk_small_input_small_buffer() {
        let input = futures::stream::iter([Ok::<_, std::io::Error>(bytes::Bytes::from("hello"))]);
        let input = std::pin::pin!(input);
        let mut buffer = [0; 3];

        let reader = tokio_util::io::StreamReader::new(input);
        let mut reader = std::pin::pin!(reader);

        let length = super::read_chunk(&mut reader, &mut buffer).await.unwrap();
        let result_1 = buffer[..length].to_vec();

        let length = super::read_chunk(&mut reader, &mut buffer).await.unwrap();
        let result_2 = buffer[..length].to_vec();

        assert_eq!(result_1, b"hel");
        assert_eq!(result_2, b"lo");
    }

    #[tokio::test]
    async fn test_read_chunk_big_input_small_buffer() {
        let input = futures::stream::iter([
            Ok::<_, std::io::Error>(bytes::Bytes::from("hello")),
            Ok(bytes::Bytes::from("world")),
        ]);
        let input = std::pin::pin!(input);
        let mut buffer = [0; 3];

        let reader = tokio_util::io::StreamReader::new(input);
        let mut reader = std::pin::pin!(reader);

        let length = super::read_chunk(&mut reader, &mut buffer).await.unwrap();
        let result_1 = buffer[..length].to_vec();

        let length = super::read_chunk(&mut reader, &mut buffer).await.unwrap();
        let result_2 = buffer[..length].to_vec();

        let length = super::read_chunk(&mut reader, &mut buffer).await.unwrap();
        let result_3 = buffer[..length].to_vec();

        let length = super::read_chunk(&mut reader, &mut buffer).await.unwrap();
        let result_4 = buffer[..length].to_vec();

        assert_eq!(result_1, b"hel");
        assert_eq!(result_2, b"low");
        assert_eq!(result_3, b"orl");
        assert_eq!(result_4, b"d");
    }

    #[tokio::test]
    async fn test_read_chunk_big_input_big_buffer() {
        let input = futures::stream::iter([
            Ok::<_, std::io::Error>(bytes::Bytes::from("hello")),
            Ok(bytes::Bytes::from("world")),
        ]);
        let input = std::pin::pin!(input);
        let mut buffer = [0; 1024];

        let reader = tokio_util::io::StreamReader::new(input);
        let mut reader = std::pin::pin!(reader);

        let length = super::read_chunk(&mut reader, &mut buffer).await.unwrap();
        let result = &buffer[..length];

        assert_eq!(result, b"helloworld");
    }
}
