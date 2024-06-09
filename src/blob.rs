use std::sync::atomic::AtomicUsize;

use brioche_core::blob::BlobHash;
use eyre::WrapErr as _;
use futures::{StreamExt as _, TryStreamExt as _};

use crate::server::{ServerError, ServerState};

#[derive(Debug, Clone, Copy)]
pub enum CompressionScheme {
    Zstd,
}

/// The key used to store a blob in the object store.
///
/// Currently, this key is `blobs/{hash[0..4]}/{hash[4..]}`. This
/// structure might be overkill, but there are some good reasons for it:
///
/// - AWS S3 rate limits are based on the object prefix, so breaking up
///   blobs into more prefixes should lead to higher potential throughput.
/// - Some filesystems have performance issues around directories with
///   lots of files in a flat structure, so using multiple subdirectories
///   can help with that. Git puts blobs into directories named by the first
///   2 digits of their hashes, for example.
/// - This is a hard thing to change, so it's better to try something that
///   could stand up longer-term.
fn blob_key(blob_hash: BlobHash, compression: CompressionScheme) -> String {
    let extension = match compression {
        CompressionScheme::Zstd => ".zst",
    };

    let blob_hash_key = blob_hash.to_string();
    let (prefix, suffix) = blob_hash_key.split_at(4);
    format!("blobs/{prefix}/{suffix}{extension}")
}

pub async fn try_get_as_http_response(
    state: &ServerState,
    blob_hash: BlobHash,
    compression: CompressionScheme,
) -> eyre::Result<Option<axum::response::Response>> {
    let response = state
        .object_store
        .try_get_as_http_response(&blob_key(blob_hash, compression))
        .await?;
    Ok(response)
}

pub async fn blob_exists(
    state: &ServerState,
    blob_hash: BlobHash,
    compression: CompressionScheme,
) -> Result<bool, ServerError> {
    let mut db_transaction = state.db_pool.begin().await.map_err(ServerError::other)?;

    let blob_hash_value = blob_hash.to_string();
    let object_store_url_value = state.env.object_store_url.to_string();
    let object_store_key_value = blob_key(blob_hash, compression);
    let result = sqlx::query!(
        r#"
            SELECT count(*) AS count
            FROM blobs
            WHERE blob_hash = $1 AND object_store_url = $2 AND object_store_key = $3
        "#,
        blob_hash_value,
        object_store_url_value,
        object_store_key_value,
    )
    .fetch_one(&mut *db_transaction)
    .await
    .map_err(ServerError::other)?;

    db_transaction.commit().await.map_err(ServerError::other)?;

    Ok(result.count.unwrap_or(0) > 0)
}

pub async fn upload_blob<E>(
    state: &ServerState,
    blob_hash: BlobHash,
    compression: CompressionScheme,
    content: impl futures::Stream<Item = Result<bytes::Bytes, E>>,
) -> Result<(), ServerError>
where
    eyre::Error: From<E>,
{
    // Get thee object store key for the blob
    let blob_key = blob_key(blob_hash, compression);

    // Keep track of the total size of the blob before compression
    let uncompressed_blob_size = AtomicUsize::new(0);
    let content = content.inspect_ok(|chunk| {
        uncompressed_blob_size.fetch_add(chunk.len(), std::sync::atomic::Ordering::Relaxed);
    });

    // Validate the hash of the uncompressed blob
    let hasher = std::sync::Arc::new(std::sync::Mutex::new(blake3::Hasher::new()));
    let content = content
        .map_err(eyre::Error::from)
        .inspect_ok({
            let hasher = hasher.clone();
            move |chunk| {
                let mut hasher = hasher.lock().unwrap();
                hasher.update(chunk);
            }
        })
        .chain(futures::stream::once(async move {
            let expected_hash = blob_hash.to_blake3();
            let actual_hash = hasher.lock().unwrap().finalize();
            if expected_hash != actual_hash {
                return Err(eyre::eyre!(
                    "expected blob hash to be {expected_hash}, but got {actual_hash}"
                ));
            }

            eyre::Ok(bytes::Bytes::new())
        }));

    // Compress the blob
    let content = content.map_err(std::io::Error::other);
    let content_reader = tokio_util::io::StreamReader::new(content);
    let content_reader = tokio::io::BufReader::new(content_reader);
    let content_reader = async_compression::tokio::bufread::ZstdEncoder::new(content_reader);

    let content_stream = tokio_util::io::ReaderStream::new(content_reader);

    // Upload the blob, getting the total size of the blob after compression
    let compressed_blob_size = state
        .object_store
        .put::<std::io::Error>(&blob_key, content_stream)
        .await?;

    // Get the final total size of the blob after compression
    let uncompressed_blob_size = uncompressed_blob_size.load(std::sync::atomic::Ordering::SeqCst);

    // Record the uploaded blob in the database
    {
        let mut db_transaction = state.db_pool.begin().await.map_err(ServerError::other)?;

        let blob_hash_value = blob_hash.to_string();
        let object_store_url_value = state.env.object_store_url.to_string();
        let object_store_key_value = blob_key.clone();
        let uncompressed_blob_size_value: i64 = uncompressed_blob_size
            .try_into()
            .wrap_err("blob too big")
            .map_err(ServerError::other)?;
        let compressed_blob_size_value: i64 = compressed_blob_size
            .try_into()
            .wrap_err("blob too big")
            .map_err(ServerError::other)?;
        let compression_value = match compression {
            CompressionScheme::Zstd => "zstd",
        };

        let replaced_records = sqlx::query!(
            r#"
                DELETE FROM blobs
                WHERE blob_hash = $1
                RETURNING blob_hash, object_store_url, object_store_key
            "#,
            blob_hash_value,
        )
        .fetch_all(&mut *db_transaction)
        .await
        .map_err(ServerError::other)?;

        sqlx::query!(
            r#"
                INSERT INTO blobs (
                    blob_hash,
                    object_store_url,
                    object_store_key,
                    compression,
                    compressed_object_size,
                    blob_size
                ) VALUES (
                    $1,
                    $2,
                    $3,
                    $4,
                    $5,
                    $6
                )
            "#,
            blob_hash_value,
            object_store_url_value,
            object_store_key_value,
            compression_value,
            compressed_blob_size_value,
            uncompressed_blob_size_value,
        )
        .execute(&mut *db_transaction)
        .await
        .map_err(ServerError::other)?;

        if !replaced_records.is_empty() {
            tracing::warn!(%blob_hash, ?replaced_records, "replaced existing blob already recorded in database");
        }

        db_transaction.commit().await.map_err(ServerError::other)?;
    }

    Ok(())
}
