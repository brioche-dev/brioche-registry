use brioche::blob::BlobHash;
use eyre::WrapErr as _;

use crate::server::{ServerError, ServerState};

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
fn blob_key(blob_hash: BlobHash) -> String {
    let blob_hash_key = blob_hash.to_string();
    let (prefix, suffix) = blob_hash_key.split_at(4);
    format!("blobs/{prefix}/{suffix}")
}

pub async fn try_get_as_http_response(
    state: &ServerState,
    blob_hash: BlobHash,
) -> eyre::Result<Option<axum::response::Response>> {
    let response = state
        .object_store
        .try_get_as_http_response(&blob_key(blob_hash))
        .await?;
    Ok(response)
}

pub async fn blob_exists(state: &ServerState, blob_hash: BlobHash) -> Result<bool, ServerError> {
    let mut db_transaction = state.db_pool.begin().await.map_err(ServerError::other)?;

    let blob_hash_value = blob_hash.to_string();
    let object_store_url_value = state.env.object_store_url.to_string();
    let object_store_key_value = blob_key(blob_hash);
    let result = sqlx::query!(
        r#"
            SELECT count(*) AS count
            FROM blobs
            WHERE blob_hash = ? AND object_store_url = ? AND object_store_key = ?
        "#,
        blob_hash_value,
        object_store_url_value,
        object_store_key_value,
    )
    .fetch_one(&mut *db_transaction)
    .await
    .map_err(ServerError::other)?;

    db_transaction.commit().await.map_err(ServerError::other)?;

    Ok(result.count > 0)
}

pub async fn upload_blob<E>(
    state: &ServerState,
    blob_hash: BlobHash,
    content: impl futures::Stream<Item = Result<bytes::Bytes, E>>,
) -> Result<(), ServerError>
where
    eyre::Error: From<E>,
{
    let blob_key = blob_key(blob_hash);
    let object_size = state
        .object_store
        .put_and_validate(&blob_key, content, blob_hash.to_blake3())
        .await?;

    {
        let mut db_transaction = state.db_pool.begin().await.map_err(ServerError::other)?;

        let blob_hash_value = blob_hash.to_string();
        let object_store_url_value = state.env.object_store_url.to_string();
        let object_store_key_value = blob_key.clone();
        let object_size_value: i64 = object_size
            .try_into()
            .wrap_err("object too big")
            .map_err(ServerError::other)?;

        let replaced_records = sqlx::query!(
            r#"
                DELETE FROM blobs
                WHERE blob_hash = ?
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
                    ?,
                    ?,
                    ?,
                    'none',
                    ?,
                    ?
                )
            "#,
            blob_hash_value,
            object_store_url_value,
            object_store_key_value,
            object_size_value,
            object_size_value,
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
