CREATE TABLE blobs (
    blob_hash TEXT PRIMARY KEY NOT NULL,
    object_store_url TEXT NOT NULL,
    object_store_key TEXT NOT NULL,
    compression TEXT NOT NULL,
    compressed_object_size INTEGER NOT NULL,
    blob_size INTEGER,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
) STRICT;
