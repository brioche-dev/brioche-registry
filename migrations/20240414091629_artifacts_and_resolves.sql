CREATE TABLE artifacts (
    artifact_hash TEXT PRIMARY KEY NOT NULL,
    artifact_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
) STRICT;

CREATE TABLE resolves (
    id INTEGER PRIMARY KEY NOT NULL,
    input_hash TEXT NOT NULL REFERENCES artifacts (artifact_hash),
    output_hash TEXT NOT NULL REFERENCES artifacts (artifact_hash),
    is_canonical BOOLEAN,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX resolves_input_hash_is_canonical
ON resolves (input_hash, output_hash, is_canonical);

CREATE UNIQUE INDEX resolves_input_hash_output_hash
ON resolves (input_hash, output_hash);
