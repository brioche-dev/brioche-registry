-- Rename `artifacts` table to `recipes`
-- - Rename `artifact_hash` column to `recipe_hash`
-- - Rename `artifact_json` column to `recipe_json`
CREATE TABLE recipes (
    recipe_hash TEXT PRIMARY KEY NOT NULL,
    recipe_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
) STRICT;

INSERT INTO recipes (recipe_hash, recipe_json, created_at)
SELECT artifact_hash, artifact_json, created_at
FROM artifacts;

-- Rename `resolves` table to `bakes`
CREATE TABLE bakes (
    id INTEGER PRIMARY KEY NOT NULL,
    input_hash TEXT NOT NULL REFERENCES recipes (recipe_hash),
    output_hash TEXT NOT NULL REFERENCES recipes (recipe_hash),
    is_canonical BOOLEAN,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX bakes_input_hash_is_canonical
ON bakes (input_hash, is_canonical);

CREATE UNIQUE INDEX bakes_input_hash_output_hash
ON bakes (input_hash, output_hash);

INSERT INTO bakes (input_hash, output_hash, is_canonical, created_at)
SELECT input_hash, output_hash, is_canonical, created_at
FROM resolves;

DROP INDEX resolves_input_hash_is_canonical;
DROP INDEX resolves_input_hash_output_hash;
DROP TABLE resolves;
DROP TABLE artifacts;
