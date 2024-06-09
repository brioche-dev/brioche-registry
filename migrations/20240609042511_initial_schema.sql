CREATE TABLE projects (
    project_hash TEXT PRIMARY KEY NOT NULL,
    project_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE project_tags (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    tag TEXT NOT NULL,
    project_hash TEXT NOT NULL,
    is_current BOOLEAN,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX project_tags_name_tag_is_current ON project_tags (name, tag, is_current);

CREATE TABLE blobs (
    blob_hash TEXT PRIMARY KEY NOT NULL,
    object_store_url TEXT NOT NULL,
    object_store_key TEXT NOT NULL,
    compression TEXT NOT NULL,
    compressed_object_size BIGINT NOT NULL,
    blob_size BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE recipes (
    recipe_hash TEXT PRIMARY KEY NOT NULL,
    recipe_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE bakes (
    id BIGSERIAL PRIMARY KEY,
    input_hash TEXT NOT NULL REFERENCES recipes (recipe_hash),
    output_hash TEXT NOT NULL REFERENCES recipes (recipe_hash),
    is_canonical BOOLEAN,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX bakes_input_hash_is_canonical
    ON bakes (input_hash, is_canonical);
CREATE UNIQUE INDEX bakes_input_hash_output_hash
    ON bakes (input_hash, output_hash);

CREATE TABLE recipe_children (
    id BIGSERIAL PRIMARY KEY,
    recipe_hash TEXT NOT NULL,
    child_type TEXT NOT NULL,
    child_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX recipe_children_recipe_hash_child_type_child_hash
    ON recipe_children (recipe_hash, child_type, child_hash);
