CREATE TABLE projects (
    project_hash BLOB PRIMARY KEY NOT NULL,
    project_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
) STRICT;

CREATE TABLE project_tags (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    tag TEXT NOT NULL,
    project_hash BLOB NOT NULL,
    is_current BOOLEAN,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX project_tags_name_tag_is_current ON project_tags (name, tag, is_current);

CREATE TRIGGER [project_tags_updated_at]
    AFTER UPDATE ON project_tags
    FOR EACH ROW
BEGIN
    UPDATE project_tags SET updated_at = CURRENT_TIMESTAMP WHERE id = new.id;
END;
