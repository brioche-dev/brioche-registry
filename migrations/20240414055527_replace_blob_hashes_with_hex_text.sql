-- Change the project_hash columns in the projects and
-- project_tags tables to use hex-encoded TEXT instead of
-- BLOBs

-------------------------------------------
-- projects
-- Changing project_hash from BLOB to TEXT
-------------------------------------------

-- Rename the old table
ALTER TABLE projects RENAME TO projects_old;

-- Create the new table
CREATE TABLE projects (
    project_hash TEXT PRIMARY KEY NOT NULL,
    project_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
) STRICT;

-- Copy the data
INSERT INTO projects(project_hash, project_json, created_at)
SELECT lower(hex(project_hash)), project_json, created_at
FROM projects_old;

-- Drop the old table
DROP TABLE projects_old;

-------------------------------------------
-- project_tags
-- Changing project_hash from BLOB to TEXT
-------------------------------------------

-- Drop trigger and index
DROP TRIGGER project_tags_updated_at;
DROP INDEX project_tags_name_tag_is_current;

-- Rename the old table
ALTER TABLE project_tags RENAME TO project_tags_old;

-- Create the new table
CREATE TABLE project_tags (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    tag TEXT NOT NULL,
    project_hash TEXT NOT NULL,
    is_current BOOLEAN,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Copy the data (note: ID is not copied, and instead uses SQLite rowid)
INSERT INTO project_tags (name, tag, project_hash, is_current, created_at, updated_at)
SELECT name, tag, lower(hex(project_hash)), is_current, created_at, updated_at
FROM project_tags_old;

-- Drop the old table
DROP TABLE project_tags_old;

-- Re-add the index and trigger

CREATE UNIQUE INDEX project_tags_name_tag_is_current ON project_tags (name, tag, is_current);

CREATE TRIGGER [project_tags_updated_at]
    AFTER UPDATE ON project_tags
    FOR EACH ROW
BEGIN
    UPDATE project_tags SET updated_at = CURRENT_TIMESTAMP WHERE id = new.id;
END;
