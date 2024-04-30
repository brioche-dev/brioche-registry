CREATE TABLE recipe_children (
    id INTEGER PRIMARY KEY NOT NULL,
    recipe_hash TEXT NOT NULL,
    child_type TEXT NOT NULL,
    child_hash TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
) STRICT;

CREATE UNIQUE INDEX recipe_children_recipe_hash_child_type_child_hash
ON recipe_children (recipe_hash, child_type, child_hash);

-- Clear existing recipes/bakes, so that recipes will be re-added with
-- their children
DELETE FROM bakes;
DELETE FROM recipes;
