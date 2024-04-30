use std::{borrow::Borrow, collections::HashSet};

use brioche::{
    blob::BlobHash,
    recipe::{Recipe, RecipeHash},
};
use eyre::OptionExt as _;
use joinery::JoinableIterator as _;
use sqlx::Arguments as _;

use crate::server::ServerState;

pub async fn save_recipes<R>(
    state: &ServerState,
    recipes: impl IntoIterator<Item = R>,
) -> eyre::Result<u64>
where
    R: Borrow<Recipe>,
{
    let mut db_transaction = state.db_pool.begin().await?;

    let mut arguments = sqlx::sqlite::SqliteArguments::default();
    let mut child_arguments = sqlx::sqlite::SqliteArguments::default();
    let mut num_recipes = 0;
    let mut num_child_records = 0;
    for recipe in recipes {
        let recipe = recipe.borrow();
        let recipe_hash = recipe.hash();

        let recipe_json = serde_json::to_string(recipe)?;

        arguments.add(recipe_hash.to_string());
        arguments.add(recipe_json);
        num_recipes += 1;

        let referenced_recipes = brioche::references::referenced_recipes(recipe);
        for child_recipe in referenced_recipes {
            child_arguments.add(recipe_hash.to_string());
            child_arguments.add("recipe".to_string());
            child_arguments.add(child_recipe.to_string());
            num_child_records += 1;
        }

        let referenced_blobs = brioche::references::referenced_blobs(recipe);
        for child_blob in referenced_blobs {
            child_arguments.add(recipe_hash.to_string());
            child_arguments.add("blob".to_string());
            child_arguments.add(child_blob.to_string());
            num_child_records += 1;
        }
    }

    let placeholders = std::iter::repeat("(?, ?)")
        .take(num_recipes)
        .join_with(", ");

    let result = sqlx::query_with(
        &format!(
            r#"
                INSERT INTO recipes (recipe_hash, recipe_json)
                VALUES {placeholders}
                ON CONFLICT DO NOTHING;
            "#,
        ),
        arguments,
    )
    .execute(&mut *db_transaction)
    .await?;

    let child_record_placeholders = std::iter::repeat("(?, ?, ?)")
        .take(num_child_records)
        .join_with(", ");

    sqlx::query_with(
        &format!(
            r#"
                INSERT INTO recipe_children (recipe_hash, child_type, child_hash)
                VALUES {child_record_placeholders}
                ON CONFLICT DO NOTHING;
            "#,
        ),
        child_arguments,
    )
    .execute(&mut *db_transaction)
    .await?;

    db_transaction.commit().await?;

    Ok(result.rows_affected())
}

pub struct RecipeDescendents {
    pub recipes: HashSet<RecipeHash>,
    pub blobs: HashSet<BlobHash>,
}

pub async fn get_recipe_descendents(
    state: &ServerState,
    recipe: RecipeHash,
) -> eyre::Result<RecipeDescendents> {
    let mut db_transaction = state.db_pool.begin().await?;

    let recipe_hash_value = recipe.to_string();
    let records = sqlx::query!(
        r#"
            WITH RECURSIVE recipe_descendents (descendent_type, descendent_hash) AS (
                SELECT "recipe", ?
                UNION
                SELECT recipe_children.child_type, recipe_children.child_hash
                FROM recipe_children
                INNER JOIN recipe_descendents
                    ON recipe_descendents.descendent_type = 'recipe'
                    AND recipe_descendents.descendent_hash = recipe_children.recipe_hash
            ) SELECT * FROM recipe_descendents;
        "#,
        recipe_hash_value,
    )
    .fetch_all(&mut *db_transaction)
    .await?;

    db_transaction.commit().await?;

    let mut descendents = RecipeDescendents {
        recipes: HashSet::new(),
        blobs: HashSet::new(),
    };

    for record in records {
        let descendent_hash = record
            .descendent_hash
            .ok_or_eyre("descendent hash is null")?;
        match &*record.descendent_type {
            "recipe" => {
                let recipe_hash: Result<RecipeHash, _> = descendent_hash.parse();
                let recipe_hash = recipe_hash.map_err(|error| eyre::eyre!(error))?;
                descendents.recipes.insert(recipe_hash);
            }
            "blob" => {
                let blob_hash: Result<BlobHash, _> = descendent_hash.parse();
                let blob_hash = blob_hash.map_err(|error| eyre::eyre!(error))?;
                descendents.blobs.insert(blob_hash);
            }
            other => {
                eyre::bail!("unexpected descendent type: {other:?}");
            }
        }
    }

    Ok(descendents)
}
