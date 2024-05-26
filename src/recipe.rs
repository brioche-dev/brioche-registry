use std::collections::HashSet;

use brioche_core::{
    blob::BlobHash,
    recipe::{Recipe, RecipeHash},
};
use eyre::OptionExt as _;
use joinery::JoinableIterator as _;
use sqlx::Arguments as _;

use crate::server::ServerState;

pub async fn save_recipes(state: &ServerState, recipes: &[Recipe]) -> eyre::Result<u64> {
    let mut db_transaction = state.db_pool.begin().await?;

    let mut num_new_recipes = 0;
    for recipe_batch in recipes.chunks(400) {
        let mut arguments = sqlx::sqlite::SqliteArguments::default();
        let mut num_recipes = 0;
        let mut recipe_children = vec![];
        for recipe in recipe_batch {
            let recipe_hash = recipe.hash();

            arguments.add(recipe_hash.to_string());
            arguments.add(sqlx::types::Json(recipe.clone()));
            num_recipes += 1;

            let referenced_recipes = brioche_core::references::referenced_recipes(recipe);
            for child_recipe in referenced_recipes {
                recipe_children.push((recipe_hash, RecipeChild::Recipe(child_recipe)));
            }

            let referenced_blobs = brioche_core::references::referenced_blobs(recipe);
            for child_blob in referenced_blobs {
                recipe_children.push((recipe_hash, RecipeChild::Blob(child_blob)));
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

        num_new_recipes += result.rows_affected();

        for children in recipe_children.chunks(300) {
            let mut child_arguments = sqlx::sqlite::SqliteArguments::default();

            for (recipe_hash, child) in children {
                match child {
                    RecipeChild::Recipe(child_recipe) => {
                        child_arguments.add(recipe_hash.to_string());
                        child_arguments.add("recipe");
                        child_arguments.add(child_recipe.to_string());
                    }
                    RecipeChild::Blob(child_blob) => {
                        child_arguments.add(recipe_hash.to_string());
                        child_arguments.add("blob");
                        child_arguments.add(child_blob.to_string());
                    }
                }
            }

            let child_record_placeholders = std::iter::repeat("(?, ?, ?)")
                .take(children.len())
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
        }
    }

    db_transaction.commit().await?;

    Ok(num_new_recipes)
}

enum RecipeChild {
    Recipe(RecipeHash),
    Blob(BlobHash),
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
