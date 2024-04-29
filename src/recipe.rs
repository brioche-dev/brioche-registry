use std::borrow::Borrow;

use brioche::recipe::Recipe;
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
