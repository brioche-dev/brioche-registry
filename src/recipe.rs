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
    let mut num_recipes = 0;
    for recipe in recipes {
        let recipe = recipe.borrow();

        let recipe_json = serde_json::to_string(recipe)?;

        arguments.add(recipe.hash().to_string());
        arguments.add(recipe_json);
        num_recipes += 1;
    }

    let placeholders = std::iter::repeat("(?, ?)")
        .take(num_recipes)
        .join_with(", ");

    let result = sqlx::query_with(
        &format!(
            r#"
                INSERT INTO recipes (recipe_hash, recipe_json)
                VALUES {placeholders}
            "#,
        ),
        arguments,
    )
    .execute(&mut *db_transaction)
    .await?;

    db_transaction.commit().await?;

    Ok(result.rows_affected())
}
