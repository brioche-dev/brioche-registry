use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    net::SocketAddr,
    str::FromStr as _,
    sync::Arc,
    time::Duration,
};

use argon2::PasswordVerifier;
use axum::body::Body;
use axum_extra::headers::{authorization::Basic, Authorization};
use brioche_core::{
    blob::BlobHash,
    project::{Project, ProjectHash},
    recipe::{Artifact, Recipe, RecipeHash},
    registry::{
        CreateBakeRequest, CreateBakeResponse, CreateProjectTagsRequest, CreateProjectTagsResponse,
        GetBakeResponse, GetProjectTagResponse, UpdatedTag,
    },
};
use bstr::ByteSlice as _;
use eyre::{Context as _, OptionExt as _};
use joinery::JoinableIterator as _;
use sqlx::Arguments as _;
use tracing::Span;

pub async fn start_server(state: Arc<ServerState>, addr: &SocketAddr) -> eyre::Result<()> {
    let proxy_layers = state.proxy_layers;
    let trace_layer = tower_http::trace::TraceLayer::new_for_http()
        .make_span_with(move |req: &axum::http::Request<Body>| {
            let request_id = ulid::Ulid::new();

            let connect_info = req
                .extensions()
                .get::<axum::extract::ConnectInfo<SocketAddr>>();
            let received_ip = connect_info.map(|connect_info| connect_info.0.ip().to_string());
            let received_ip = received_ip.as_deref().unwrap_or("<unknown>");
            let forwarded_for = req
                .headers()
                .get_all("X-Forwarded-For")
                .into_iter()
                .flat_map(|forwarded_for| forwarded_for.as_bytes().split_str(","))
                .map(|forwarded_for| String::from_utf8_lossy(forwarded_for.trim()));
            let client_ips = [Cow::Borrowed(received_ip)]
                .into_iter()
                .chain(forwarded_for);
            let client_ip = client_ips
                .take(proxy_layers + 1)
                .last()
                .unwrap_or(Cow::Borrowed("<unknown>"));

            let user_agent = req
                .headers()
                .get("User-Agent")
                .map(|user_agent| user_agent.to_str().unwrap_or("<invalid>"))
                .unwrap_or("<unknown>");

            tracing::info_span!(
                "request",
                method = %req.method(),
                path = %req.uri().path(),
                %client_ip,
                %request_id,
                %user_agent,
            )
        })
        .on_request(|_req: &axum::http::Request<Body>, _span: &Span| {
            tracing::info!("received request")
        })
        .on_response(
            |res: &axum::http::Response<Body>, latency: Duration, _span: &Span| {
                tracing::info!(
                    status = res.status().as_u16(),
                    latency = latency.as_secs_f32(),
                    "response"
                )
            },
        )
        .on_failure(
            |err: tower_http::classify::ServerErrorsFailureClass,
             latency: Duration,
             _span: &Span| {
                tracing::error!(error = %err, latency = latency.as_secs_f32(), "request failed")
            },
        );

    let app = axum::Router::new()
        .route("/v0/healthcheck", axum::routing::get(healthcheck_handler))
        .route(
            "/v0/projects",
            axum::routing::post(bulk_create_projects_handler),
        )
        .route(
            "/v0/projects/:project_hash",
            axum::routing::get(get_project_handler),
        )
        .route(
            "/v0/project-tags",
            axum::routing::post(bulk_create_project_tags_handler),
        )
        .route(
            "/v0/project-tags/:project_name/:tag",
            axum::routing::get(get_project_tag_handler),
        )
        .route(
            "/v0/blobs/:blob_hash",
            axum::routing::get(get_blob_handler).put(put_blob_handler),
        )
        .route(
            "/v0/recipes/:recipe_hash",
            axum::routing::get(get_recipe_handler).put(put_recipe_handler),
        )
        .route(
            "/v0/recipes/:recipe_hash/bake",
            axum::routing::get(get_bake_handler).post(create_bake_handler),
        )
        .route(
            "/v0/recipes",
            axum::routing::post(bulk_create_recipes_handler),
        )
        .route(
            "/v0/known-projects",
            axum::routing::post(known_projects_handler),
        )
        .route(
            "/v0/known-recipes",
            axum::routing::post(known_recipes_handler),
        )
        .route("/v0/known-blobs", axum::routing::post(known_blobs_handler))
        .route("/v0/known-bakes", axum::routing::post(known_bakes_handler))
        .layer(trace_layer)
        .with_state(state.clone());
    let app = app.into_make_service_with_connect_info::<SocketAddr>();

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let listen_addr = listener.local_addr()?;
    tracing::info!("listening on {listen_addr}");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    tracing::info!("shutting down");
    state.handle_shutdown().await;

    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c()
        .await
        .inspect_err(|error| tracing::error!(%error, "failed while awaiting shutdown signal"));
}

pub struct ServerState {
    pub env: super::ServerEnv,
    pub proxy_layers: usize,
    pub object_store: crate::object_store::ObjectStore,
    pub db_pool: sqlx::PgPool,
}

impl ServerState {
    pub async fn new(env: super::ServerEnv) -> eyre::Result<Self> {
        let object_store =
            crate::object_store::ObjectStore::new(crate::object_store::ObjectStoreConfig {
                url: env.object_store_url.clone(),
                public_base_url: env.public_object_store_base_url.clone(),
            })
            .await?;

        let db_opts = sqlx::postgres::PgConnectOptions::from_str(&env.database_url)?;
        let db_pool = sqlx::postgres::PgPoolOptions::new()
            .connect_with(db_opts)
            .await?;

        let proxy_layers = env.proxy_layers.unwrap_or(0);

        tracing::info!("set up database connection");

        Ok(Self {
            env,
            proxy_layers,
            object_store,
            db_pool,
        })
    }

    fn password_hash(&self) -> eyre::Result<argon2::PasswordHash<'_>> {
        let password_hash =
            argon2::PasswordHash::new(&self.env.password_hash).wrap_err("invalid password hash")?;
        Ok(password_hash)
    }

    async fn handle_shutdown(&self) {
        self.db_pool.close().await;
    }
}

async fn healthcheck_handler() -> axum::Json<Healthcheck> {
    axum::Json(Healthcheck { status: "ok" })
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Healthcheck {
    status: &'static str,
}

async fn get_project_handler(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    axum::extract::Path(project_hash): axum::extract::Path<ProjectHash>,
) -> Result<axum::Json<serde_json::Value>, ServerError> {
    let project_hash_value = project_hash.to_string();
    let record = sqlx::query!(
        "SELECT project_json FROM projects WHERE project_hash = $1",
        project_hash_value,
    )
    .fetch_one(&state.db_pool)
    .await
    .wrap_err("failed to fetch project from database")?;

    Ok(axum::Json(record.project_json))
}

async fn bulk_create_project_tags_handler(
    Authenticated(state): Authenticated,
    axum::Json(project_tags): axum::Json<CreateProjectTagsRequest>,
) -> Result<axum::Json<CreateProjectTagsResponse>, ServerError> {
    let mut db_transaction = state.db_pool.begin().await.map_err(ServerError::other)?;

    let mut tags = vec![];

    for tag in &project_tags.tags {
        let project_hash_value = tag.project_hash.to_string();
        let update_result = sqlx::query!(
            r#"
                UPDATE project_tags
                SET
                    is_current = NULL,
                    updated_at = NOW()
                WHERE
                    name = $1
                    AND tag = $2
                    AND project_hash <> $3
                    AND is_current
                RETURNING project_hash
            "#,
            tag.project_name,
            tag.tag,
            project_hash_value,
        )
        .fetch_all(&mut *db_transaction)
        .await
        .map_err(ServerError::other)?;

        let inserted_records = sqlx::query!(
            r#"
                INSERT INTO project_tags (
                    name,
                    tag,
                    project_hash,
                    is_current
                ) VALUES ($1, $2, $3, TRUE)
                ON CONFLICT (name, tag, is_current) DO NOTHING
                RETURNING name, tag
            "#,
            tag.project_name,
            tag.tag,
            project_hash_value,
        )
        .fetch_all(&mut *db_transaction)
        .await
        .map_err(ServerError::other)?;

        // Run a query to make sure the current tag matches the expected
        // project hash. This is a sanity check

        let records = sqlx::query!(
            r#"
                SELECT project_hash
                FROM project_tags
                WHERE name = $1 AND tag = $2 AND is_current = TRUE
            "#,
            tag.project_name,
            tag.tag,
        )
        .fetch_all(&mut *db_transaction)
        .await
        .map_err(ServerError::other)?;

        let record = records
            .first()
            .ok_or_eyre("failed to get updated project tag")
            .map_err(ServerError::other)?;
        let record_project_hash: Result<ProjectHash, _> = record.project_hash.parse();
        let record_project_hash = record_project_hash
            .map_err(|error| eyre::eyre!(error))
            .map_err(ServerError::other)?;
        if record_project_hash != tag.project_hash {
            return Err(ServerError::Other(eyre::eyre!("project tag did not match")));
        }

        let previous_hash = update_result
            .first()
            .and_then(|record| record.project_hash.parse().ok());

        for record in inserted_records {
            tags.push(UpdatedTag {
                name: record.name.to_string(),
                tag: record.tag.to_string(),
                previous_hash,
            });
        }
    }

    db_transaction.commit().await.map_err(ServerError::other)?;

    Ok(axum::Json(CreateProjectTagsResponse { tags }))
}

async fn get_project_tag_handler(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    axum::extract::Path((project_name, tag)): axum::extract::Path<(String, String)>,
) -> Result<axum::Json<GetProjectTagResponse>, ServerError> {
    let records = sqlx::query!(
        "SELECT project_hash FROM project_tags WHERE name = $1 AND tag = $2 AND is_current",
        project_name,
        tag,
    )
    .fetch_all(&state.db_pool)
    .await
    .wrap_err("failed to fetch project tags from database")?;
    let record = records.first().ok_or(ServerError::NotFound)?;

    let project_hash: Result<ProjectHash, _> = record.project_hash.parse();
    let project_hash = project_hash
        .map_err(|error| eyre::eyre!(error))
        .wrap_err("failed to parse project hash from database")
        .map_err(ServerError::other)?;
    let response = GetProjectTagResponse { project_hash };
    Ok(axum::Json(response))
}

async fn bulk_create_projects_handler(
    Authenticated(state): Authenticated,
    axum::Json(projects): axum::Json<HashMap<ProjectHash, Project>>,
) -> Result<axum::Json<u64>, ServerError> {
    for (project_hash, project) in &projects {
        project_hash
            .validate_matches(project)
            .map_err(|error| ServerError::BadRequest(error.to_string().into()))?;
    }

    let mut db_transaction = state.db_pool.begin().await.map_err(ServerError::other)?;

    let mut new_projects = 0;
    for (project_hash, project) in &projects {
        let project_hash_value = project_hash.to_string();
        let project_json = serde_json::to_value(&project).map_err(ServerError::other)?;
        let result = sqlx::query!(
            "INSERT INTO projects (project_hash, project_json) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            project_hash_value,
            project_json
        )
        .execute(&mut *db_transaction)
        .await
        .map_err(ServerError::other)?;

        new_projects += result.rows_affected();
    }

    db_transaction.commit().await.map_err(ServerError::other)?;

    tracing::info!(new_projects, "created projects");

    Ok(axum::Json(new_projects))
}

async fn get_blob_handler(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    axum::extract::Path(blob_hash_with_extension): axum::extract::Path<String>,
) -> Result<axum::response::Response, ServerError> {
    let (blob_hash, compression) = match blob_hash_with_extension.rsplit_once('.') {
        Some((blob_hash, "zst")) => {
            let blob_hash: BlobHash = blob_hash.parse().map_err(|error| {
                ServerError::BadRequest(Cow::Owned(format!("invalid blob hash: {error}")))
            })?;
            (blob_hash, crate::blob::CompressionScheme::Zstd)
        }
        _ => {
            return Err(ServerError::BadRequest(Cow::Borrowed(
                "must fetch blob with an extension (such as /v0/blobs/000000.zst)",
            )))
        }
    };

    let response = crate::blob::try_get_as_http_response(&state, blob_hash, compression).await?;
    let response = response.ok_or_else(|| ServerError::NotFound)?;
    Ok(response)
}

async fn put_blob_handler(
    Authenticated(state): Authenticated,
    axum::extract::Path(blob_hash): axum::extract::Path<BlobHash>,
    body: axum::body::Body,
) -> Result<(axum::http::StatusCode, axum::Json<BlobHash>), ServerError> {
    // Return an error if the blob already exists
    let blob_exists =
        crate::blob::blob_exists(&state, blob_hash, crate::blob::CompressionScheme::Zstd).await?;
    if blob_exists {
        return Err(ServerError::AlreadyExists);
    }

    crate::blob::upload_blob(
        &state,
        blob_hash,
        crate::blob::CompressionScheme::Zstd,
        body.into_data_stream(),
    )
    .await?;

    Ok((axum::http::StatusCode::CREATED, axum::Json(blob_hash)))
}

async fn get_recipe_handler(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    axum::extract::Path(recipe_hash): axum::extract::Path<RecipeHash>,
) -> Result<axum::Json<serde_json::Value>, ServerError> {
    let mut db_transaction = state.db_pool.begin().await.map_err(ServerError::other)?;

    let recipe_hash_value = recipe_hash.to_string();
    let record = sqlx::query!(
        "SELECT recipe_json FROM recipes WHERE recipe_hash = $1",
        recipe_hash_value,
    )
    .fetch_optional(&mut *db_transaction)
    .await
    .map_err(ServerError::other)?;

    db_transaction.commit().await.map_err(ServerError::other)?;

    match record {
        Some(record) => {
            let recipe: serde_json::Value = serde_json::from_value(record.recipe_json)
                .wrap_err_with(|| {
                    format!("failed to deserialize recipe JSON with hash {recipe_hash}")
                })
                .map_err(ServerError::other)?;
            Ok(axum::Json(recipe))
        }
        None => Err(ServerError::NotFound),
    }
}

async fn put_recipe_handler(
    Authenticated(state): Authenticated,
    axum::extract::Path(recipe_hash): axum::extract::Path<RecipeHash>,
    axum::Json(recipe): axum::Json<Recipe>,
) -> Result<(axum::http::StatusCode, axum::Json<RecipeHash>), ServerError> {
    if recipe_hash != recipe.hash() {
        return Err(ServerError::BadRequest(Cow::Owned(format!(
            "expected recipe hash to be {recipe_hash}, but was {}",
            recipe.hash()
        ))));
    }

    let new_rows = crate::recipe::save_recipes(&state, &[recipe])
        .await
        .map_err(ServerError::other)?;

    if new_rows == 0 {
        Ok((axum::http::StatusCode::OK, axum::Json(recipe_hash)))
    } else {
        Ok((axum::http::StatusCode::CREATED, axum::Json(recipe_hash)))
    }
}

async fn bulk_create_recipes_handler(
    Authenticated(state): Authenticated,
    axum::Json(recipes): axum::Json<HashMap<RecipeHash, Recipe>>,
) -> Result<(axum::http::StatusCode, axum::Json<usize>), ServerError> {
    for (recipe_hash, recipe) in &recipes {
        if *recipe_hash != recipe.hash() {
            return Err(ServerError::BadRequest(Cow::Owned(format!(
                "expected recipe hash to be {recipe_hash}, but was {}",
                recipe.hash()
            ))));
        }
    }

    let recipe_values = recipes.values().cloned().collect::<Vec<_>>();
    let new_rows = crate::recipe::save_recipes(&state, &recipe_values)
        .await
        .map_err(ServerError::other)?;
    let new_rows: usize = new_rows.try_into().map_err(ServerError::other)?;
    if new_rows != recipes.len() {
        return Err(ServerError::other(eyre::eyre!(
            "failed to insert all recipes",
        )));
    }

    Ok((axum::http::StatusCode::CREATED, axum::Json(new_rows)))
}

async fn known_projects_handler(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    axum::Json(project_hashes): axum::Json<HashSet<ProjectHash>>,
) -> Result<axum::Json<HashSet<ProjectHash>>, ServerError> {
    let mut db_transaction = state.db_pool.begin().await.map_err(ServerError::other)?;

    let mut arguments = sqlx::postgres::PgArguments::default();
    for hash in &project_hashes {
        arguments.add(hash.to_string());
    }

    let placeholders = (0..project_hashes.len()).map(|n| format!("${}", n + 1))
        .join_with(", ");

    let rows = sqlx::query_as_with::<_, (String,), _>(
        &format!(
            r#"
            SELECT project_hash
            FROM projects
            WHERE project_hash IN ({placeholders})
        "#,
        ),
        arguments,
    )
    .fetch_all(&mut *db_transaction)
    .await
    .map_err(ServerError::other)?;

    let results = rows
        .into_iter()
        .map(|row| row.0.parse())
        .collect::<Result<HashSet<ProjectHash>, _>>()
        .map_err(|error| eyre::eyre!(error))
        .map_err(ServerError::other)?;
    Ok(axum::Json(results))
}

async fn known_recipes_handler(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    axum::Json(recipe_hashes): axum::Json<HashSet<RecipeHash>>,
) -> Result<axum::Json<HashSet<RecipeHash>>, ServerError> {
    let mut db_transaction = state.db_pool.begin().await.map_err(ServerError::other)?;

    let mut arguments = sqlx::postgres::PgArguments::default();
    for hash in &recipe_hashes {
        arguments.add(hash.to_string());
    }

    let placeholders = (0..recipe_hashes.len()).map(|n| format!("${}", n + 1))
        .join_with(", ");

    let rows = sqlx::query_as_with::<_, (String,), _>(
        &format!(
            r#"
            SELECT recipe_hash
            FROM recipes
            WHERE recipe_hash IN ({placeholders})
        "#,
        ),
        arguments,
    )
    .fetch_all(&mut *db_transaction)
    .await
    .map_err(ServerError::other)?;

    let results = rows
        .into_iter()
        .map(|row| row.0.parse())
        .collect::<Result<HashSet<RecipeHash>, _>>()
        .map_err(|error| eyre::eyre!(error))
        .map_err(ServerError::other)?;
    Ok(axum::Json(results))
}

async fn known_blobs_handler(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    axum::Json(blob_hashes): axum::Json<HashSet<BlobHash>>,
) -> Result<axum::Json<HashSet<BlobHash>>, ServerError> {
    let mut db_transaction = state.db_pool.begin().await.map_err(ServerError::other)?;

    let mut arguments = sqlx::postgres::PgArguments::default();
    for hash in &blob_hashes {
        arguments.add(hash.to_string());
    }

    let placeholders = (0..blob_hashes.len()).map(|n| format!("${}", n + 1))
        .join_with(", ");

    // TODO: Handle changes in `object_store_url` and `object_store_key`
    let rows = sqlx::query_as_with::<_, (String,), _>(
        &format!(
            r#"
            SELECT blob_hash
            FROM blobs
            WHERE blob_hash IN ({placeholders})
        "#,
        ),
        arguments,
    )
    .fetch_all(&mut *db_transaction)
    .await
    .map_err(ServerError::other)?;

    let results = rows
        .into_iter()
        .map(|row| row.0.parse())
        .collect::<Result<HashSet<BlobHash>, _>>()
        .map_err(|error| eyre::eyre!(error))
        .map_err(ServerError::other)?;
    Ok(axum::Json(results))
}

async fn known_bakes_handler(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    axum::Json(bakes): axum::Json<HashSet<(RecipeHash, RecipeHash)>>,
) -> Result<axum::Json<HashSet<(RecipeHash, RecipeHash)>>, ServerError> {
    let mut db_transaction = state.db_pool.begin().await.map_err(ServerError::other)?;

    let mut arguments = sqlx::postgres::PgArguments::default();
    for (input_hash, output_hash) in &bakes {
        arguments.add(input_hash.to_string());
        arguments.add(output_hash.to_string());
    }


    let placeholders = (0..bakes.len()).map(|n| format!("(${}, ${})", n * 2 + 1, n * 2 + 2))
    .join_with(", ");

    let rows = sqlx::query_as_with::<_, (String, String), _>(
        &format!(
            r#"
            SELECT input_hash, output_hash
            FROM bakes
            WHERE (input_hash, output_hash) IN (VALUES {placeholders})
        "#,
        ),
        arguments,
    )
    .fetch_all(&mut *db_transaction)
    .await
    .map_err(ServerError::other)?;

    let results = rows
        .into_iter()
        .map(|row| {
            let input_hash: Result<RecipeHash, _> = row.0.parse();
            let input_hash = input_hash.map_err(|error| eyre::eyre!(error))?;
            let output_hash: Result<RecipeHash, _> = row.1.parse();
            let output_hash = output_hash.map_err(|error| eyre::eyre!(error))?;
            eyre::Ok((input_hash, output_hash))
        })
        .collect::<Result<HashSet<(RecipeHash, RecipeHash)>, _>>()
        .map_err(ServerError::other)?;
    Ok(axum::Json(results))
}

async fn get_bake_handler(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    axum::extract::Path(recipe_hash): axum::extract::Path<RecipeHash>,
) -> Result<axum::Json<GetBakeResponse>, ServerError> {
    let mut db_transaction = state.db_pool.begin().await.map_err(ServerError::other)?;

    let recipe_hash_value = recipe_hash.to_string();
    let record = sqlx::query!(
        r#"
            SELECT recipe_hash, recipe_json
            FROM recipes
            INNER JOIN bakes
                ON bakes.output_hash = recipes.recipe_hash
            WHERE bakes.input_hash = $1 AND bakes.is_canonical
        "#,
        recipe_hash_value,
    )
    .fetch_optional(&mut *db_transaction)
    .await
    .map_err(ServerError::other)?;

    db_transaction.commit().await.map_err(ServerError::other)?;

    let Some(record) = record else {
        return Err(ServerError::NotFound);
    };

    let output_artifact: Artifact = serde_json::from_value(record.recipe_json)
        .wrap_err_with(|| format!("failed to deserialize recipe JSON with hash {recipe_hash}"))
        .map_err(ServerError::other)?;
    let record_recipe_hash: Result<RecipeHash, _> = record.recipe_hash.parse();
    let record_recipe_hash = record_recipe_hash
        .map_err(|error| eyre::eyre!(error))
        .wrap_err_with(|| format!("failed to parse recipe hash {}", record.recipe_hash))
        .map_err(ServerError::other)?;

    if output_artifact.hash() != record_recipe_hash {
        return Err(ServerError::Other(eyre::eyre!(
            "artifact hash {} did not match expected hash {record_recipe_hash}",
            output_artifact.hash(),
        )));
    }

    let descendents = crate::recipe::get_recipe_descendents(&state, output_artifact.hash())
        .await
        .map_err(ServerError::other)?;

    let response = GetBakeResponse {
        output_hash: output_artifact.hash(),
        output_artifact,
        referenced_blobs: descendents.blobs,
        referenced_recipes: descendents.recipes,
    };
    Ok(axum::Json(response))
}

async fn create_bake_handler(
    Authenticated(state): Authenticated,
    axum::extract::Path(input_hash): axum::extract::Path<RecipeHash>,
    axum::Json(request): axum::Json<CreateBakeRequest>,
) -> Result<(axum::http::StatusCode, axum::Json<CreateBakeResponse>), ServerError> {
    let mut db_transaction = state.db_pool.begin().await.map_err(ServerError::other)?;

    let input_hash_value = input_hash.to_string();
    let input_result = sqlx::query!(
        r#"
            SELECT recipe_json
            FROM recipes
            WHERE recipe_hash = $1
        "#,
        input_hash_value,
    )
    .fetch_optional(&mut *db_transaction)
    .await
    .map_err(ServerError::other)?;

    let Some(input_result) = input_result else {
        return Err(ServerError::NotFound);
    };

    let input_recipe: Recipe = serde_json::from_value(input_result.recipe_json)
        .wrap_err_with(|| format!("failed to deserialize input recipe JSON with hash {input_hash}"))
        .map_err(ServerError::other)?;
    if input_recipe.hash() != input_hash {
        return Err(ServerError::Other(eyre::eyre!(
            "recipe hash {} did not match expected hash {input_hash}",
            input_recipe.hash()
        )));
    }

    let output_hash = request.output_hash;
    let output_hash_value = output_hash.to_string();
    let output_result = sqlx::query!(
        r#"
            SELECT recipe_json
            FROM recipes
            WHERE recipe_hash = $1
        "#,
        output_hash_value,
    )
    .fetch_optional(&mut *db_transaction)
    .await
    .map_err(ServerError::other)?;

    let Some(output_result) = output_result else {
        return Err(ServerError::BadRequest(Cow::Owned(format!(
            "output artifact {output_hash} does not exist"
        ))));
    };

    let output_artifact: Recipe = serde_json::from_value(output_result.recipe_json)
        .wrap_err_with(|| {
            format!("failed to deserialize output recipe JSON with hash {output_hash}")
        })
        .map_err(ServerError::other)?;
    let _output_artifact: Artifact = output_artifact.try_into().map_err(|_| {
        ServerError::BadRequest(Cow::Owned(format!(
            "output recipe {output_hash} is not a complete artifact"
        )))
    })?;

    let canonical_result = sqlx::query!(
        r#"
            SELECT output_hash
            FROM bakes
            WHERE input_hash = $1 AND is_canonical
        "#,
        input_hash_value,
    )
    .fetch_optional(&mut *db_transaction)
    .await
    .map_err(ServerError::other)?;

    let (canonical_output_hash, is_new) = match canonical_result {
        Some(canonical_result) => {
            let canonical_output_hash: Result<RecipeHash, _> = canonical_result.output_hash.parse();
            let canonical_output_hash = canonical_output_hash
                .map_err(|error| eyre::eyre!(error))
                .map_err(ServerError::other)?;

            let insert_result = sqlx::query!(
                r#"
                    INSERT INTO bakes (input_hash, output_hash, is_canonical)
                    VALUES ($1, $2, NULL)
                    ON CONFLICT (input_hash, output_hash) DO NOTHING
                "#,
                input_hash_value,
                output_hash_value,
            )
            .execute(&mut *db_transaction)
            .await
            .map_err(ServerError::other)?;

            if insert_result.rows_affected() > 0 {
                tracing::info!(%input_hash, %output_hash, "added bake (non-canonical)");
                (canonical_output_hash, true)
            } else {
                tracing::info!(%input_hash, %output_hash, "received bake, but already exists (non-canonical)");
                (canonical_output_hash, false)
            }
        }
        None => {
            sqlx::query!(
                r#"
                    INSERT INTO bakes (input_hash, output_hash, is_canonical)
                    VALUES ($1, $2, TRUE)
                "#,
                input_hash_value,
                output_hash_value,
            )
            .execute(&mut *db_transaction)
            .await
            .map_err(ServerError::other)?;

            tracing::info!(%input_hash, %output_hash, "added bake (canonical)");

            (output_hash, true)
        }
    };

    db_transaction.commit().await.map_err(ServerError::other)?;

    let status = if is_new {
        axum::http::StatusCode::CREATED
    } else {
        axum::http::StatusCode::OK
    };
    let response = CreateBakeResponse {
        canonical_output_hash,
    };

    Ok((status, axum::Json(response)))
}

struct Authenticated(Arc<ServerState>);

#[async_trait::async_trait]
impl axum::extract::FromRequestParts<Arc<ServerState>> for Authenticated {
    type Rejection = ServerError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &Arc<ServerState>,
    ) -> Result<Self, Self::Rejection> {
        let authorization =
            axum_extra::TypedHeader::<Authorization<Basic>>::from_request_parts(parts, &()).await;
        let authorization = match authorization {
            Ok(authorization) => authorization,
            Err(rejection) if rejection.is_missing() => {
                return Err(ServerError::CredentialsRequired);
            }
            Err(rejection) => {
                return Err(rejection.into());
            }
        };

        let username_matches = authorization.username() == "admin";
        let password_matches = argon2::Argon2::default()
            .verify_password(authorization.password().as_bytes(), &state.password_hash()?)
            .is_ok();
        if username_matches && password_matches {
            Ok(Self(state.clone()))
        } else {
            Err(ServerError::InvalidCredentials)
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("internal error")]
    Other(#[from] eyre::Error),

    #[error("not found")]
    NotFound,

    #[error("{0}")]
    BadRequest(Cow<'static, str>),

    #[error("credentials required")]
    CredentialsRequired,

    #[error("username or password did not match")]
    InvalidCredentials,

    #[error("resource already exists")]
    AlreadyExists,

    #[error(transparent)]
    TypedHeaderRejection(#[from] axum_extra::typed_header::TypedHeaderRejection),
}

impl ServerError {
    pub fn other(error: impl Into<eyre::Error>) -> Self {
        Self::Other(error.into())
    }
}

impl axum::response::IntoResponse for ServerError {
    fn into_response(self) -> axum::response::Response {
        let body = serde_json::json!({
            "error": self.to_string(),
        });

        let status_code = match self {
            ServerError::Other(error) => {
                tracing::error!("internal error: {error:#}");
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            }
            ServerError::BadRequest(_) => axum::http::StatusCode::BAD_REQUEST,
            ServerError::AlreadyExists => axum::http::StatusCode::CONFLICT,
            ServerError::NotFound => axum::http::StatusCode::NOT_FOUND,
            ServerError::InvalidCredentials => axum::http::StatusCode::UNAUTHORIZED,
            ServerError::CredentialsRequired => axum::http::StatusCode::FORBIDDEN,
            ServerError::TypedHeaderRejection(rejection) => rejection.into_response().status(),
        };

        (status_code, axum::response::Json(body)).into_response()
    }
}
