use std::{borrow::Cow, str::FromStr as _, sync::Arc, time::Duration};

use argon2::PasswordVerifier;
use axum::body::Body;
use axum_extra::headers::{authorization::Basic, Authorization};
use brioche::{
    artifact::{ArtifactHash, LazyArtifact},
    blob::BlobId,
    project::{Project, ProjectHash, ProjectListing},
    registry::{GetProjectTagResponse, PublishProjectResponse, UpdatedTag},
};
use eyre::{Context as _, OptionExt as _};
use futures::StreamExt as _;
use tokio::io::AsyncWriteExt as _;
use tracing::Span;

pub async fn start_server(
    state: Arc<ServerState>,
    addr: &std::net::SocketAddr,
) -> eyre::Result<()> {
    let trace_layer = tower_http::trace::TraceLayer::new_for_http()
        .make_span_with(|_req: &axum::http::Request<Body>| {
            tracing::info_span!("request")
        })
        .on_request(|req: &axum::http::Request<Body>, _span: &Span| {
            tracing::info!(method = %req.method(), path = %req.uri().path(), "received request")
        })
        .on_response(|res: &axum::http::Response<Body>, latency: Duration, _span: &Span| {
            tracing::info!(status = res.status().as_u16(), latency = latency.as_secs_f32(), "response")
        })
        .on_failure(|err: tower_http::classify::ServerErrorsFailureClass, latency: Duration, _span: &Span| {
            tracing::error!(error = %err, latency = latency.as_secs_f32(), "request failed")
        });

    let app = axum::Router::new()
        .route("/v0/healthcheck", axum::routing::get(healthcheck_handler))
        .route("/v0/projects", axum::routing::post(publish_project_handler))
        .route(
            "/v0/projects/:project_hash",
            axum::routing::get(get_project_handler),
        )
        .route(
            "/v0/project-tags/:project_name/:tag",
            axum::routing::get(get_project_tag_handler),
        )
        .route(
            "/v0/blobs/:blob_id",
            axum::routing::get(get_blob_handler).put(put_blob_handler),
        )
        .route(
            "/v0/artifacts/:artifact_hash",
            axum::routing::get(get_artifact_handler).put(put_artifact_handler),
        )
        .layer(trace_layer)
        .with_state(state.clone());

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
    env: super::ServerEnv,
    object_store: Box<dyn object_store::ObjectStore>,
    object_store_path: object_store::path::Path,
    pub db_pool: sqlx::SqlitePool,
}

impl ServerState {
    pub async fn new(env: super::ServerEnv) -> eyre::Result<Self> {
        // Handle the special `relative-file` URL (primarily for development)
        let object_store_url = match env.object_store_url.scheme() {
            "relative-file" => {
                let relative_path = env.object_store_url.path();
                let relative_path = relative_path.strip_prefix('/').unwrap_or(relative_path);
                let abs_path = tokio::fs::canonicalize(std::path::Path::new(relative_path))
                    .await
                    .with_context(|| {
                        format!(
                            "failed to canonicalize relative object store path: {relative_path}"
                        )
                    })?;
                url::Url::from_directory_path(abs_path)
                    .map_err(|_| eyre::eyre!("failed to create object store URL"))?
            }
            _ => env.object_store_url.clone(),
        };
        let object_store_opts = std::env::vars().map(|(k, v)| (k.to_ascii_lowercase(), v));
        let (object_store, object_store_path) =
            object_store::parse_url_opts(&object_store_url, object_store_opts)?;

        let db_opts = sqlx::sqlite::SqliteConnectOptions::from_str(&env.database_url)?
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
        let db_filename = db_opts.clone().get_filename();
        let db_pool = sqlx::sqlite::SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(1)
            .connect_with(db_opts)
            .await?;

        tracing::info!(db_filename = %db_filename.display(), "set up database connection");

        Ok(Self {
            env,
            object_store,
            object_store_path,
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
) -> Result<axum::Json<Project>, ServerError> {
    let project_hash_value = project_hash.to_string();
    let record = sqlx::query!(
        "SELECT project_json FROM projects WHERE project_hash = ?",
        project_hash_value,
    )
    .fetch_all(&state.db_pool)
    .await
    .wrap_err("failed to fetch project from database")?;
    let record = record.first().ok_or(ServerError::NotFound)?;

    let project: Project =
        serde_json::from_str(&record.project_json).wrap_err("failed to parse project")?;
    Ok(axum::Json(project))
}

async fn get_project_tag_handler(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    axum::extract::Path((project_name, tag)): axum::extract::Path<(String, String)>,
) -> Result<axum::Json<GetProjectTagResponse>, ServerError> {
    let records = sqlx::query!(
        "SELECT project_hash FROM project_tags WHERE name = ? AND tag = ? AND is_current",
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

async fn publish_project_handler(
    Authenticated(state): Authenticated,
    axum::Json(project_listing): axum::Json<ProjectListing>,
) -> Result<(axum::http::StatusCode, axum::Json<PublishProjectResponse>), ServerError> {
    let mut new_files = 0;
    for (file_id, file_contents) in &project_listing.files {
        let file_path = state
            .object_store_path
            .child("blobs")
            .child(file_id.to_string());
        let head_result = state.object_store.head(&file_path).await;

        match head_result {
            Ok(_) => {
                // File already uploaded, so ignore
                continue;
            }
            Err(object_store::Error::NotFound { .. }) => {
                // File does not exist, so upload
            }
            Err(error) => {
                return Err(ServerError::other(
                    eyre::Error::new(error)
                        .wrap_err("failed to check if project file already exists"),
                ));
            }
        }

        state
            .object_store
            .put(&file_path, bytes::Bytes::copy_from_slice(file_contents))
            .await
            .wrap_err("failed to upload new project file")
            .map_err(ServerError::other)?;

        new_files += 1;
    }

    let mut db_transaction = state.db_pool.begin().await.map_err(ServerError::other)?;

    let mut new_projects = 0;
    for (project_hash, project) in &project_listing.projects {
        let project_hash_value = project_hash.to_string();
        let project_json = serde_json::to_string(&project).map_err(ServerError::other)?;
        let result = sqlx::query!(
            "INSERT OR IGNORE INTO projects (project_hash, project_json) VALUES (?, ?)",
            project_hash_value,
            project_json
        )
        .execute(&mut *db_transaction)
        .await
        .map_err(ServerError::other)?;

        new_projects += result.rows_affected();
    }

    let root_project_hash = project_listing.root_project;
    let root_project_hash_value = root_project_hash.to_string();
    let root_project = project_listing
        .projects
        .get(&project_listing.root_project)
        .ok_or(ServerError::BadRequest(Cow::Borrowed(
            "root project not found in project list",
        )))?;
    let root_project_name =
        root_project
            .definition
            .name
            .as_ref()
            .ok_or(ServerError::BadRequest(Cow::Borrowed(
                "cannot publish a project without a name",
            )))?;

    // Tag with `latest` plus the version number
    let root_project_tags = ["latest"]
        .into_iter()
        .chain(root_project.definition.version.as_deref());

    let mut tags = vec![];

    for tag in root_project_tags {
        let update_result = sqlx::query!(
            r#"
                UPDATE project_tags
                SET is_current = NULL
                WHERE
                    name = ?
                    AND tag = ?
                    AND project_hash <> ?
                    AND is_current
                RETURNING project_hash
            "#,
            root_project_name,
            tag,
            root_project_hash_value,
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
                ) VALUES (?, ?, ?, TRUE)
                ON CONFLICT (name, tag, is_current) DO NOTHING
                RETURNING name, tag
            "#,
            root_project_name,
            tag,
            root_project_hash_value,
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
                WHERE name = ? AND tag = ? AND is_current = TRUE
            "#,
            root_project_name,
            tag
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
        if record_project_hash != root_project_hash {
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

    tracing::info!(new_files, new_projects, root_project = %project_listing.root_project, ?tags, "published project");

    let response = PublishProjectResponse {
        root_project: project_listing.root_project,
        new_files,
        new_projects,
        tags,
    };
    Ok((axum::http::StatusCode::CREATED, axum::Json(response)))
}

async fn get_blob_handler(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    axum::extract::Path(blob_id): axum::extract::Path<BlobId>,
) -> Result<axum::response::Response, ServerError> {
    let blob_path = state
        .object_store_path
        .child("blobs")
        .child(blob_id.to_string());
    let object = state.object_store.get(&blob_path).await;
    let object = match object {
        Ok(object) => object,
        Err(object_store::Error::NotFound { .. }) => {
            return Err(ServerError::NotFound);
        }
        Err(error) => {
            return Err(ServerError::other(error));
        }
    };

    let body = match object.payload {
        object_store::GetResultPayload::File(file, _) => {
            let file = tokio::fs::File::from_std(file);
            let stream = tokio_util::io::ReaderStream::new(file);
            axum::body::Body::from_stream(stream)
        }
        object_store::GetResultPayload::Stream(stream) => axum::body::Body::from_stream(stream),
    };
    let response = axum::response::Response::builder()
        .header(axum::http::header::CONTENT_TYPE, "application/octet-stream")
        .body(body)
        .map_err(ServerError::other)?;
    Ok(response)
}

async fn put_blob_handler(
    Authenticated(state): Authenticated,
    axum::extract::Path(blob_id): axum::extract::Path<BlobId>,
    body: axum::body::Body,
) -> Result<(axum::http::StatusCode, axum::Json<BlobId>), ServerError> {
    let blob_path = state
        .object_store_path
        .child("blobs")
        .child(blob_id.to_string());

    let head = state.object_store.head(&blob_path).await;
    match head {
        Ok(_) => {
            return Err(ServerError::AlreadyExists);
        }
        Err(object_store::Error::NotFound { .. }) => {
            // Object doesn't exist, so we can create it
        }
        Err(error) => {
            return Err(ServerError::other(error));
        }
    }

    let mut hasher = blake3::Hasher::new();
    let (multipart_id, mut object_writer) = state
        .object_store
        .put_multipart(&blob_path)
        .await
        .map_err(ServerError::other)?;

    let mut body_stream = body.into_data_stream();
    while let Some(chunk) = body_stream.next().await {
        let chunk = chunk.wrap_err("failed to read next blob chunk");
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                state
                    .object_store
                    .abort_multipart(&blob_path, &multipart_id)
                    .await
                    .inspect_err(|error| {
                        tracing::warn!(?blob_path, %error, "failed to abort multipart upload");
                    })
                    .map_err(ServerError::other)?;
                return Err(ServerError::other(error));
            }
        };

        hasher.update(&chunk);

        let put_result = object_writer
            .write_all(&chunk)
            .await
            .wrap_err("failed to write blob chunk");
        match put_result {
            Ok(()) => {}
            Err(error) => {
                state
                    .object_store
                    .abort_multipart(&blob_path, &multipart_id)
                    .await
                    .inspect_err(|error| {
                        tracing::warn!(?blob_path, %error, "failed to abort multipart upload");
                    })
                    .map_err(ServerError::other)?;
                return Err(ServerError::other(error));
            }
        }
    }

    // TODO: Compare directly
    let expected_hash_string = blob_id.to_string();
    let actual_hash = hasher.finalize();
    let actual_hash_string = actual_hash.to_hex().to_string();

    if expected_hash_string != actual_hash_string {
        state
            .object_store
            .abort_multipart(&blob_path, &multipart_id)
            .await
            .inspect_err(|error| {
                tracing::warn!(?blob_path, %error, "failed to abort multipart upload");
            })
            .map_err(ServerError::other)?;
        return Err(ServerError::BadRequest(Cow::Owned(format!(
            "blob hash mismatch: expected {}, got {}",
            expected_hash_string, actual_hash_string
        ))));
    }

    object_writer
        .flush()
        .await
        .wrap_err("failed to flush blob writer")
        .map_err(ServerError::other)?;
    object_writer
        .shutdown()
        .await
        .wrap_err("failed to shutdown blob writer")?;

    Ok((axum::http::StatusCode::CREATED, axum::Json(blob_id)))
}

async fn get_artifact_handler(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    axum::extract::Path(artifact_hash): axum::extract::Path<ArtifactHash>,
) -> Result<axum::Json<LazyArtifact>, ServerError> {
    let mut db_transaction = state.db_pool.begin().await.map_err(ServerError::other)?;

    let artifact_hash_value = artifact_hash.to_string();
    let record = sqlx::query!(
        "SELECT artifact_json FROM artifacts WHERE artifact_hash = ?",
        artifact_hash_value,
    )
    .fetch_optional(&mut *db_transaction)
    .await
    .map_err(ServerError::other)?;

    db_transaction.commit().await.map_err(ServerError::other)?;

    match record {
        Some(record) => {
            let artifact: LazyArtifact = serde_json::from_str(&record.artifact_json)
                .wrap_err_with(|| {
                    format!("failed to deserialize artifact JSON with hash {artifact_hash}")
                })
                .map_err(ServerError::other)?;
            Ok(axum::Json(artifact))
        }
        None => Err(ServerError::NotFound),
    }
}

async fn put_artifact_handler(
    Authenticated(state): Authenticated,
    axum::extract::Path(artifact_hash): axum::extract::Path<ArtifactHash>,
    axum::Json(artifact): axum::Json<LazyArtifact>,
) -> Result<(axum::http::StatusCode, axum::Json<ArtifactHash>), ServerError> {
    if artifact_hash != artifact.hash() {
        return Err(ServerError::BadRequest(Cow::Owned(format!(
            "expected artifact hash to be {artifact_hash}, but was {}",
            artifact.hash()
        ))));
    }

    let mut db_transaction = state.db_pool.begin().await.map_err(ServerError::other)?;

    let artifact_hash_value = artifact_hash.to_string();
    let artifact_json_value = serde_json::to_string(&artifact).map_err(ServerError::other)?;
    let result = sqlx::query!(
        r#"
            INSERT INTO artifacts (artifact_hash, artifact_json)
            VALUES (?, ?)
            ON CONFLICT (artifact_hash) DO NOTHING
        "#,
        artifact_hash_value,
        artifact_json_value,
    )
    .execute(&mut *db_transaction)
    .await
    .map_err(ServerError::other)?;

    db_transaction.commit().await.map_err(ServerError::other)?;

    if result.rows_affected() == 0 {
        Ok((axum::http::StatusCode::OK, axum::Json(artifact_hash)))
    } else {
        Ok((axum::http::StatusCode::CREATED, axum::Json(artifact_hash)))
    }
}

struct Authenticated(Arc<ServerState>);

#[async_trait::async_trait]
impl axum::extract::FromRequestParts<Arc<ServerState>> for Authenticated {
    type Rejection = ServerError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &Arc<ServerState>,
    ) -> Result<Self, Self::Rejection> {
        let axum_extra::TypedHeader(authorization) =
            axum_extra::TypedHeader::<Authorization<Basic>>::from_request_parts(parts, &()).await?;

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
enum ServerError {
    #[error("internal error")]
    Other(#[from] eyre::Error),

    #[error("not found")]
    NotFound,

    #[error("{0}")]
    BadRequest(Cow<'static, str>),

    #[error("username or password did not match")]
    InvalidCredentials,

    #[error("resource already exists")]
    AlreadyExists,

    #[error(transparent)]
    TypedHeaderRejection(#[from] axum_extra::typed_header::TypedHeaderRejection),
}

impl ServerError {
    fn other(error: impl Into<eyre::Error>) -> Self {
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
            ServerError::TypedHeaderRejection(rejection) => rejection.into_response().status(),
        };

        (status_code, axum::response::Json(body)).into_response()
    }
}
