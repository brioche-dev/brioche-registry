use std::{borrow::Cow, str::FromStr as _, sync::Arc, time::Duration};

use argon2::PasswordVerifier;
use axum::body::Body;
use axum_extra::headers::{authorization::Basic, Authorization};
use brioche::brioche::{
    project::{Project, ProjectHash, ProjectListing},
    vfs::FileId,
};
use eyre::{Context as _, OptionExt as _};
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
        .route("/projects", axum::routing::post(publish_project_handler))
        .route(
            "/projects/:project_hash",
            axum::routing::get(get_project_handler),
        )
        .route(
            "/project-tags/:project_name/:tag",
            axum::routing::get(get_project_tag_handler),
        )
        .route("/blobs/:file_id", axum::routing::get(get_blob_handler))
        .layer(trace_layer)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let listen_addr = listener.local_addr()?;
    tracing::info!("listening on {listen_addr}");
    axum::serve(listener, app).await?;

    Ok(())
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
                let relative_path = relative_path.strip_prefix("/").unwrap_or(relative_path);
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
        let object_store_opts = std::env::vars().map(|(k, v)| (k, v.to_ascii_lowercase()));
        let (object_store, object_store_path) =
            object_store::parse_url_opts(&object_store_url, object_store_opts)?;

        let db_opts = sqlx::sqlite::SqliteConnectOptions::from_str(&env.database_url)?
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
        let db_filename = db_opts.clone().get_filename();
        let db_pool = sqlx::sqlite::SqlitePoolOptions::new()
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
}

async fn get_project_handler(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    axum::extract::Path(project_hash): axum::extract::Path<ProjectHash>,
) -> Result<axum::Json<Project>, ServerError> {
    let project_hash_bytes = project_hash.as_slice();
    let record = sqlx::query!(
        "SELECT project_json FROM projects WHERE project_hash = ?",
        project_hash_bytes,
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

    let project_hash = ProjectHash::try_from_slice(&record.project_hash)
        .map_err(|error| eyre::eyre!(error))
        .wrap_err("failed to parse project hash from database")
        .map_err(ServerError::other)?;
    let response = GetProjectTagResponse { project_hash };
    Ok(axum::Json(response))
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetProjectTagResponse {
    project_hash: ProjectHash,
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
        let result = state
            .object_store
            .put_opts(
                &file_path,
                bytes::Bytes::copy_from_slice(&file_contents),
                object_store::PutOptions {
                    mode: object_store::PutMode::Create,
                    ..Default::default()
                },
            )
            .await;

        match result {
            Ok(_) => {
                new_files += 1;
            }
            Err(object_store::Error::AlreadyExists { .. }) => {
                // File already uploaded, so ignore
            }
            Err(error) => {
                return Err(ServerError::other(error));
            }
        }
    }

    let mut db_transaction = state.db_pool.begin().await.map_err(ServerError::other)?;

    let mut new_projects = 0;
    for (project_hash, project) in &project_listing.projects {
        let project_hash_bytes = project_hash.as_slice();
        let project_json = serde_json::to_string(&project).map_err(ServerError::other)?;
        let result = sqlx::query!(
            "INSERT OR IGNORE INTO projects (project_hash, project_json) VALUES (?, ?)",
            project_hash_bytes,
            project_json
        )
        .execute(&mut *db_transaction)
        .await
        .map_err(ServerError::other)?;

        new_projects += result.rows_affected();
    }

    let root_project_hash = project_listing.root_project;
    let root_project_hash_bytes = root_project_hash.as_slice();
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
            root_project_hash_bytes,
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
            root_project_hash_bytes,
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
        if record.project_hash.as_slice() != root_project_hash_bytes {
            return Err(ServerError::Other(eyre::eyre!("project tag did not match")));
        }

        let previous_hash = update_result
            .first()
            .and_then(|record| ProjectHash::try_from_slice(&record.project_hash).ok());

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

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct PublishProjectResponse {
    root_project: ProjectHash,
    new_files: u64,
    new_projects: u64,
    tags: Vec<UpdatedTag>,
}

#[serde_with::serde_as]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpdatedTag {
    name: String,
    tag: String,
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    previous_hash: Option<ProjectHash>,
}

async fn get_blob_handler(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    axum::extract::Path(file_id): axum::extract::Path<FileId>,
) -> Result<axum::response::Response, ServerError> {
    let file_path = state
        .object_store_path
        .child("blobs")
        .child(file_id.to_string());
    let object = state.object_store.get(&file_path).await;
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
            ServerError::NotFound => axum::http::StatusCode::NOT_FOUND,
            ServerError::InvalidCredentials => axum::http::StatusCode::UNAUTHORIZED,
            ServerError::TypedHeaderRejection(rejection) => rejection.into_response().status(),
        };

        (status_code, axum::response::Json(body)).into_response()
    }
}
