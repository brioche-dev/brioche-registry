use std::{borrow::Cow, net::SocketAddr, str::FromStr as _, sync::Arc, time::Duration};

use argon2::PasswordVerifier;
use axum::body::Body;
use axum_extra::headers::{Authorization, authorization::Basic};
use brioche_core::{
    project::ProjectHash,
    registry::{
        CreateProjectTagsRequest, CreateProjectTagsResponse, GetProjectTagResponse, UpdatedTag,
    },
};
use bstr::ByteSlice as _;
use eyre::{Context as _, OptionExt as _};
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
            let client_ips = std::iter::once(Cow::Borrowed(received_ip)).chain(forwarded_for);
            let client_ip = client_ips
                .take(proxy_layers + 1)
                .last()
                .unwrap_or(Cow::Borrowed("<unknown>"));

            let user_agent = req
                .headers()
                .get("User-Agent")
                .map_or("<unknown>", |user_agent| {
                    user_agent.to_str().unwrap_or("<invalid>")
                });

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
            tracing::info!("received request");
        })
        .on_response(
            |res: &axum::http::Response<Body>, latency: Duration, _span: &Span| {
                tracing::info!(
                    status = res.status().as_u16(),
                    latency = latency.as_secs_f32(),
                    "response"
                );
            },
        )
        .on_failure(
            |err: tower_http::classify::ServerErrorsFailureClass,
             latency: Duration,
             _span: &Span| {
                tracing::error!(error = %err, latency = latency.as_secs_f32(), "request failed");
            },
        );

    let app = axum::Router::new()
        .route("/v0/healthcheck", axum::routing::get(healthcheck_handler))
        .route(
            "/v0/project-tags",
            axum::routing::post(bulk_create_project_tags_handler),
        )
        .route(
            "/v0/project-tags/{project_name}/{tag}",
            axum::routing::get(get_project_tag_handler),
        )
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
    pub db_pool: sqlx::PgPool,
}

impl ServerState {
    pub async fn new(env: super::ServerEnv) -> eyre::Result<Self> {
        let db_opts = sqlx::postgres::PgConnectOptions::from_str(&env.database_url)?;
        let mut db_pool_opts = sqlx::postgres::PgPoolOptions::new();

        if let Some(min_connections) = env.database_min_connections {
            db_pool_opts = db_pool_opts.min_connections(min_connections);
        }
        if let Some(max_connections) = env.database_max_connections {
            db_pool_opts = db_pool_opts.max_connections(max_connections);
        }
        if let Some(acquire_timeout_seconds) = env.database_acquire_timeout_seconds {
            db_pool_opts =
                db_pool_opts.acquire_timeout(Duration::from_secs(acquire_timeout_seconds.into()));
        }
        if let Some(max_lifetime_seconds) = env.database_max_lifetime_seconds {
            db_pool_opts =
                db_pool_opts.max_lifetime(Duration::from_secs(max_lifetime_seconds.into()));
        }
        if let Some(idle_timeout_seconds) = env.database_idle_timeout_seconds {
            db_pool_opts =
                db_pool_opts.idle_timeout(Duration::from_secs(idle_timeout_seconds.into()));
        }

        let db_pool = db_pool_opts.connect_with(db_opts).await?;

        let proxy_layers = env.proxy_layers.unwrap_or(0);

        tracing::info!("set up database connection");

        Ok(Self {
            env,
            proxy_layers,
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
                name: record.name.clone(),
                tag: record.tag.clone(),
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

struct Authenticated(Arc<ServerState>);

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

    #[error("credentials required")]
    CredentialsRequired,

    #[error("username or password did not match")]
    InvalidCredentials,

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
            Self::Other(error) => {
                tracing::error!("internal error: {error:#}");
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            }
            Self::NotFound => axum::http::StatusCode::NOT_FOUND,
            Self::InvalidCredentials => axum::http::StatusCode::UNAUTHORIZED,
            Self::CredentialsRequired => axum::http::StatusCode::FORBIDDEN,
            Self::TypedHeaderRejection(rejection) => rejection.into_response().status(),
        };

        (status_code, axum::response::Json(body)).into_response()
    }
}
