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
use eyre::{Context as _, OptionExt as _};
use password_hash::PasswordHashString;
use tracing::Span;

pub async fn start_server(state: Arc<ServerState>, addr: &SocketAddr) -> eyre::Result<()> {
    let proxy_layers = state.proxy_layers;
    let trace_layer = tower_http::trace::TraceLayer::new_for_http()
        .make_span_with(move |req: &axum::http::Request<Body>| {
            let request_id = ulid::Ulid::new();

            // Determine client IP based on proxy configuration
            let client_ip = {
                // Try X-Forwarded-For first if we're behind proxies
                let forwarded_ip = if proxy_layers > 0 {
                    req.headers()
                        .get_all("X-Forwarded-For")
                        .into_iter()
                        .flat_map(|header| header.as_bytes().split(|&b| b == b','))
                        .nth(proxy_layers.saturating_sub(1))
                        .map(|value| String::from_utf8_lossy(value.trim_ascii()))
                } else {
                    None
                };

                // Fall back to direct connection IP only if needed
                forwarded_ip.unwrap_or_else(|| {
                    req.extensions()
                        .get::<axum::extract::ConnectInfo<SocketAddr>>()
                        // And as a last resort, fallback to "<unknown>"
                        .map_or(Cow::Borrowed("<unknown>"), |connect_info| {
                            Cow::Owned(connect_info.0.ip().to_string())
                        })
                })
            };

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
        .layer(tower_http::compression::CompressionLayer::new())
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
    pub proxy_layers: usize,
    pub db_pool: sqlx::PgPool,
    password_hash: PasswordHashString,
}

impl ServerState {
    pub async fn new(env: super::ServerEnv) -> eyre::Result<Self> {
        let password_hash = PasswordHashString::new(&env.password_hash)
            .map_err(|error| eyre::eyre!(error))
            .wrap_err("invalid password hash format")?;

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
            proxy_layers,
            db_pool,
            password_hash,
        })
    }

    fn password_hash(&self) -> argon2::PasswordHash<'_> {
        self.password_hash.password_hash()
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

#[tracing::instrument(skip_all, fields(tag_count = project_tags.tags.len()))]
async fn bulk_create_project_tags_handler(
    Authenticated(state): Authenticated,
    axum::Json(project_tags): axum::Json<CreateProjectTagsRequest>,
) -> Result<axum::Json<CreateProjectTagsResponse>, ServerError> {
    let mut db_transaction = state.db_pool.begin().await.map_err(ServerError::other)?;

    // Pre-allocate capacity for tags vector with the number of tags
    let mut tags = Vec::with_capacity(project_tags.tags.len());

    for tag in &project_tags.tags {
        let project_hash_value = tag.project_hash.blake3().to_hex();
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
            project_hash_value.as_str(),
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
            project_hash_value.as_str(),
        )
        .fetch_all(&mut *db_transaction)
        .await
        .map_err(ServerError::other)?;

        // Run a query to make sure the current tag matches the expected
        // project hash. This is a sanity check

        let record = sqlx::query!(
            r#"
                SELECT project_hash
                FROM project_tags
                WHERE name = $1 AND tag = $2 AND is_current = TRUE
            "#,
            tag.project_name,
            tag.tag,
        )
        .fetch_optional(&mut *db_transaction)
        .await
        .wrap_err("failed to get updated project tag")
        .map_err(ServerError::other)?
        .ok_or_eyre("updated project tag not found")
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

        let new_tags_iter = inserted_records.into_iter().map(|record| UpdatedTag {
            name: record.name,
            tag: record.tag,
            previous_hash,
        });
        tags.extend(new_tags_iter);
    }

    db_transaction.commit().await.map_err(ServerError::other)?;

    Ok(axum::Json(CreateProjectTagsResponse { tags }))
}

#[tracing::instrument(skip(state))]
async fn get_project_tag_handler(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    axum::extract::Path((project_name, tag)): axum::extract::Path<(String, String)>,
) -> Result<axum::Json<GetProjectTagResponse>, ServerError> {
    let record = sqlx::query!(
        "SELECT project_hash FROM project_tags WHERE name = $1 AND tag = $2 AND is_current",
        project_name,
        tag,
    )
    .fetch_optional(&state.db_pool)
    .await
    .wrap_err("failed to fetch project tag from database")?
    .ok_or(ServerError::NotFound)?;

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
            .verify_password(authorization.password().as_bytes(), &state.password_hash())
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
