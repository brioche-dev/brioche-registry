use std::{io::BufRead as _, str::FromStr as _, sync::Arc, time::Duration};

use argon2::{PasswordHasher as _, PasswordVerifier};
use axum::body::Body;
use axum_extra::headers::{authorization::Basic, Authorization};
use brioche::brioche::{project::ProjectListing, vfs::FileId};
use clap::Parser;
use eyre::{Context as _, OptionExt};
use tracing::Span;
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

#[derive(Debug, Parser)]
enum Args {
    Migrate,
    Serve {
        #[clap(default_value = "0.0.0.0:2000")]
        addr: std::net::SocketAddr,
        #[clap(long)]
        no_migrate: bool,
    },
    HashPassword,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    match dotenvy::dotenv() {
        Ok(_) => {}
        Err(error) if error.not_found() => {}
        Err(error) => eyre::bail!(error),
    }

    color_eyre::install()?;
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive("brioche_registry=info".parse().expect("invalid filter"))
                .from_env_lossy(),
        )
        .init();

    let args = Args::parse();

    match args {
        Args::Migrate => {
            migrate().await?;
        }
        Args::Serve { addr, no_migrate } => {
            serve(&addr, no_migrate).await?;
        }
        Args::HashPassword => {
            hash_password().await?;
        }
    }

    Ok(())
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ServerEnv {
    database_url: String,
    object_store_url: url::Url,
    password_hash: String,
}

struct ServerState {
    env: ServerEnv,
    object_store: Box<dyn object_store::ObjectStore>,
    object_store_path: object_store::path::Path,
    db_pool: sqlx::SqlitePool,
}

impl ServerState {
    async fn new(env: ServerEnv) -> eyre::Result<Self> {
        let object_store_opts = std::env::vars().map(|(k, v)| (k, v.to_ascii_lowercase()));
        let (object_store, object_store_path) =
            object_store::parse_url_opts(&env.object_store_url, object_store_opts)?;

        let db_opts = sqlx::sqlite::SqliteConnectOptions::from_str(&env.database_url)?
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
        let db_pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect_with(db_opts)
            .await?;

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

async fn migrate() -> eyre::Result<()> {
    let env: ServerEnv = envy::prefixed("BRIOCHE_REGISTRY_")
        .from_env()
        .wrap_err("failed to loan environment variables (see .env.example for example config)")?;
    let state = ServerState::new(env).await?;

    sqlx::migrate!().run(&state.db_pool).await?;

    Ok(())
}

async fn serve(addr: &std::net::SocketAddr, no_migrate: bool) -> eyre::Result<()> {
    let env: ServerEnv = envy::prefixed("BRIOCHE_REGISTRY_")
        .from_env()
        .wrap_err("failed to loan environment variables (see .env.example for example config)")?;
    let state = ServerState::new(env).await?;
    let state = Arc::new(state);

    if !no_migrate {
        tracing::info!("running database migrations");
        sqlx::migrate!().run(&state.db_pool).await?;
    }

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
        .route("/blobs/:file_id", axum::routing::get(get_blob_handler))
        .layer(trace_layer)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let listen_addr = listener.local_addr()?;
    tracing::info!("listening on {listen_addr}");
    axum::serve(listener, app).await?;

    Ok(())
}

async fn hash_password() -> eyre::Result<()> {
    let (password_tx, password_rx) = tokio::sync::oneshot::channel();

    std::thread::spawn(move || {
        eprint!("Enter a password: ");

        let mut password = String::new();
        let mut stdin = std::io::stdin().lock();
        stdin.read_line(&mut password).unwrap();

        // Remove the trailing newline from the password
        let password = password.lines().next().unwrap().to_string();

        password_tx.send(password).unwrap();
    });

    let password = password_rx.await?;

    let salt =
        argon2::password_hash::SaltString::generate(&mut argon2::password_hash::rand_core::OsRng);
    let argon2 = argon2::Argon2::default();
    let password_hash = argon2.hash_password(password.as_bytes(), &salt)?;

    println!("BRIOCHE_REGISTRY_PASSWORD_HASH='{password_hash}'");

    Ok(())
}

async fn publish_project_handler(
    Authenticated(state): Authenticated,
    project_listing: axum::Json<ProjectListing>,
) -> Result<axum::http::StatusCode, HttpError> {
    let mut subprojects = vec![project_listing.root_project];

    // TODO: Parallelize upload
    while let Some(subproject_hash) = subprojects.pop() {
        let subproject = project_listing
            .projects
            .get(&subproject_hash)
            .ok_or_eyre("subproject not found in project listing")?;
        for dep_hash in subproject.dependencies.values() {
            subprojects.push(*dep_hash);
        }
        for module_id in subproject.modules.values() {
            let module_contents = project_listing
                .files
                .get(module_id)
                .ok_or_eyre("module not found in project listing")?;
            let module_path = state
                .object_store_path
                .child("blobs")
                .child(module_id.to_string());

            state
                .object_store
                .put(
                    &module_path,
                    bytes::Bytes::copy_from_slice(&module_contents),
                )
                .await
                .wrap_err("failed to upload module to object store")?;
        }
    }

    Ok(axum::http::StatusCode::NO_CONTENT)
}

async fn get_blob_handler(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    axum::extract::Path(file_id): axum::extract::Path<FileId>,
) -> Result<axum::response::Response, HttpError> {
    let file_path = state
        .object_store_path
        .child("blobs")
        .child(file_id.to_string());
    let object = state.object_store.get(&file_path).await;
    let object = match object {
        Ok(object) => object,
        Err(object_store::Error::NotFound { .. }) => {
            return Err(HttpError::NotFound);
        }
        Err(error) => {
            return Err(HttpError::Other(error.into()));
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
        .map_err(|err| HttpError::Other(err.into()))?;
    Ok(response)
}

struct Authenticated(Arc<ServerState>);

#[async_trait::async_trait]
impl axum::extract::FromRequestParts<Arc<ServerState>> for Authenticated {
    type Rejection = HttpError;

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
            Err(HttpError::InvalidCredentials)
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum HttpError {
    #[error("internal error")]
    Other(#[from] eyre::Error),

    #[error("not found")]
    NotFound,

    #[error("username or password did not match")]
    InvalidCredentials,

    #[error(transparent)]
    TypedHeaderRejection(#[from] axum_extra::typed_header::TypedHeaderRejection),
}

impl axum::response::IntoResponse for HttpError {
    fn into_response(self) -> axum::response::Response {
        let body = serde_json::json!({
            "error": self.to_string(),
        });

        let status_code = match self {
            HttpError::Other(error) => {
                tracing::error!(error = %error, "internal error");
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            }
            HttpError::NotFound => axum::http::StatusCode::NOT_FOUND,
            HttpError::InvalidCredentials => axum::http::StatusCode::UNAUTHORIZED,
            HttpError::TypedHeaderRejection(rejection) => rejection.into_response().status(),
        };

        (status_code, axum::response::Json(body)).into_response()
    }
}
