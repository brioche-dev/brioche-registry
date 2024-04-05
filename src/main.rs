use std::{io::BufRead as _, sync::Arc};

use argon2::{PasswordHasher as _, PasswordVerifier};
use axum_extra::headers::{authorization::Basic, Authorization};
use clap::Parser;
use eyre::Context as _;
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

#[derive(Debug, Parser)]
enum Args {
    Serve {
        #[clap(default_value = "0.0.0.0:2000")]
        addr: std::net::SocketAddr,
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
        Args::Serve { addr } => {
            serve(&addr).await?;
        }
        Args::HashPassword => {
            hash_password().await?;
        }
    }

    Ok(())
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ServerEnv {
    password_hash: String,
}

struct ServerState {
    env: ServerEnv,
}

impl ServerState {
    fn new(env: ServerEnv) -> Self {
        Self { env }
    }

    fn password_hash(&self) -> eyre::Result<argon2::PasswordHash<'_>> {
        let password_hash =
            argon2::PasswordHash::new(&self.env.password_hash).wrap_err("invalid password hash")?;
        Ok(password_hash)
    }
}

async fn serve(addr: &std::net::SocketAddr) -> eyre::Result<()> {
    let env: ServerEnv = envy::prefixed("BRIOCHE_REGISTRY_")
        .from_env()
        .wrap_err("failed to loan environment variables (see .env.example for example config)")?;
    let state = Arc::new(ServerState::new(env));
    let app = axum::Router::new()
        .route("/", axum::routing::get(root_handler))
        .route("/admin", axum::routing::get(admin_handler))
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

async fn root_handler() -> &'static str {
    "Hello, World!"
}

async fn admin_handler(_: Authenticated) -> &'static str {
    "Hello, admin"
}

struct Authenticated(());

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
            Ok(Self(()))
        } else {
            Err(HttpError::InvalidCredentials)
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum HttpError {
    #[error("internal error")]
    Other(#[from] eyre::Error),

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
            HttpError::InvalidCredentials => axum::http::StatusCode::UNAUTHORIZED,
            HttpError::TypedHeaderRejection(rejection) => rejection.into_response().status(),
        };

        (status_code, axum::response::Json(body)).into_response()
    }
}
