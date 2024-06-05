use std::{io::BufRead as _, sync::Arc};

use argon2::PasswordHasher as _;
use clap::Parser;
use eyre::Context as _;
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

mod blob;
mod object_store;
mod recipe;
mod server;

#[derive(Debug, Parser)]
enum Args {
    Migrate,
    Serve {
        #[arg(default_value = "0.0.0.0:2000")]
        addr: std::net::SocketAddr,
        #[arg(long)]
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

    let mut fmt_layer = None;
    let mut json_layer = None;
    match std::env::var("LOG_FORMAT").as_deref() {
        Ok("json") => {
            json_layer = Some(tracing_subscriber::fmt::layer().json());
        }
        _ => {
            fmt_layer = Some(tracing_subscriber::fmt::layer());
        }
    }

    color_eyre::install()?;
    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(json_layer)
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
    proxy_layers: Option<usize>,
    public_object_store_base_url: Option<url::Url>,
}

async fn migrate() -> eyre::Result<()> {
    let env: ServerEnv = envy::prefixed("BRIOCHE_REGISTRY_")
        .from_env()
        .wrap_err("failed to load environment variables (see .env.example for example config)")?;
    let state = server::ServerState::new(env).await?;

    sqlx::migrate!().run(&state.db_pool).await?;

    Ok(())
}

async fn serve(addr: &std::net::SocketAddr, no_migrate: bool) -> eyre::Result<()> {
    let env: ServerEnv = envy::prefixed("BRIOCHE_REGISTRY_")
        .from_env()
        .wrap_err("failed to load environment variables (see .env.example for example config)")?;
    let state = server::ServerState::new(env).await?;
    let state = Arc::new(state);

    if !no_migrate {
        tracing::info!("running database migrations");
        sqlx::migrate!().run(&state.db_pool).await?;
    }

    server::start_server(state, addr).await?;

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
