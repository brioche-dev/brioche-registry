use std::io::BufRead as _;

use argon2::PasswordHasher as _;
use clap::Parser;
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

async fn serve(addr: &std::net::SocketAddr) -> eyre::Result<()> {
    let app = axum::Router::new().route("/", axum::routing::get(root_handler));

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
        password_tx.send(password).unwrap();
    });

    let password = password_rx.await?;

    let salt =
        argon2::password_hash::SaltString::generate(&mut argon2::password_hash::rand_core::OsRng);
    let argon2 = argon2::Argon2::default();
    let password_hash = argon2.hash_password(password.as_bytes(), &salt)?;

    println!("BRIOCHE_REGISTRY_PASSWORD_HASH={password_hash}");

    Ok(())
}

async fn root_handler() -> &'static str {
    "Hello, World!"
}
