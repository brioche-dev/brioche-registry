use clap::Parser;
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

#[derive(Debug, Parser)]
enum Args {
    Serve {
        #[clap(default_value = "0.0.0.0:2000")]
        addr: std::net::SocketAddr,
    },
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

async fn root_handler() -> &'static str {
    "Hello, World!"
}
