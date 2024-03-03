mod background;
mod clustering;
mod db;
mod feeds;
mod openai;
mod web;

use clap::Parser;

#[derive(Parser)]
struct Cli {
    #[arg(long, default_value = "database.sqlite3")]
    database_file: std::path::PathBuf,
    #[arg(long)]
    openai_token: String,
    #[arg(long, default_value = "https://api.openai.com/")]
    openai_base_url: url::Url,
    #[arg(long, default_value = "127.0.0.1:8080")]
    address: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = tracing_subscriber::fmt::fmt()
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::NEW
                | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        )
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let cli = Cli::parse();
    let db = db::Client::new(cli.database_file)
        .await
        .expect("failed to create db client");
    let openai_client = openai::Client::new(&cli.openai_base_url, &cli.openai_token);

    futures::future::try_join(
        web::serve(db.clone(), &cli.address),
        background::run(db, openai_client),
    )
    .await?;

    Ok(())
}
