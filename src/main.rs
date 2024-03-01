mod clustering;
mod db;
mod feeds;
mod openai;

use clap::{Parser, Subcommand};

#[derive(Parser)]
struct Cli {
    #[arg(long, default_value = "database.sqlite3")]
    database_file: std::path::PathBuf,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Fetch {
        #[arg(long)]
        openai_token: String,
        #[arg(long, default_value = "https://api.openai.com/")]
        openai_base_url: url::Url,
    },
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

    match cli.command {
        Commands::Fetch {
            openai_token,
            openai_base_url,
        } => fetch(&db, &openai_token, &openai_base_url).await,
    }
}

#[allow(clippy::too_many_lines)]
async fn fetch(
    db: &db::Client,
    openai_token: &str,
    openai_base_url: &url::Url,
) -> Result<(), Box<dyn std::error::Error>> {
    let openai_client = openai::Client::new(openai_base_url, openai_token);
    let translator = openai::Translator::new(&openai_client);
    let crawler = feeds::Crawler::default();

    fetch_feeds(db, &crawler).await?;

    let untranslated_sv_fields = db.list_sv_fields_without_en_translation().await?;

    for sv_field in untranslated_sv_fields {
        let sv_translation = db
            .find_translation_by_md5_hash(&sv_field.value.md5_hash)
            .await?;

        let translation = translator
            .translate_sv_to_en(&sv_translation.value.value)
            .await?;

        let en_translation = feeds::Translation {
            md5_hash: md5::compute(&translation),
            value: translation,
        };
        db.insert_translation(&en_translation).await?;

        let en_field = feeds::Field {
            lang_code: feeds::LanguageCode::EN,
            md5_hash: en_translation.md5_hash,
            ..sv_field.value
        };
        db.insert_field(&en_field).await?;
    }

    let translations_without_embeddings = db
        .list_translations_without_embeddings(feeds::LanguageCode::EN)
        .await?;

    for translation in translations_without_embeddings {
        let embedding = openai_client.embeddings(&translation.value.value).await?;

        let embedding = db::Embedding {
            md5_hash: translation.value.md5_hash,
            size: embedding
                .len()
                .try_into()
                .expect("failed to convert usize into u32"),
            value: embedding,
        };
        db.insert_embeddig(&embedding).await?;
    }

    let today_en_title_embeddings = db
        .list_embeddings_by_field_name_lang_code_date(
            feeds::FieldName::Title,
            feeds::LanguageCode::EN,
            chrono::Utc::now().date_naive(),
        )
        .await?;

    let (clusters, score) =
        clustering::group_embeddings(&today_en_title_embeddings, 2, 0.75..=1.0).await;
    let report = db::Report {
        groups: clusters,
        score,
    };

    db.insert_report(&report).await?;

    let latest_report = db.find_latest_report().await?;

    let mut lines = vec![];
    for group in latest_report.value.groups {
        lines.push(String::new());
        for embedding_id in group {
            let embedding = db.find_embedding_by_id(embedding_id).await?;

            let translation = db
                .find_translation_by_md5_hash(&embedding.value.md5_hash)
                .await?;

            let fields = db.list_fields_md5_hash(&embedding.value.md5_hash).await?;

            for field in fields {
                let entry = db.find_entry_by_id(field.value.entry_id).await?;

                lines.push(format!(
                    "- {} ({})",
                    translation.value.value, entry.value.href
                ));
            }
        }
    }

    std::fs::write("./report.txt", lines.join("\n")).expect("failed to save report");

    Ok(())
}

async fn fetch_feeds(
    db: &db::Client,
    crawler: &feeds::Crawler,
) -> Result<(), Box<dyn std::error::Error>> {
    let feeds = db.list_feeds().await?;

    let entries =
        futures::future::try_join_all(feeds.iter().map(|feed| crawler.crawl(feed))).await?;

    for (entry, fields) in entries.into_iter().flatten() {
        if let Some(entry) = db.insert_entry(&entry).await? {
            let fields = fields.into_iter().map(|(name, lang_code, value)| {
                let md5_hash = md5::compute(&value);
                (
                    feeds::Field {
                        entry_id: entry.id,
                        name,
                        lang_code,
                        md5_hash,
                    },
                    feeds::Translation {
                        value: value.to_string(),
                        md5_hash,
                    },
                )
            });

            for (sv_field, sv_translation) in fields {
                futures::try_join!(
                    db.insert_field(&sv_field),
                    db.insert_translation(&sv_translation)
                )?;
            }
        }
    }

    Ok(())
}
