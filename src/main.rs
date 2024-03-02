mod clustering;
mod db;
mod feeds;
mod openai;
mod web;

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
    Serve {
        #[arg(long, default_value = "127.0.0.1:8080")]
        address: String,
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
        Commands::Serve { address } => web::serve(&db, &address).await,
    }
}

async fn fetch(
    db: &db::Client,
    openai_token: &str,
    openai_base_url: &url::Url,
) -> Result<(), Box<dyn std::error::Error>> {
    let openai_client = openai::Client::new(openai_base_url, openai_token);
    let translator = openai::Translator::new(&openai_client);
    let crawler = feeds::Crawler::default();

    crawl(db, &crawler).await?;
    translate(db, &translator).await?;
    generate_embedding(db, &openai_client).await?;

    let today_en_title_embeddings = db
        .list_embeddings_by_field_name_lang_code_date(
            feeds::FieldName::Title,
            feeds::LanguageCode::EN,
            chrono::Utc::now().date_naive(),
        )
        .await?;

    let (groups, score) =
        clustering::group_embeddings(&today_en_title_embeddings, 2, 0.75..=1.0).await;

    let report = db.insert_report(&db::Report { score }).await?;

    for embedding_ids in groups {
        let group = db::ReportGroup {
            report_id: report.id,
            embedding_ids,
        };
        db.insert_report_group(&group).await?;
    }

    Ok(())
}

async fn crawl(
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

async fn translate(
    db: &db::Client,
    translator: &openai::Translator<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    let untranslated_sv_fields = db
        .list_sv_fields_without_en_translation_by_date(&chrono::Utc::now().date_naive())
        .await?;

    for sv_field in untranslated_sv_fields {
        let sv_translation = db
            .find_translation_by_md5_hash(&sv_field.value.md5_hash)
            .await?;

        let translation = translator
            .translate_sv_to_en(&sv_translation.value.value)
            .await?;

        let md5_hash = md5::compute(&translation);
        futures::future::try_join(
            db.insert_translation(&feeds::Translation {
                md5_hash,
                value: translation,
            }),
            db.insert_field(&feeds::Field {
                md5_hash,
                lang_code: feeds::LanguageCode::EN,
                ..sv_field.value
            }),
        )
        .await?;
    }

    Ok(())
}

async fn generate_embedding(
    db: &db::Client,
    openai_client: &openai::Client<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    let translations_without_embeddings = db
        .list_translations_without_embeddings_by_lang_code_date(
            feeds::LanguageCode::EN,
            &chrono::Utc::now().date_naive(),
        )
        .await?;

    for translation in translations_without_embeddings {
        let embedding = openai_client.embeddings(&translation.value.value).await?;

        db.insert_embeddig(&db::Embedding {
            md5_hash: translation.value.md5_hash,
            size: embedding
                .len()
                .try_into()
                .expect("failed to convert usize into u32"),
            value: embedding,
        })
        .await?;
    }
    Ok(())
}
