use crate::{clustering, db, feeds, openai};

#[tracing::instrument(skip_all)]
pub async fn run(
    db: db::Client,
    openai_client: openai::Client,
) -> Result<(), Box<dyn std::error::Error>> {
    let executor = lightspeed_scheduler::JobExecutor::new_with_utc_tz();

    executor
        .add_job_with_scheduler(
            lightspeed_scheduler::scheduler::Scheduler::Interval {
                interval_duration: std::time::Duration::from_secs(60 * 15),
                execute_at_startup: true,
            },
            lightspeed_scheduler::job::Job::new("background", "fetch", None, move || {
                let db = db.clone();
                let openai_client = openai_client.clone();
                Box::pin(async move { fetch(&db, &openai_client).await })
            }),
        )
        .await;

    executor.run().await?;

    Ok(())
}

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

#[tracing::instrument(skip_all)]
async fn fetch(db: &db::Client, openai_client: &openai::Client) -> Result<(), Error> {
    let translator = openai::Translator::new(openai_client);
    let crawler = feeds::Crawler::default();

    crawl(db, &crawler).await?;
    translate(db, &translator).await?;
    generate_embeddings(db, openai_client).await?;

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

#[tracing::instrument(skip_all)]
async fn crawl(db: &db::Client, crawler: &feeds::Crawler) -> Result<(), Error> {
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

#[tracing::instrument(skip_all)]
async fn translate(db: &db::Client, translator: &openai::Translator<'_>) -> Result<(), Error> {
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

#[tracing::instrument(skip_all)]
async fn generate_embeddings(db: &db::Client, openai_client: &openai::Client) -> Result<(), Error> {
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
