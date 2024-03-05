use crate::{clustering, db, feeds, id::Id, md5_hash, openai};

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
    let crawler = feeds::Crawler::default();

    crawl(db, &crawler).await?;
    generate_embeddings(db, openai_client).await?;
    generate_report(db, openai_client).await?;

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
                let md5_hash = md5_hash::compute(&value);
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
                    db.insert_field(sv_field),
                    db.insert_translation(sv_translation)
                )?;
            }
        }
    }

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn generate_embeddings(db: &db::Client, openai_client: &openai::Client) -> Result<(), Error> {
    let translations_without_embeddings = db
        .list_translations_without_embeddings_by_lang_code_date(
            feeds::LanguageCode::SV,
            &chrono::Utc::now().date_naive(),
        )
        .await?;

    for translation in translations_without_embeddings {
        let embedding = openai_client.embeddings(&translation.value.value).await?;

        db.insert_embeddig(&clustering::Embedding {
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

#[tracing::instrument(skip_all)]
async fn generate_report(db: &db::Client, openai_client: &openai::Client) -> Result<(), Error> {
    let today_title_embeddings = db
        .list_embeddings_by_field_name_lang_code_date(
            feeds::FieldName::Title,
            feeds::LanguageCode::SV,
            chrono::Utc::now().date_naive(),
        )
        .await?;

    let (groups, score) =
        clustering::group_embeddings(&today_title_embeddings, 3, 0.75..=1.0).await;

    let translator = openai::Translator::new(openai_client);
    futures::future::try_join_all(
        groups
            .iter()
            .flatten()
            .map(|id| translate(db, &translator, id, &feeds::LanguageCode::EN)),
    )
    .await?;

    let report = db.insert_report(&clustering::Report { score }).await?;
    futures::future::try_join_all(groups.into_iter().map(|embedding_ids| {
        db.insert_report_group(clustering::ReportGroup {
            report_id: report.id,
            embedding_ids,
        })
    }))
    .await?;

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn translate(
    db: &db::Client,
    translator: &openai::Translator<'_>,
    embedding_id: &Id<clustering::Embedding>,
    lang_code: &feeds::LanguageCode,
) -> Result<(), Error> {
    let embedding = db.find_embedding_by_id(embedding_id).await?;
    let (translation, fields) = futures::future::try_join(
        db.find_translation_by_md5_hash(&embedding.value.md5_hash),
        db.list_fields_by_md5_hash(&embedding.value.md5_hash),
    )
    .await?;

    if fields.is_empty() {
        return Ok(());
    }

    let translated = futures::future::try_join_all(
        fields
            .iter()
            .map(|field| db.find_field_by_entry_id_lang_code(&field.value.entry_id, lang_code)),
    )
    .await?;

    if translated.iter().all(Option::is_some) {
        return Ok(());
    }

    let to_translate = fields
        .iter()
        .enumerate()
        .filter_map(|(i, field)| {
            if translated[i].is_none() {
                Some(field)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    let translation = translator
        .translate_sv_to_en(&translation.value.value)
        .await?;
    let md5_hash = md5_hash::compute(&translation);

    futures::future::try_join(
        futures::future::try_join_all(to_translate.iter().map(|_| {
            db.insert_translation(feeds::Translation {
                md5_hash: md5_hash.clone(),
                value: translation.clone(),
            })
        })),
        futures::future::try_join_all(to_translate.iter().map(|field| {
            db.insert_field(feeds::Field {
                md5_hash,
                lang_code: feeds::LanguageCode::EN,
                ..field.value.clone()
            })
        })),
    )
    .await?;

    Ok(())
}
