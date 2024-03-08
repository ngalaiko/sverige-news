use crate::{clustering, db, feeds, id::Id, md5_hash, normalizer::normalize_sv, openai};

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
                Box::pin(async move {
                    fetch(&db, &openai_client).await.map_err(|error| {
                        tracing::error!("background fetch failed: {}", error);
                        error
                    })
                })
            }),
        )
        .await;

    executor.run().await?;

    Ok(())
}

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

#[tracing::instrument(skip_all)]
async fn fetch(db: &db::Client, openai_client: &openai::Client) -> Result<(), Error> {
    crawl(db).await?;
    generate_embeddings(db, openai_client).await?;
    generate_report(db, openai_client).await?;

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn crawl(db: &db::Client) -> Result<(), Error> {
    let http_client = reqwest::ClientBuilder::new()
        .user_agent("svergie news crawler")
        .build()?;

    let (
        abc_entries,
        dagen_entries,
        aftonbladet_entries,
        dn_entries,
        expressen_entries,
        nkpg_entries,
        scaraborgs_entries,
        svd_entries,
        svt_entries,
        tv4_entries,
    ) = futures::try_join!(
        feeds::abc::crawl(&http_client),
        feeds::aftonbladet::crawl(&http_client),
        feeds::dagen::crawl(&http_client),
        feeds::dn::crawl(&http_client),
        feeds::expressen::crawl(&http_client),
        feeds::nkpg::crawl(&http_client),
        feeds::scaraborgs::crawl(&http_client),
        feeds::svd::crawl(&http_client),
        feeds::svt::crawl(&http_client),
        feeds::tv4::crawl(&http_client),
    )?;

    let entries = []
        .into_iter()
        .chain(abc_entries.into_iter())
        .chain(aftonbladet_entries.into_iter())
        .chain(dagen_entries.into_iter())
        .chain(dn_entries.into_iter())
        .chain(expressen_entries.into_iter())
        .chain(nkpg_entries.into_iter())
        .chain(scaraborgs_entries.into_iter())
        .chain(svd_entries.into_iter())
        .chain(svt_entries.into_iter())
        .chain(tv4_entries.into_iter());

    for (entry, fields) in entries {
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
        .list_translations_without_embeddings_by_lang_code_field_name_date(
            feeds::LanguageCode::SV,
            feeds::FieldName::Description,
            &chrono::Utc::now().date_naive(),
        )
        .await?;

    for translation in translations_without_embeddings {
        let text = normalize_sv(&translation.value.value);
        let embedding = openai_client.embeddings(&text).await?;

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
            feeds::FieldName::Description,
            feeds::LanguageCode::SV,
            chrono::Utc::now().date_naive(),
        )
        .await?;

    let (groups, (min_points, tolerance), score) =
        clustering::group_embeddings(&today_title_embeddings).await;

    // ensure that all translations are available
    let translator = openai::Translator::new(openai_client);
    futures::future::try_join_all(groups.iter().flat_map(|(group, _)| group).map(|id| {
        translate(
            db,
            &translator,
            id,
            &feeds::FieldName::Title,
            &feeds::LanguageCode::EN,
        )
    }))
    .await?;

    let report = db
        .insert_report(&clustering::Report {
            score,
            tolerance,
            min_points: min_points.try_into().expect("usize -> u32 failed"),
            rows: today_title_embeddings
                .len()
                .try_into()
                .expect("usize -> u32 failed"),
            dimentions: today_title_embeddings[0].value.size,
        })
        .await?;

    futures::future::try_join_all(groups.into_iter().map(|(embedding_ids, center)| {
        db.insert_report_group(clustering::ReportGroup {
            report_id: report.id,
            center_embedding_id: embedding_ids[center],
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
    field_name: &feeds::FieldName,
    lang_code: &feeds::LanguageCode,
) -> Result<(), Error> {
    let embedding = db.find_embedding_by_id(embedding_id).await?;
    let fields = db
        .list_fields_by_md5_hash(&embedding.value.md5_hash)
        .await?;
    if fields.is_empty() {
        return Ok(());
    }

    let fields = futures::future::try_join_all(fields.iter().map(|field| {
        futures::future::try_join(
            db.find_field_by_entry_id_name_lang_code(
                &field.value.entry_id,
                field_name,
                &field.value.lang_code,
            ),
            db.find_field_by_entry_id_name_lang_code(&field.value.entry_id, field_name, lang_code),
        )
    }))
    .await?;

    let to_translate = fields
        .into_iter()
        .filter_map(|(original, translation)| {
            if translation.is_some() {
                None
            } else {
                original
            }
        })
        .collect::<Vec<_>>();

    let originals = futures::future::try_join_all(
        to_translate
            .iter()
            .map(|field| db.find_translation_by_md5_hash(&field.value.md5_hash)),
    )
    .await?;

    for (field, original) in to_translate.into_iter().zip(originals) {
        let translation = translator.translate_sv_to_en(&original.value.value).await?;
        let md5_hash = md5_hash::compute(&translation);
        futures::future::try_join(
            db.insert_translation(feeds::Translation {
                md5_hash,
                value: translation.clone(),
            }),
            db.insert_field(feeds::Field {
                md5_hash,
                lang_code: feeds::LanguageCode::EN,
                ..field.value.clone()
            }),
        )
        .await?;
    }

    Ok(())
}
