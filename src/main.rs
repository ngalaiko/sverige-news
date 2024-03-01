mod db;
mod feeds;
mod openai;

use clap::Parser;
use linfa::{metrics::SilhouetteScore, traits::Transformer, DatasetBase};
use linfa_clustering::Dbscan;
use ndarray::Array2;

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    openai_token: String,
    #[arg(long, default_value = "https://api.openai.com/")]
    openai_base_url: url::Url,
    #[arg(long, default_value = "database.sqlite3")]
    database_file: std::path::PathBuf,
}

#[derive(Debug, thiserror::Error)]
enum FetchFeedsError {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Crawler(#[from] feeds::CrawlError),
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() {
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
    let translator = openai::Translator::new(&openai_client);
    let crawler = feeds::Crawler::default();

    fetch_feeds(&db, &crawler)
        .await
        .expect("failed to fetch feeds");

    let untranslated_sv_fields = db
        .list_sv_fields_without_en_translation()
        .await
        .expect("failed to list entries without embeddings");
    for sv_field in untranslated_sv_fields {
        let sv_translation = db
            .find_translation_by_md5_hash(&sv_field.value.md5_hash)
            .await
            .expect("failed to query translation by md5 hash");

        let translation = translator
            .translate_sv_to_en(&sv_translation.value.value)
            .await
            .expect("failed to translate");

        let en_translation = feeds::Translation {
            md5_hash: md5::compute(&translation),
            value: translation,
        };
        db.insert_translation(&en_translation)
            .await
            .expect("failed to insert en translation");

        let en_field = feeds::Field {
            lang_code: feeds::LanguageCode::EN,
            md5_hash: en_translation.md5_hash,
            ..sv_field.value
        };
        db.insert_field(&en_field)
            .await
            .expect("failed to insert en field");
    }

    let translations_without_embeddings = db
        .list_translations_without_embeddings(feeds::LanguageCode::EN)
        .await
        .expect("failed to list translations without embeddings");
    for translation in translations_without_embeddings {
        let embedding = openai_client
            .embeddings(&translation.value.value)
            .await
            .expect("failed to get embeddings");
        let embedding = db::Embedding {
            md5_hash: translation.value.md5_hash,
            size: embedding
                .len()
                .try_into()
                .expect("failed to convert usize into u32"),
            value: embedding,
        };
        db.insert_embeddig(&embedding)
            .await
            .expect("failed to insert embedding");
    }

    let today_en_title_embeddings = db
        .list_embeddings_by_field_name_lang_code_date(
            feeds::FieldName::Title,
            feeds::LanguageCode::EN,
            chrono::Utc::now().date_naive(),
        )
        .await
        .expect("failed to query embeddings by md5 hash");

    let (clusters, score) = group_embeddings_best(&today_en_title_embeddings, 2, 0.75..=1.0).await;
    let report = db::Report {
        groups: clusters,
        score,
    };

    db.insert_report(&report)
        .await
        .expect("failed to insert report");

    let latest_report = db
        .find_latest_report()
        .await
        .expect("failed to find latest report");

    let mut lines = vec![];
    for group in latest_report.value.groups {
        lines.push(String::new());
        for embedding_id in group {
            let embedding = db
                .find_embedding_by_id(embedding_id)
                .await
                .expect("failed to find entry by id");

            let translation = db
                .find_translation_by_md5_hash(&embedding.value.md5_hash)
                .await
                .expect("failed to find translation by md5 hash");

            let fields = db
                .list_fields_md5_hash(&embedding.value.md5_hash)
                .await
                .expect("failed to find title by entry id");

            for field in fields {
                let entry = db
                    .find_entry_by_id(field.value.entry_id)
                    .await
                    .expect("failed to find entry by id");

                lines.push(format!(
                    "- {} ({})",
                    translation.value.value, entry.value.href
                ));
            }
        }
    }

    std::fs::write("./report.txt", lines.join("\n")).expect("failed to save report");
}

async fn fetch_feeds(db: &db::Client, crawler: &feeds::Crawler) -> Result<(), FetchFeedsError> {
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

// find the best tolerance using binary search
async fn group_embeddings_best(
    embeddings: &[db::Persisted<db::Embedding>],
    min_points: usize,
    range: std::ops::RangeInclusive<f32>,
) -> (Vec<Vec<db::Id<db::Embedding>>>, f32) {
    let shape = (embeddings.len(), embeddings[0].value.size as usize);
    let vectors = embeddings
        .iter()
        .flat_map(|embedding| embedding.value.value.iter().copied())
        .collect::<Vec<_>>();
    let vectors: Array2<f32> = Array2::from_shape_vec(shape, vectors).expect("invalid shape");

    let mut range = range;
    let mut best_result = (Vec::new(), -1.0);

    let (mut left_result, mut right_result) = futures::join!(
        group_embeddings(&vectors, min_points, *range.start()),
        group_embeddings(&vectors, min_points, *range.end()),
    );

    loop {
        println!("range {range:?}");

        println!("tolerance: {}, score: {:}", *range.start(), left_result.1);
        println!("tolerance: {}, score: {:}", *range.end(), right_result.1);

        if left_result.1 > best_result.1 {
            best_result = left_result.clone();
        } else if right_result.1 > best_result.1 {
            best_result = right_result.clone();
        } else {
            // result is not improving, stop
            break;
        }

        let center = (*range.start() + *range.end()) / 2.0;
        let center_result = group_embeddings(&vectors, min_points, center).await;

        if left_result.1 > right_result.1 {
            range = *range.start()..=center;
            right_result = center_result;
        } else if right_result.1 > left_result.1 {
            range = center..=*range.end();
            left_result = center_result;
        } else {
            // ranges are equal, break
            break;
        };
    }

    let (clusters, score) = best_result;
    let clusters = clusters
        .into_iter()
        .map(|cluster| {
            cluster
                .into_iter()
                .map(|i| embeddings[i].id)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    (clusters, score)
}

#[tracing::instrument(skip(vectors), fields(dim = ?vectors.dim()))]
async fn group_embeddings(
    vectors: &Array2<f32>,
    min_points: usize,
    tolerance: f32,
) -> (Vec<Vec<usize>>, f32) {
    let (send, recv) = tokio::sync::oneshot::channel();

    let dim = vectors.dim();
    let dataset = DatasetBase::from(vectors.clone());

    rayon::spawn(move || {
        let cluster_memberships = Dbscan::params(min_points)
            .tolerance(tolerance)
            .transform(dataset.clone())
            .expect("failed to cluster");

        let silhouette_score = cluster_memberships.silhouette_score().unwrap();

        let indices = (0..dim.0).collect::<Vec<_>>();
        let clustered_indices = cluster_memberships
            .targets()
            .into_iter()
            .zip(indices.into_iter())
            .filter_map(|(target, index)| target.map(|target| (target, index)))
            .fold(
                std::collections::HashMap::new(),
                |mut acc: std::collections::HashMap<usize, Vec<usize>>, (target, index)| {
                    acc.entry(target).or_default().push(index);
                    acc
                },
            )
            .values()
            .cloned()
            .collect::<Vec<_>>();

        let _ = send.send((clustered_indices, silhouette_score));
    });

    recv.await.expect("panic in rayon::spawn")
}
