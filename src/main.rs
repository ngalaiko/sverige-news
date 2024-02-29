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

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() {
    let subscriber = tracing_subscriber::fmt::fmt()
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let cli = Cli::parse();

    let db = db::Client::new(cli.database_file)
        .await
        .expect("failed to create db client");
    let openai_client = openai::Client::new(&cli.openai_base_url, &cli.openai_token);
    let crawler = feeds::Crawler::default();

    let feeds = db.list_feeds().await.expect("failed to list feeds");

    let entries = futures::future::try_join_all(feeds.iter().map(|feed| crawler.crawl(feed)))
        .await
        .unwrap();

    for (entry, fields) in entries.into_iter().flatten() {
        let entry = db
            .insert_entry(&entry)
            .await
            .expect("failed to insert entry");
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
            db.insert_field(&sv_field)
                .await
                .expect("failed to insert field");
            db.insert_translation(&sv_translation)
                .await
                .expect("failed to insert translation");
        }
    }

    let untranslated_sv_fields = db
        .list_sv_fields_without_en_translation()
        .await
        .expect("failed to list entries without embeddings");
    for sv_field in untranslated_sv_fields {
        let sv_translation = db
            .find_translation_by_md5_hash(&sv_field.value.md5_hash)
            .await
            .expect("failed to query translation by md5 hash");

        let translation = translate_sv_to_en(&openai_client, &sv_translation.value.value)
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
        .list_en_translations_without_embeddings()
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

    let today_entries = db
        .list_entries_for_date(chrono::Utc::now().date_naive())
        .await
        .expect("failed to query entries for time range");

    let today_entries_en_title_fields = futures::future::try_join_all(
        today_entries
            .iter()
            .map(|entry| db.find_en_title_by_entry_id(entry.id)),
    )
    .await
    .expect("failed to query embeddings by entry id");

    let today_en_title_embeddings = futures::future::try_join_all(
        today_entries_en_title_fields
            .iter()
            .map(|field| db.find_embedding_by_md5_hash(&field.value.md5_hash)),
    )
    .await
    .expect("failed to query embeddings by md5 hash");

    let clusters = group_embeddings_best(&today_en_title_embeddings, 2, 0.75..=1.0).await;
    let mut report = vec![];
    for cluster in clusters {
        report.push("---".to_string());
        for index in cluster {
            let title = today_entries_en_title_fields
                .get(index)
                .expect("got invalid clusring index");
            let entry = today_entries
                .get(index)
                .expect("got invalud clusring index");
            let en_translation = db
                .find_translation_by_md5_hash(&title.value.md5_hash)
                .await
                .expect("failed to query en title by entry id");

            report.push(format!("- {} ({})", en_translation.value.value, entry.value.href));
        }
    }

    println!("{}", report.join("\n"));
}

#[tracing::instrument(skip_all)]
async fn translate_sv_to_en(
    client: &openai::Client<'_>,
    value: &str,
) -> Result<String, openai::Error> {
    let task = "You are a highly skilled and concise professional translator. When you receive a sentence in Swedish, your task is to translate it into English. VERY IMPORTANT: Do not output any notes, explanations, alternatives or comments after or before the translation.";
    client.comptetions(task, value).await
}

// find the best tolerance using binary search
async fn group_embeddings_best(
    embeddings: &[db::Persisted<db::Embedding>],
    min_points: usize,
    range: std::ops::RangeInclusive<f32>,
) -> Vec<Vec<usize>> {
    let mut range = range;
    let mut best_result = (Vec::new(), -1.0);
    let mut attempt = 1;
    loop {
        if attempt == 10 {
            break;
        }

        println!("attempt {attempt} with range {range:?}",);

        let (left_result, right_result) = futures::join!(
            group_embeddings(embeddings, min_points, *range.start()),
            group_embeddings(embeddings, min_points, *range.end()),
        );

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
        if left_result.1 > right_result.1 {
            range = *range.start()..=center;
        } else if right_result.1 > left_result.1 {
            range = center..=*range.end();
        } else {
            break;
        };

        attempt += 1;
    }

    best_result.0
}

async fn group_embeddings(
    embeddings: &[db::Persisted<db::Embedding>],
    min_points: usize,
    tolerance: f32,
) -> (Vec<Vec<usize>>, f32) {
    let (send, recv) = tokio::sync::oneshot::channel();

    let embeddings_len = embeddings.len();
    let vectors = embeddings
        .iter()
        .flat_map(|embedding| embedding.value.value.iter().copied())
        .collect::<Vec<_>>();

    let size: usize = embeddings[0].value.size.try_into().expect("invalid size");

    rayon::spawn(move || {
        let vectors: Array2<f32> =
            Array2::from_shape_vec((embeddings_len, size), vectors).expect("invalid shape");

        let dataset: DatasetBase<_, _> = vectors.into();

        let cluster_memberships = Dbscan::params(min_points)
            .tolerance(tolerance)
            .transform(dataset.clone())
            .expect("failed to cluster");

        let silhouette_score = cluster_memberships.silhouette_score().unwrap();

        let indices = (0..embeddings_len).collect::<Vec<_>>();
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
