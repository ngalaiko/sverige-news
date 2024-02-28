mod db;
mod feeds;
mod openai;

use db::{Embedding, Persistent};

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

const EMBEDDING_SIZE: usize = 3072;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let db = db::Client::new(cli.database_file)
        .await
        .expect("failed to create db client");

    let feeds = db.list_feeds().await.expect("failed to list feeds");

    let crawler = feeds::Crawler::default();
    let entries = futures::future::try_join_all(feeds.iter().map(|feed| crawler.crawl(feed)))
        .await
        .unwrap();

    for entry in entries.iter().flatten() {
        match db.insert_entry(entry).await {
            Ok(_) => {}
            Err(error)
                if error
                    .as_database_error()
                    .map(sqlx::error::DatabaseError::kind)
                    == Some(sqlx::error::ErrorKind::UniqueViolation) => {}
            Err(error) => {
                panic!("{}", error);
            }
        }
    }

    let entries = db
        .list_entries_without_embeddings()
        .await
        .expect("failed to list entries without embeddings");

    let openai_client = openai::Client::new(&cli.openai_base_url, &cli.openai_token);
    for entry in entries {
        let embedding = openai_client
            .embeddings(&entry.1.title)
            .await
            .expect("failed to fetch embeddings");
        let embedding = Embedding {
            entry_id: entry.0,
            value: embedding,
        };
        db.insert_embeddig(&embedding)
            .await
            .expect("failed to insert embedding");
    }

    let today_entries = db
        .query_enteies_for_date(chrono::Utc::now().date_naive())
        .await
        .expect("failed to query entries for time range");

    let today_embeddings = futures::future::try_join_all(
        today_entries
            .iter()
            .map(|entry| db.query_embeddings_by_entry_id(entry.0)),
    )
    .await
    .expect("failed to query embeddings by entry id");

    let clusters = group_embeddings_best(&today_embeddings, 2, 0.1..=1.0).await;
    for cluster in clusters {
        println!("");
        for index in cluster {
            let entry = today_entries
                .get(index)
                .expect("got invalud clusring index");
            println!("- {} ({})", entry.1.title, entry.1.href);
        }
    }
}

// find the best tolerance using binary search
async fn group_embeddings_best(
    embeddings: &[Persistent<Embedding>],
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

        println!("attempt {} with range {:?}", attempt, range);

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
    embeddings: &[Persistent<Embedding>],
    min_points: usize,
    tolerance: f32,
) -> (Vec<Vec<usize>>, f32) {
    let (send, recv) = tokio::sync::oneshot::channel();

    let embeddings_len = embeddings.len();
    let vectors = embeddings
        .iter()
        .flat_map(|embedding| embedding.1.value.iter().cloned())
        .collect::<Vec<_>>();

    rayon::spawn(move || {
        let vectors: Array2<f32> =
            Array2::from_shape_vec((embeddings_len, EMBEDDING_SIZE), vectors)
                .expect("invalid shape");

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
                |mut acc, (target, index)| {
                    acc.entry(target).or_insert_with(Vec::new).push(index);
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
