use linfa::{metrics::SilhouetteScore, traits::Transformer, DatasetBase};
use linfa_clustering::Dbscan;
use linfa_nn::{distance, CommonNearestNeighbour};
use ndarray::Array2;

use crate::{id::Id, md5_hash::Md5Hash, persisted::Persisted};

#[derive(Debug, Clone)]
pub struct Embedding {
    pub md5_hash: Md5Hash,
    pub value: Vec<f32>,
    pub size: u32,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Report {
    pub min_points: u32,
    pub tolerance: f32,
    pub score: f32,
    pub rows: u32,
    pub dimentions: u32,
}

#[derive(Debug, Clone)]
pub struct ReportGroup {
    pub report_id: Id<Report>,
    pub embedding_ids: Vec<Id<Embedding>>,
}

static MIN_POINTS: usize = 3;
static RANGE: std::ops::RangeInclusive<f32> = 0.7..=1.2;

fn cmp_results(a: &(Vec<Vec<usize>>, f32), b: &(Vec<Vec<usize>>, f32)) -> std::cmp::Ordering {
    // Prefer more clusters
    a.0.len().cmp(&b.0.len())
}

pub async fn group_embeddings(
    embeddings: &[Persisted<Embedding>],
) -> (Vec<Vec<Id<Embedding>>>, (usize, f32), f32) {
    let shape = (embeddings.len(), embeddings[0].value.size as usize);
    let vectors = embeddings
        .iter()
        .flat_map(|embedding| embedding.value.value.iter().copied())
        .collect::<Vec<_>>();
    let vectors: Array2<f32> = Array2::from_shape_vec(shape, vectors).expect("invalid shape");

    let (left_result, right_result) = futures::join!(
        dbscan(&vectors, MIN_POINTS, *RANGE.start()),
        dbscan(&vectors, MIN_POINTS, *RANGE.end()),
    );

    let mut best_result = if cmp_results(&left_result, &right_result) == std::cmp::Ordering::Greater
    {
        left_result.clone()
    } else {
        right_result.clone()
    };

    let mut range = RANGE.clone();
    loop {
        let mid = range.start() + (range.end() - range.start()) / 2.0;
        let mid_result = dbscan(&vectors, MIN_POINTS, mid).await;

        match cmp_results(&mid_result, &best_result) {
            std::cmp::Ordering::Greater => {
                best_result = mid_result;
                range = mid..=*range.end();
            }
            std::cmp::Ordering::Less => {
                range = *range.start()..=mid;
            }
            std::cmp::Ordering::Equal => {
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
                return (clusters, (MIN_POINTS, *range.end()), score);
            }
        }
    }
}

async fn dbscan(
    vectors: &Array2<f32>,
    min_points: usize,
    tolerance: f32,
) -> (Vec<Vec<usize>>, f32) {
    let (send, recv) = tokio::sync::oneshot::channel();

    let dim = vectors.dim();
    let dataset = DatasetBase::from(vectors.clone());

    rayon::spawn(move || {
        let cluster_memberships = Dbscan::params_with(
            min_points,
            distance::L2Dist,
            CommonNearestNeighbour::BallTree,
        )
        .tolerance(tolerance)
        .transform(dataset)
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

        tracing::info!(
            min_points = min_points,
            tolerance = tolerance,
            score = ?silhouette_score, clusters_len = clustered_indices.len(),
            "dbscan"
        );

        let _ = send.send((clustered_indices, silhouette_score));
    });

    recv.await.expect("panic in rayon::spawn")
}
