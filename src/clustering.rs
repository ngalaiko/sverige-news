use linfa::{metrics::SilhouetteScore, traits::Transformer, DatasetBase};
use linfa_clustering::Dbscan;
use linfa_nn::{
    distance::{self, L2Dist},
    BallTree, CommonNearestNeighbour, NearestNeighbour,
};
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
    pub center_embedding_id: Id<Embedding>,
}

static MIN_POINTS: usize = 3;
static RANGE: std::ops::RangeInclusive<f32> = 0.9..=1.1;
static SAMPLES: usize = 50;

/// given a set of embeddings, group them into clusters
/// using the DBSCAN algorithm
///
/// returns a list of pairs of clusters and their most central point,
/// parameters used to generate the clusters, and the silhouette score
#[tracing::instrument(skip(embeddings))]
pub async fn group_embeddings(
    embeddings: &[Persisted<Embedding>],
) -> (Vec<(Vec<Id<Embedding>>, usize)>, (usize, f32), f32) {
    let shape = (embeddings.len(), embeddings[0].value.size as usize);
    let vectors = embeddings
        .iter()
        .flat_map(|embedding| embedding.value.value.iter().copied())
        .collect::<Vec<_>>();
    let vectors: Array2<f32> = Array2::from_shape_vec(shape, vectors).expect("invalid shape");

    // first, run a grid search to find the best tolerance for the DBSCAN algorithm
    let step = (RANGE.end() - RANGE.start()) / SAMPLES as f32;
    let (mut best_clusters, mut best_tolerance, mut best_score) = (vec![], 0.0, 0.0);
    for i in 0..SAMPLES {
        let tolerance = RANGE.start() + step * i as f32;
        let (clusters, score) = dbscan(&vectors, MIN_POINTS, tolerance).await;
        tracing::info!(tolerance = tolerance, score = ?score, clusters_len = clusters.len(), "sample");
        if clusters.len() as f32 * score > best_clusters.len() as f32 * best_score {
            best_clusters = clusters;
            best_tolerance = tolerance;
            best_score = score;
        } else if clusters.len() < best_clusters.len() {
            // break once number of clusters starts to decrease
            break;
        }
    }

    tracing::info!(
        tolerance = best_tolerance,
        score = best_score,
        clusters_len = best_clusters.len(),
        "best"
    );

    let clusters = best_clusters
        .into_iter()
        .map(|cluster| {
            let ids = cluster
                .iter()
                .map(|i| embeddings[*i].id)
                .collect::<Vec<_>>();

            // for each cluster, find the nearest point to the centroid
            // we'll use it to represent the cluster
            let embeddings = cluster
                .iter()
                .map(|i| embeddings[*i].clone())
                .collect::<Vec<_>>();
            let shape = (embeddings.len(), embeddings[0].value.size as usize);
            let vectors = embeddings
                .iter()
                .flat_map(|embedding| embedding.value.value.iter().copied())
                .collect::<Vec<_>>();
            let vectors: Array2<f32> =
                Array2::from_shape_vec(shape, vectors).expect("invalid shape");
            let centroid = vectors
                .mean_axis(ndarray::Axis(0))
                .expect("failed to find mean");
            let ball_tree = BallTree::new()
                .from_batch(&vectors, L2Dist)
                .expect("failed to construct ball tree");
            let points = ball_tree
                .k_nearest(centroid.view(), 1)
                .expect("failed to get nearest");

            (ids, points[0].1)
        })
        .collect::<Vec<_>>();

    (clusters, (MIN_POINTS, best_tolerance), best_score)
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

        let _ = send.send((clustered_indices, silhouette_score));
    });

    recv.await.expect("panic in rayon::spawn")
}
