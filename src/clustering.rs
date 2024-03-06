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
}

#[derive(Debug, Clone)]
pub struct ReportGroup {
    pub report_id: Id<Report>,
    pub embedding_ids: Vec<Id<Embedding>>,
}

static MIN_POINTS: usize = 3;
static RANGE: std::ops::RangeInclusive<f32> = 0.75..=1.1;

pub async fn group_embeddings(
    embeddings: &[Persisted<Embedding>],
) -> (Vec<Vec<Id<Embedding>>>, (usize, f32), f32) {
    let shape = (embeddings.len(), embeddings[0].value.size as usize);
    // try to reducing dimentions
    let vectors = embeddings
        .iter()
        .flat_map(|embedding| embedding.value.value.iter().copied())
        .collect::<Vec<_>>();
    let vectors: Array2<f32> = Array2::from_shape_vec(shape, vectors).expect("invalid shape");

    let mut range = RANGE.clone();
    let mut best_result = (Vec::new(), -1.0);
    let mut final_tolerance = 0.0;

    let (mut left_result, mut right_result) = futures::join!(
        dbscan(&vectors, MIN_POINTS, *range.start()),
        dbscan(&vectors, MIN_POINTS, *range.end()),
    );

    loop {
        if left_result.1 > best_result.1 {
            best_result = left_result.clone();
            final_tolerance = *range.start();
        } else if right_result.1 > best_result.1 {
            best_result = right_result.clone();
            final_tolerance = *range.end();
        } else {
            // result is not improving, stop
            break;
        }

        let center = (*range.start() + *range.end()) / 2.0;
        let center_result = dbscan(&vectors, MIN_POINTS, center).await;

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

    (clusters, (MIN_POINTS, final_tolerance), score)
}

#[tracing::instrument(skip(vectors), fields(dim = ?vectors.dim()))]
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

        tracing::info!(score = ?silhouette_score, clusters_len = clustered_indices.len());

        let _ = send.send((clustered_indices, silhouette_score));
    });

    recv.await.expect("panic in rayon::spawn")
}
