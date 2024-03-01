use crate::db;

use linfa::{metrics::SilhouetteScore, traits::Transformer, DatasetBase};
use linfa_clustering::Dbscan;
use ndarray::Array2;

pub async fn group_embeddings(
    embeddings: &[db::Persisted<db::Embedding>],
    min_points: usize,
    range: std::ops::RangeInclusive<f32>,
) -> (Vec<Vec<db::Id<db::Embedding>>>, f32) {
    let shape = (embeddings.len(), embeddings[0].value.size as usize);
    // try to reducing dimentions
    let vectors = embeddings
        .iter()
        .flat_map(|embedding| embedding.value.value.iter().copied())
        .collect::<Vec<_>>();
    let vectors: Array2<f32> = Array2::from_shape_vec(shape, vectors).expect("invalid shape");

    let mut range = range;
    let mut best_result = (Vec::new(), -1.0);

    let (mut left_result, mut right_result) = futures::join!(
        group_vectors(&vectors, min_points, *range.start()),
        group_vectors(&vectors, min_points, *range.end()),
    );

    loop {
        if left_result.1 > best_result.1 {
            best_result = left_result.clone();
        } else if right_result.1 > best_result.1 {
            best_result = right_result.clone();
        } else {
            // result is not improving, stop
            break;
        }

        let center = (*range.start() + *range.end()) / 2.0;
        let center_result = group_vectors(&vectors, min_points, center).await;

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
async fn group_vectors(
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

        tracing::info!(score = ?silhouette_score, clusters_len = clustered_indices.len());

        let _ = send.send((clustered_indices, silhouette_score));
    });

    recv.await.expect("panic in rayon::spawn")
}
