use axum::extract::{Path, State};
use axum::response::Html;
use axum::routing::get;
use axum::Router;

use crate::db;

#[derive(Clone)]
struct AppState {
    db: db::Client,
}

pub async fn serve(db: &db::Client, address: &str) -> Result<(), Box<dyn std::error::Error>> {
    let state = AppState { db: db.clone() };
    let router = Router::new()
        .route("/", get(render_index))
        .route("/groups/:id", get(render_group))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(address).await?;
    tracing::info!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, router).await?;
    Ok(())
}

struct Page(maud::Markup);

impl axum::response::IntoResponse for Page {
    fn into_response(self) -> axum::response::Response {
        Html(self.0.into_string()).into_response()
    }
}

impl From<maud::Markup> for Page {
    fn from(markup: maud::Markup) -> Self {
        Self(markup)
    }
}

struct ErrorPage(Box<dyn std::error::Error>);

impl From<sqlx::Error> for ErrorPage {
    fn from(value: sqlx::Error) -> Self {
        Self(Box::new(value))
    }
}

impl From<NotFound> for ErrorPage {
    fn from(value: NotFound) -> Self {
        Self(Box::new(value))
    }
}

impl axum::response::IntoResponse for ErrorPage {
    fn into_response(self) -> axum::response::Response {
        Page::from(maud::html! {
            h1 { "Error:" }
            p { (self.0) }
        })
        .into_response()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("not found")]
struct NotFound;

#[derive(serde::Deserialize)]
struct GroupParams {
    id: db::Id<db::ReportGroup>,
}

async fn render_group(
    State(state): State<AppState>,
    Path(params): Path<GroupParams>,
) -> Result<Page, ErrorPage> {
    let group = state.db.find_group_by_id(&params.id).await?;

    let mut entries = vec![];
    for embedding_id in group.value.embedding_ids.iter() {
        let embedding = state.db.find_embedding_by_id(embedding_id).await?;
        let translation = state
            .db
            .find_translation_by_md5_hash(&embedding.value.md5_hash)
            .await?;
        let fields = state
            .db
            .list_fields_by_md5_hash(&translation.value.md5_hash)
            .await?;

        for field in fields {
            let entry = state.db.find_entry_by_id(&field.value.entry_id).await?;
            let feed = state.db.find_feed_by_id(&entry.value.feed_id).await?;
            entries.push((feed, entry, translation.clone()));
        }
    }

    entries.sort_by(|a, b| b.1.value.published_at.cmp(&a.1.value.published_at));

    let page = maud::html! {
        ul {
            @for (feed, entry, translation) in entries {
                li {
                    h3 {
                        a href=(entry.value.href) { (translation.value.value) }
                    }
                    p {
                        time datetime=(entry.value.published_at.to_rfc3339()) { (entry.value.published_at.format("%H:%M")) }
                        " by " (feed.value.title)
                    }
                }
            }
        }
    };

    Ok(page.into())
}

async fn render_index(State(state): State<AppState>) -> Result<Page, ErrorPage> {
    let report = state.db.find_latest_report().await?;
    let groups = state.db.list_groups_by_report_id(&report.id).await?;

    let mut gg = vec![];
    for group in groups {
        let mut entries = vec![];
        for embedding_id in &group.value.embedding_ids {
            let embedding = state.db.find_embedding_by_id(embedding_id).await?;
            let translation = state
                .db
                .find_translation_by_md5_hash(&embedding.value.md5_hash)
                .await?;
            let fields = state
                .db
                .list_fields_by_md5_hash(&translation.value.md5_hash)
                .await?;

            for field in fields {
                let entry = state.db.find_entry_by_id(&field.value.entry_id).await?;
                let feed = state.db.find_feed_by_id(&entry.value.feed_id).await?;
                entries.push((feed, entry, translation.clone()));
            }
        }

        let earliest_entry = entries
            .iter()
            .min_by_key(|(_, entry, _)| entry.value.published_at)
            .expect("group is not empty");

        gg.push((group.id, earliest_entry.clone(), entries.len()));
    }

    gg.sort_by(|a, b| b.2.cmp(&a.2));

    let page = maud::html! {
        h2 {
            time datetime=(report.created_at.to_rfc3339()) { (report.created_at.format("%A, %e %B")) }
        }
        ul {
            @for (group_id, (feed, entry, translation), count) in gg {
                li {
                    h3 {
                        a href=(entry.value.href) { (translation.value.value) }
                    }
                    p {
                        date time=(entry.value.published_at.to_rfc3339()) { (entry.value.published_at.format("%H:%M")) }
                        " by "
                        (feed.value.title)
                        " and "
                        a href=(format!("/groups/{}", group_id)) {
                            @if count == 2 {
                                "1 other"
                            } @else {
                                (count - 1) " others"
                            }
                        }
                    }
                }
            }
        }
    };

    Ok(page.into())
}
