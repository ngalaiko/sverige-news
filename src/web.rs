use axum::extract::{Path, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::Uri;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::Router;
use rust_embed::RustEmbed;
use tower_http::compression::CompressionLayer;
use tower_http::trace::{self, TraceLayer};
use tracing::Level;

use crate::id::Id;
use crate::{clustering, db, feeds};

#[derive(Clone)]
struct AppState {
    db: db::Client,
}

#[tracing::instrument(skip_all)]
pub async fn serve(db: db::Client, address: &str) -> Result<(), Box<dyn std::error::Error>> {
    let state = AppState { db };
    let router = Router::new()
        .route("/", get(render_index))
        .route("/groups/:id", get(render_group))
        .fallback(serve_asset)
        .with_state(state)
        .layer(
            CompressionLayer::new()
                .br(true)
                .deflate(true)
                .gzip(true)
                .zstd(true),
        )
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(trace::DefaultMakeSpan::new().level(Level::INFO))
                .on_response(trace::DefaultOnResponse::new().level(Level::INFO)),
        );
    let listener = tokio::net::TcpListener::bind(address).await?;
    tracing::info!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, router).await?;
    Ok(())
}

struct Page {
    title: String,
    body: maud::Markup,
}

impl Page {
    pub fn new(title: &str, body: maud::Markup) -> Self {
        Self {
            title: title.to_string(),
            body,
        }
    }
}

impl axum::response::IntoResponse for Page {
    fn into_response(self) -> axum::response::Response {
        let page = maud::html! {
            (maud::DOCTYPE)
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                link rel="stylesheet" href="/index.css";
                title { (self.title) }
            }
            body {
                main {
                    (self.body)
                }
            }
            footer {
                nav {
                    ul {
                        li { a href="https://github.com/ngalaiko/sverige-news" { "GitHub" } }
                    }
                }
            }
        };
        Html(page.into_string()).into_response()
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
        Page::new(
            "Error",
            maud::html! {
                p { (self.0) }
            },
        )
        .into_response()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("not found")]
struct NotFound;

#[derive(serde::Deserialize)]
struct GroupParams {
    id: Id<clustering::ReportGroup>,
}

async fn render_index(State(state): State<AppState>) -> Result<Page, ErrorPage> {
    let report = state.db.find_latest_report().await?;
    let groups = state.db.list_groups_by_report_id(&report.id).await?;

    let mut gg = vec![];
    for group in groups {
        let mut entries = vec![];
        for embedding_id in &group.value.embedding_ids {
            let embedding = state.db.find_embedding_by_id(embedding_id).await?;
            let fields = state
                .db
                .list_fields_by_md5_hash(&embedding.value.md5_hash)
                .await?;
            for field in fields {
                let en_field = state
                    .db
                    .find_field_by_entry_id_lang_code(
                        &field.value.entry_id,
                        &feeds::LanguageCode::EN,
                    )
                    .await?
                    .expect("translation must exist");
                let en_translation = state
                    .db
                    .find_translation_by_md5_hash(&en_field.value.md5_hash)
                    .await?;
                let entry = state.db.find_entry_by_id(&field.value.entry_id).await?;
                let feed = feeds::LIST
                    .iter()
                    .find(|f| f.id == entry.value.feed_id)
                    .expect("feed must exist");
                entries.push((feed, entry, en_translation.clone()));
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
        header {
            h2 {
                time datetime=(report.created_at.to_rfc3339()) { (report.created_at.with_timezone(&SWEDEN_TZ).format("%A, %e %B")) }
            }
        }
        ul {
            @for (group_id, (feed, entry, translation), count) in gg {
                li {
                    h3 {
                        a href=(entry.value.href) { (translation.value.value) }
                    }
                    p {
                        date time=(entry.value.published_at.to_rfc3339()) { (entry.value.published_at.with_timezone(&SWEDEN_TZ).format("%H:%M")) }
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

    let title = format!("{} in Sweden", report.created_at.format("%A"));

    Ok(Page::new(&title, page))
}

const SWEDEN_TZ: chrono_tz::Tz = chrono_tz::Europe::Stockholm;

async fn render_group(
    State(state): State<AppState>,
    Path(params): Path<GroupParams>,
) -> Result<Page, ErrorPage> {
    let group = state.db.find_group_by_id(&params.id).await?;

    let mut entries = vec![];
    for embedding_id in &group.value.embedding_ids {
        let embedding = state.db.find_embedding_by_id(embedding_id).await?;
        let fields = state
            .db
            .list_fields_by_md5_hash(&embedding.value.md5_hash)
            .await?;

        for field in fields {
            let entry = state.db.find_entry_by_id(&field.value.entry_id).await?;
            let en_field = state
                .db
                .find_field_by_entry_id_lang_code(&field.value.entry_id, &feeds::LanguageCode::EN)
                .await?
                .expect("translation must exist");
            let translation = state
                .db
                .find_translation_by_md5_hash(&en_field.value.md5_hash)
                .await?;
            let feed = feeds::LIST
                .iter()
                .find(|f| f.id == entry.value.feed_id)
                .expect("feed must exist");
            entries.push((feed, entry, translation.clone()));
        }
    }

    entries.sort_by(|a, b| b.1.value.published_at.cmp(&a.1.value.published_at));

    let page = maud::html! {
        header {
            a href="/" { "← Back to main page" }
        }
        ul {
            @for (feed, entry, translation) in &entries {
                li {
                    h3 {
                        a href=(entry.value.href) { (translation.value.value) }
                    }
                    p {
                        time datetime=(entry.value.published_at.to_rfc3339()) { (entry.value.published_at.with_timezone(&SWEDEN_TZ).format("%H:%M")) }
                        " by " (feed.value.title)
                    }
                }
            }
        }
    };

    let title = entries
        .last()
        .map(|(_, _, translation)| translation.value.value.as_str())
        .expect("at least one entry is always present in a group");

    Ok(Page::new(&title, page))
}

#[derive(RustEmbed)]
#[folder = "assets"]
struct Assets;

async fn serve_asset(uri: Uri) -> Result<impl IntoResponse, ErrorPage> {
    let Some(asset) = Assets::get(uri.path().trim_start_matches('/')) else {
        return Err(ErrorPage::from(NotFound));
    };
    let content_type = asset.metadata.mimetype();
    let bytes = asset.data.to_vec();
    Ok(([(CONTENT_TYPE, content_type.to_string())], bytes))
}
