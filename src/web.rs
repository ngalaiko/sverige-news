use axum::extract::{Path, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::Uri;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::Router;
use chrono::TimeZone;
use rust_embed::RustEmbed;
use tower_http::compression::CompressionLayer;
use tower_http::trace::{self, TraceLayer};
use tracing::Level;

use crate::clustering::ReportGroup;
use crate::id::Id;
use crate::{clustering, db, feeds};

#[derive(Clone)]
struct AppState {
    db: db::Client,
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn serve(db: db::Client, address: &str) -> Result<(), Box<dyn std::error::Error>> {
    let state = AppState { db };
    let router = Router::new()
        .route("/", get(render_index))
        .route("/:year/:month/:day", get(render_index_for_date))
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
                .make_span_with(trace::DefaultMakeSpan::new().level(Level::DEBUG))
                .on_response(trace::DefaultOnResponse::new().level(Level::DEBUG)),
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
                link rel="stylesheet" href="/css/pico.classless.yellow.min.css";
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
                        li { a href="/about.html" { "About" } }
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

#[derive(serde::Deserialize)]
struct DateParams {
    year: i32,
    month: u32,
    day: u32,
}

async fn render_index(State(state): State<AppState>) -> Result<Page, ErrorPage> {
    let date = chrono_tz::Europe::Stockholm
        .from_utc_datetime(&chrono::Utc::now().naive_utc())
        .date_naive();
    render_entries(state, date).await
}

async fn render_index_for_date(
    Path(params): Path<DateParams>,
    State(state): State<AppState>,
) -> Result<Page, ErrorPage> {
    let date =
        chrono::NaiveDate::from_ymd_opt(params.year, params.month, params.day).ok_or(NotFound)?;
    render_entries(state, date).await
}

async fn render_entries(state: AppState, date: chrono::NaiveDate) -> Result<Page, ErrorPage> {
    let entries = state
        .db
        .list_report_group_entries_by_date_lang_code(date, &feeds::LanguageCode::EN)
        .await?;

    let entries_feed_titles = entries
        .iter()
        .map(|entry| {
            let feed = feeds::LIST
                .iter()
                .find(|f| f.id == entry.feed_id)
                .expect("feed must exist");
            (entry, feed.value.title.clone())
        })
        .collect::<Vec<_>>();

    let entries_by_group_id = entries_feed_titles.into_iter().fold(
        std::collections::BTreeMap::<Id<ReportGroup>, Vec<(&GroupEntryView, String)>>::new(),
        |mut map, entry| {
            map.entry(entry.0.group_id).or_default().push(entry);
            map
        },
    );

    let mut scored_groups = entries_by_group_id
        .values()
        .map(|entries| {
            // score is the sum of minutes since the start of the day
            let score: i64 = entries
                .iter()
                .map(|(e, _)| e.published_at.naive_utc())
                .map(|dt| {
                    let start_of_day = dt
                        .date()
                        .and_hms_opt(0, 0, 0)
                        .expect("failed to get start of day");
                    dt - start_of_day
                })
                .map(|delta| delta.num_minutes())
                .sum();
            let center_entry = entries
                .iter()
                .find(|(e, _)| e.is_center)
                .expect("center is present");
            (center_entry, entries.len(), score)
        })
        .collect::<Vec<_>>();
    scored_groups.sort_by(|a, b| b.1.cmp(&a.1));

    let time = chrono_tz::Europe::Stockholm
        .from_local_date(&date)
        .single()
        .ok_or(NotFound)?
        .and_hms(0, 0, 0);
    let title = time.format("%A in Sweden").to_string();

    let page = maud::html! {
        header {
            h2 {
                time datetime=(time.to_rfc3339()) { (time.format("%A in Sweden")) }
            }
        }
        ol {
            @for ((entry, feed_title), size, _) in scored_groups {
                li {
                    a href=(entry.href) { (entry.title) }
                    p {
                        date time=(entry.published_at.to_rfc3339()) { (entry.published_at.with_timezone(&SWEDEN_TZ).format("%H:%M")) }
                        " by "
                        (feed_title)
                        " and "
                        a href=(format!("/groups/{}", entry.group_id)) {
                            @if size == 2 {
                                "1 other"
                            } @else {
                                (size - 1) " others"
                            }
                        }
                    }
                }
            }
        }
    };

    Ok(Page::new(&title, page))
}

const SWEDEN_TZ: chrono_tz::Tz = chrono_tz::Europe::Stockholm;

#[derive(Debug, sqlx::FromRow)]
pub struct GroupEntryView {
    pub group_id: Id<clustering::ReportGroup>,
    pub is_center: bool,
    pub title: String,
    pub href: String,
    pub published_at: chrono::DateTime<chrono::Utc>,
    pub feed_id: Id<feeds::Feed>,
}

async fn render_group(
    State(state): State<AppState>,
    Path(params): Path<GroupParams>,
) -> Result<Page, ErrorPage> {
    let groups = state
        .db
        .list_report_group_entries_by_id_lang_code(params.id, &feeds::LanguageCode::EN)
        .await?;

    let groups = groups
        .into_iter()
        .map(|group| {
            let feed = feeds::LIST
                .iter()
                .find(|f| f.id == group.feed_id)
                .expect("feed must exist");
            (group, feed.value.title.clone())
        })
        .collect::<Vec<_>>();

    let page = maud::html! {
        header {
            nav {
                ul {
                    li { small { a href= "/" { "Back to main page" } } }
                }
            }
        }
        ol {
            @for (group, feed_title) in &groups {
                li {
                    a href=(group.href) { (group.title) }
                    p {
                        time datetime=(group.published_at.to_rfc3339()) { (group.published_at.with_timezone(&SWEDEN_TZ).format("%H:%M")) }
                        " by "
                        (feed_title)
                    }
                }
            }
        }
    };

    let title = groups
        .last()
        .map(|(entry, _)| entry.title.as_str())
        .expect("at least one entry is always present in a group");

    Ok(Page::new(title, page))
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
