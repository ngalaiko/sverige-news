use crate::db;

#[derive(Debug)]
pub struct Feed {
    pub href: url::Url,
    pub title: String,
    pub fetched_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone)]
pub struct Entry {
    pub feed_id: db::Id<Feed>,
    pub title: String,
    pub href: url::Url,
    pub published_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, thiserror::Error)]
pub enum CrawlError {
    #[error(transparent)]
    Reqwest(reqwest::Error),
    #[error(transparent)]
    Parse(feed_rs::parser::ParseFeedError),
}

pub struct Crawler {
    http_client: reqwest::Client,
}

impl Default for Crawler {
    fn default() -> Self {
        Self {
            http_client: reqwest::ClientBuilder::new()
                .user_agent("Svergie News Crawler")
                .build()
                .expect("failed to build reqwest client"),
        }
    }
}

impl Crawler {
    pub async fn crawl(&self, feed: &db::Persistent<Feed>) -> Result<Vec<Entry>, CrawlError> {
        let parser = feed_rs::parser::Builder::new()
            .base_uri(Some(feed.1.href.as_str()))
            .build();
        let response = self
            .http_client
            .get(feed.1.href.clone())
            .send()
            .await
            .map_err(CrawlError::Reqwest)?;
        let bytes = response.bytes().await.map_err(CrawlError::Reqwest)?;
        let entries = parser
            .parse(bytes.to_vec().as_slice())
            .map(|feed| feed.entries)
            .map_err(CrawlError::Parse)?;
        let entries = entries
            .into_iter()
            .filter_map(|entry| {
                Some(Entry {
                    feed_id: feed.0,
                    href: entry
                        .links
                        .first()
                        .map(|link| link.href.as_str())
                        .and_then(|href| url::Url::parse(href).ok())?,
                    title: entry.title.map(|text| text.content)?,
                    published_at: entry.updated.or(entry.published)?,
                })
            })
            .collect::<Vec<_>>();
        Ok(entries)
    }
}
