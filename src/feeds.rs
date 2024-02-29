use crate::db;

#[derive(Debug)]
pub struct Feed {
    pub href: url::Url,
    pub title: String,
}

#[derive(Debug, Clone)]
pub struct Entry {
    pub feed_id: db::Id<Feed>,
    pub href: url::Url,
    pub published_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug)]
pub enum FieldName {
    Title,
}

#[derive(Debug, thiserror::Error)]
#[error("invalid field name: {0}")]
pub struct InvalidFieldName(String);

impl std::str::FromStr for FieldName {
    type Err = InvalidFieldName;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "title" => Ok(Self::Title),
            _ => Err(InvalidFieldName(s.to_owned())),
        }
    }
}

impl std::fmt::Display for FieldName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Title => write!(f, "title"),
        }
    }
}

#[derive(Debug)]
pub enum LanguageCode {
    EN,
    SV,
}

#[derive(Debug, thiserror::Error)]
#[error("invalid language code: {0}")]
pub struct InvalidLanguageCode(String);

impl std::str::FromStr for LanguageCode {
    type Err = InvalidLanguageCode;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "en" => Ok(Self::EN),
            "sv" => Ok(Self::SV),
            _ => Err(InvalidLanguageCode(s.to_owned())),
        }
    }
}

impl std::fmt::Display for LanguageCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EN => write!(f, "en"),
            Self::SV => write!(f, "sv"),
        }
    }
}

#[derive(Debug)]
pub struct Field {
    pub entry_id: db::Id<Entry>,
    pub name: FieldName,
    pub lang_code: LanguageCode,
    pub md5_hash: md5::Digest,
}

#[derive(Debug)]
pub struct Translation {
    pub md5_hash: md5::Digest,
    pub value: String,
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
    #[tracing::instrument(skip_all, fields(href = %feed.value.href))]
    pub async fn crawl(
        &self,
        feed: &db::Persisted<Feed>,
    ) -> Result<Vec<(Entry, Vec<(FieldName, LanguageCode, String)>)>, CrawlError> {
        let parser = feed_rs::parser::Builder::new()
            .base_uri(Some(&feed.value.href))
            .build();
        let response = self
            .http_client
            .get(feed.value.href.clone())
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
                let mut fields = vec![];
                if let Some(title) = entry.title {
                    fields.push((FieldName::Title, LanguageCode::SV, title.content));
                }
                let entry = Entry {
                    feed_id: feed.id,
                    href: entry
                        .links
                        .first()
                        .map(|link| link.href.as_str())
                        .and_then(|href| url::Url::parse(href).ok())?,
                    published_at: entry.updated.or(entry.published)?,
                };
                Some((entry, fields))
            })
            .collect::<Vec<_>>();
        Ok(entries)
    }
}
