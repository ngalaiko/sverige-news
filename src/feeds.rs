use crate::{id::Id, md5_hash::Md5Hash, persisted::Persisted, url::Url};

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Feed {
    pub href: Url,
    pub title: String,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Entry {
    pub feed_id: Id<Feed>,
    pub href: Url,
    pub published_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
pub enum FieldName {
    Title,
}

#[derive(Debug, thiserror::Error)]
#[error("invalid field name: {0}")]
pub struct InvalidFieldName(String);

impl<'a> sqlx::Encode<'a, sqlx::Sqlite> for FieldName {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Sqlite as sqlx::database::HasArguments<'a>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        <String as sqlx::Encode<'a, sqlx::sqlite::Sqlite>>::encode(self.to_string(), buf)
    }
}

impl sqlx::Decode<'_, sqlx::sqlite::Sqlite> for FieldName {
    fn decode(
        value: sqlx::sqlite::SqliteValueRef<'_>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let string = <String as sqlx::Decode<sqlx::sqlite::Sqlite>>::decode(value)?;
        let name = string
            .parse()
            .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        Ok(name)
    }
}

impl sqlx::Type<sqlx::Sqlite> for FieldName {
    fn type_info() -> <sqlx::Sqlite as sqlx::Database>::TypeInfo {
        <&str as sqlx::Type<sqlx::Sqlite>>::type_info()
    }
}

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

#[derive(Debug, Clone, PartialEq)]
pub enum LanguageCode {
    EN,
    SV,
}

#[derive(Debug, thiserror::Error)]
#[error("invalid language code: {0}")]
pub struct InvalidLanguageCode(String);

impl<'a> sqlx::Encode<'a, sqlx::Sqlite> for LanguageCode {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Sqlite as sqlx::database::HasArguments<'a>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        <String as sqlx::Encode<'a, sqlx::sqlite::Sqlite>>::encode(self.to_string(), buf)
    }
}

impl sqlx::Decode<'_, sqlx::sqlite::Sqlite> for LanguageCode {
    fn decode(
        value: sqlx::sqlite::SqliteValueRef<'_>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let string = <String as sqlx::Decode<sqlx::sqlite::Sqlite>>::decode(value)?;
        let code = string
            .parse()
            .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        Ok(code)
    }
}

impl sqlx::Type<sqlx::Sqlite> for LanguageCode {
    fn type_info() -> <sqlx::Sqlite as sqlx::Database>::TypeInfo {
        <&str as sqlx::Type<sqlx::Sqlite>>::type_info()
    }
}

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

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Field {
    pub entry_id: Id<Entry>,
    pub name: FieldName,
    pub lang_code: LanguageCode,
    pub md5_hash: Md5Hash,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Translation {
    pub md5_hash: Md5Hash,
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
        feed: &Persisted<Feed>,
    ) -> Result<Vec<(Entry, Vec<(FieldName, LanguageCode, String)>)>, CrawlError> {
        let response = self
            .http_client
            .get(feed.value.href.to_string())
            .send()
            .await
            .map_err(CrawlError::Reqwest)?;
        let bytes = response.bytes().await.map_err(CrawlError::Reqwest)?;
        let parser = feed_rs::parser::Builder::new()
            .base_uri(Some(&feed.value.href.to_string()))
            .build();
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
                        .and_then(|href| href.parse().ok())?,
                    published_at: entry.updated.or(entry.published)?,
                };
                Some((entry, fields))
            })
            .collect::<Vec<_>>();
        Ok(entries)
    }
}
