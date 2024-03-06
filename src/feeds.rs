pub mod abc;
pub mod aftonbladet;
pub mod dagen;
pub mod dn;
pub mod expressen;
pub mod nkpg;
pub mod scaraborgs;
pub mod svd;
pub mod svt;
pub mod tv4;

use crate::{id::Id, md5_hash::Md5Hash, persisted::Persisted, url::Url};

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Feed {
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
    Description,
    Content,
}

#[derive(Debug, thiserror::Error)]
#[error("invalid field title: {0}")]
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
            "description" => Ok(Self::Description),
            "content" => Ok(Self::Content),
            _ => Err(InvalidFieldName(s.to_owned())),
        }
    }
}

impl std::fmt::Display for FieldName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Title => write!(f, "title"),
            Self::Description => write!(f, "description"),
            Self::Content => write!(f, "content"),
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

pub static LIST: once_cell::sync::Lazy<Vec<Persisted<Feed>>> = once_cell::sync::Lazy::new(|| {
    vec![
        svt::FEED.clone(),
        dn::FEED.clone(),
        expressen::FEED.clone(),
        tv4::FEED.clone(),
        scaraborgs::FEED.clone(),
        nkpg::FEED.clone(),
        abc::FEED.clone(),
        dagen::FEED.clone(),
        svd::FEED.clone(),
        aftonbladet::FEED.clone(),
        // Persisted {
        //     id: Id::from(8),
        //     created_at,
        //     value: Feed {
        //         title: "Nyheter Idag".to_string(),
        //         href: "https://nyheteridag.se/feed".parse().expect("valid url"),
        //     },
        // },
    ]
});
