use std::str::FromStr;

#[derive(Clone)]
pub struct Url(url::Url);

impl FromStr for Url {
    type Err = url::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let url = url::Url::parse(s)?;
        Ok(Url(url))
    }
}

impl std::fmt::Debug for Url {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Display for Url {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<url::Url> for Url {
    fn from(url: url::Url) -> Self {
        Url(url)
    }
}

impl From<Url> for url::Url {
    fn from(url: Url) -> url::Url {
        url.0
    }
}

impl sqlx::Type<sqlx::Sqlite> for Url {
    fn type_info() -> <sqlx::Sqlite as sqlx::Database>::TypeInfo {
        <&str as sqlx::Type<sqlx::Sqlite>>::type_info()
    }
}

impl<'a> sqlx::Encode<'a, sqlx::sqlite::Sqlite> for Url {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::sqlite::Sqlite as sqlx::database::HasArguments<'a>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        <String as sqlx::Encode<'a, sqlx::sqlite::Sqlite>>::encode(self.0.to_string(), buf)
    }
}

impl sqlx::Decode<'_, sqlx::sqlite::Sqlite> for Url {
    fn decode(
        value: sqlx::sqlite::SqliteValueRef<'_>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let string = <String as sqlx::Decode<sqlx::sqlite::Sqlite>>::decode(value)?;
        let url = url::Url::parse(&string)?;
        Ok(Url(url))
    }
}
