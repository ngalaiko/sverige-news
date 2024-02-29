use crate::feeds::{self, Entry};

pub struct Client {
    pool: sqlx::SqlitePool,
}

impl Client {
    pub async fn new<P: AsRef<std::path::Path>>(filename: P) -> Result<Self, sqlx::Error> {
        let opts = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(filename)
            .create_if_missing(true);

        let pool = sqlx::SqlitePool::connect_with(opts).await?;

        sqlx::migrate!("src/migrations").run(&pool).await?;
        Ok(Self { pool })
    }
}

impl Client {
    #[tracing::instrument(skip_all, fields(href = %entry.href))]
    pub async fn insert_entry(
        &self,
        entry: &feeds::Entry,
    ) -> Result<Persisted<feeds::Entry>, sqlx::Error> {
        match sqlx::query_as(
            "INSERT INTO entries (href, feed_id, published_at) VALUES ( ?, ?, ?) RETURNING *",
        )
        .bind(entry.href.to_string())
        .bind(u32::from(entry.feed_id))
        .bind(entry.published_at)
        .fetch_one(&self.pool)
        .await
        {
            Ok(entry) => Ok(entry),
            Err(error)
                if error
                    .as_database_error()
                    .map(sqlx::error::DatabaseError::kind)
                    == Some(sqlx::error::ErrorKind::UniqueViolation) =>
            {
                self.find_entry(entry).await
            }
            Err(error) => Err(error),
        }
    }

    async fn find_entry(&self, entry: &Entry) -> Result<Persisted<feeds::Entry>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM entries WHERE href = ?")
            .bind(entry.href.to_string())
            .fetch_one(&self.pool)
            .await
    }

    #[tracing::instrument(skip(self))]
    pub async fn list_entries_for_date(
        &self,
        date: chrono::NaiveDate,
    ) -> Result<Vec<Persisted<feeds::Entry>>, sqlx::Error> {
        let date = date
            .and_hms_opt(0, 0, 0)
            .expect("failed to create start of day");
        sqlx::query_as("SELECT * FROM entries WHERE published_at >= DATETIME($1, 'start of day') and published_at < DATETIME($1, 'start of day', '+1 day')")
            .bind(date)
            .fetch_all(&self.pool)
            .await
    }
}

impl Client {
    #[tracing::instrument(skip_all, fields(entry_id = %field.entry_id, name = %field.name, lang_code = %field.lang_code, md5_hash = ?field.md5_hash))]
    pub async fn insert_field(
        &self,
        field: &feeds::Field,
    ) -> Result<Persisted<feeds::Field>, sqlx::Error> {
        match sqlx::query_as("INSERT INTO fields (entry_id, name, lang_code, md5_hash) VALUES (?, ?, ?, ?) RETURNING *")
            .bind(u32::from(field.entry_id))
            .bind(field.name.to_string())
            .bind(field.lang_code.to_string())
            .bind(field.md5_hash.to_vec())
            .fetch_one(&self.pool)
            .await
        {
                    Ok(entry) => Ok(entry),
                    Err(error)
                        if error
                            .as_database_error()
                            .map(sqlx::error::DatabaseError::kind)
                            == Some(sqlx::error::ErrorKind::UniqueViolation) =>
                    {
                        self.find_field(
                            field
                        ).await
                    }
                    Err(error) => Err(error),
                }
    }

    async fn find_field(
        &self,
        field: &feeds::Field,
    ) -> Result<Persisted<feeds::Field>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM fields WHERE entry_id = ? AND name = ? AND lang_code = ?")
            .bind(field.entry_id)
            .bind(field.name.to_string())
            .bind(field.lang_code.to_string())
            .fetch_one(&self.pool)
            .await
    }

    #[tracing::instrument(skip(self))]
    pub async fn list_sv_fields_without_en_translation(
        &self,
    ) -> Result<Vec<Persisted<feeds::Field>>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM fields  WHERE lang_code = 'sv' AND NOT EXISTS (SELECT 1 FROM fields AS en WHERE en.entry_id = fields.entry_id AND en.name = fields.name AND en.lang_code = 'en')")
            .fetch_all(&self.pool)
            .await
    }

    #[tracing::instrument(skip(self))]
    pub async fn find_en_title_by_entry_id(
        &self,
        entry_id: Id<feeds::Entry>,
    ) -> Result<Persisted<feeds::Field>, sqlx::Error> {
        sqlx::query_as(
            "SELECT * FROM fields WHERE entry_id = ? AND name = 'title' AND lang_code = 'en'",
        )
        .bind(u32::from(entry_id))
        .fetch_one(&self.pool)
        .await
    }
}

impl Client {
    #[tracing::instrument(skip_all, fields(md5_hash = ?embedding.md5_hash, size = %embedding.size))]
    pub async fn insert_embeddig(
        &self,
        embedding: &Embedding,
    ) -> Result<Persisted<Embedding>, sqlx::Error> {
        match sqlx::query_as(
            "INSERT INTO embeddings (md5_hash, value, size) VALUES ( ?, ?, ? ) RETURNING *",
        )
        .bind(embedding.md5_hash.to_vec())
        .bind(serde_json::to_string(&embedding.value).expect("failed to serialize embedding"))
        .bind(embedding.size)
        .fetch_one(&self.pool)
        .await
        {
            Ok(embedding) => Ok(embedding),
            Err(error)
                if error
                    .as_database_error()
                    .map(sqlx::error::DatabaseError::kind)
                    == Some(sqlx::error::ErrorKind::UniqueViolation) =>
            {
                self.find_embedding(embedding).await
            }
            Err(error) => Err(error),
        }
    }

    async fn find_embedding(
        &self,
        embedding: &Embedding,
    ) -> Result<Persisted<Embedding>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM embeddings WHERE md5_hash = ?")
            .bind(embedding.md5_hash.to_vec())
            .fetch_one(&self.pool)
            .await
    }

    #[tracing::instrument(skip(self))]
    pub async fn find_embedding_by_md5_hash(
        &self,
        md5_hash: &md5::Digest,
    ) -> Result<Persisted<Embedding>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM embeddings WHERE md5_hash = ?")
            .bind(md5_hash.to_vec())
            .fetch_one(&self.pool)
            .await
    }
}

impl Client {
    #[tracing::instrument(skip_all, fields(md5_hash = ?transaslation.md5_hash))]
    pub async fn insert_translation(
        &self,
        transaslation: &feeds::Translation,
    ) -> Result<Persisted<feeds::Translation>, sqlx::Error> {
        match sqlx::query_as("INSERT INTO translations (md5_hash, value) VALUES (?, ?) RETURNING *")
            .bind(transaslation.md5_hash.to_vec())
            .bind(transaslation.value.to_string())
            .fetch_one(&self.pool)
            .await
        {
            Ok(translation) => Ok(translation),
            Err(error)
                if error
                    .as_database_error()
                    .map(sqlx::error::DatabaseError::kind)
                    == Some(sqlx::error::ErrorKind::UniqueViolation) =>
            {
                self.find_translation(transaslation).await
            }
            Err(error) => Err(error),
        }
    }

    async fn find_translation(
        &self,
        translation: &feeds::Translation,
    ) -> Result<Persisted<feeds::Translation>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM translations WHERE md5_hash = ?")
            .bind(translation.md5_hash.to_vec())
            .fetch_one(&self.pool)
            .await
    }

    #[tracing::instrument(skip(self), fields(md5_hash = ?md5_hash))]
    pub async fn find_translation_by_md5_hash(
        &self,
        md5_hash: &md5::Digest,
    ) -> Result<Persisted<feeds::Translation>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM translations WHERE md5_hash = ?")
            .bind(md5_hash.to_vec())
            .fetch_one(&self.pool)
            .await
    }

    #[tracing::instrument(skip(self))]
    pub async fn list_en_translations_without_embeddings(
        &self,
    ) -> Result<Vec<Persisted<feeds::Translation>>, sqlx::Error> {
        sqlx::query_as("SELECT translations.* FROM translations JOIN fields on fields.md5_hash = translations.md5_hash AND fields.lang_code = 'en' WHERE NOT EXISTS (SELECT 1 FROM embeddings WHERE embeddings.md5_hash = translations.md5_hash)")
            .fetch_all(&self.pool)
            .await
    }
}

impl Client {
    #[tracing::instrument(skip(self))]
    pub async fn list_feeds(&self) -> Result<Vec<Persisted<feeds::Feed>>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM feeds")
            .fetch_all(&self.pool)
            .await
    }
}

pub struct Id<T>(u32, std::marker::PhantomData<T>);

impl<T> Clone for Id<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> std::fmt::Display for Id<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<T> std::fmt::Debug for Id<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<T> Copy for Id<T> {}

impl<T> From<u32> for Id<T> {
    fn from(value: u32) -> Self {
        Self(value, std::marker::PhantomData)
    }
}

impl<T> From<Id<T>> for u32 {
    fn from(value: Id<T>) -> Self {
        value.0
    }
}

impl<T> sqlx::Encode<'_, sqlx::Sqlite> for Id<T> {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Sqlite as sqlx::database::HasArguments<'_>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        self.0.encode_by_ref(buf)
    }
}

impl<T> sqlx::Type<sqlx::Sqlite> for Id<T> {
    fn type_info() -> <sqlx::Sqlite as sqlx::Database>::TypeInfo {
        <u32 as sqlx::Type<sqlx::Sqlite>>::type_info()
    }
}

#[derive(Debug, Clone)]
pub struct Embedding {
    pub md5_hash: md5::Digest,
    pub value: Vec<f32>,
    pub size: u32,
}

#[derive(Debug)]
pub struct Persisted<T> {
    pub id: Id<T>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub value: T,
}

impl sqlx::FromRow<'_, sqlx::sqlite::SqliteRow> for Persisted<feeds::Feed> {
    fn from_row(row: &'_ sqlx::sqlite::SqliteRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;

        let id: u32 = row.try_get("id")?;
        let created_at = row.try_get("created_at")?;

        let href: &str = row.try_get("href")?;
        let href = url::Url::parse(href).map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        let title = row.try_get("title")?;

        Ok(Persisted {
            id: id.into(),
            created_at,
            value: feeds::Feed { href, title },
        })
    }
}

impl sqlx::FromRow<'_, sqlx::sqlite::SqliteRow> for Persisted<feeds::Entry> {
    fn from_row(row: &'_ sqlx::sqlite::SqliteRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;

        let id: u32 = row.try_get("id")?;
        let created_at = row.try_get("created_at")?;

        let href: &str = row.try_get("href")?;
        let href = url::Url::parse(href).map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        let feed_id: u32 = row.try_get("feed_id")?;
        let published_at = row.try_get("published_at")?;

        Ok(Persisted {
            id: id.into(),
            created_at,
            value: feeds::Entry {
                href,
                feed_id: feed_id.into(),
                published_at,
            },
        })
    }
}

impl sqlx::FromRow<'_, sqlx::sqlite::SqliteRow> for Persisted<feeds::Field> {
    fn from_row(row: &'_ sqlx::sqlite::SqliteRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;

        let id: u32 = row.try_get("id")?;
        let created_at = row.try_get("created_at")?;

        let entry_id: u32 = row.try_get("entry_id")?;
        let md5_hash: Vec<u8> = row.try_get("md5_hash")?;
        let md5_hash: [u8; 16] = md5_hash
            .try_into()
            .map_err(|_| sqlx::Error::Decode(Box::new(InvalidMd5HashLength)))?;
        let name: &str = row.try_get("name")?;
        let name: feeds::FieldName = name
            .parse()
            .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        let lang_code: &str = row.try_get("lang_code")?;
        let lang_code = lang_code
            .parse()
            .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;

        Ok(Persisted {
            id: id.into(),
            created_at,
            value: feeds::Field {
                entry_id: entry_id.into(),
                md5_hash: md5::Digest(md5_hash),
                name,
                lang_code,
            },
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid md5 hash length")]
struct InvalidMd5HashLength;

impl sqlx::FromRow<'_, sqlx::sqlite::SqliteRow> for Persisted<Embedding> {
    fn from_row(row: &'_ sqlx::sqlite::SqliteRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;

        let id: u32 = row.try_get("id")?;
        let created_at = row.try_get("created_at")?;

        let value: String = row.try_get("value")?;
        let value =
            serde_json::from_str(&value).map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        let size = row.try_get("size")?;
        let md5_hash: Vec<u8> = row.try_get("md5_hash")?;
        let md5_hash: [u8; 16] = md5_hash
            .try_into()
            .map_err(|_| sqlx::Error::Decode(Box::new(InvalidMd5HashLength)))?;

        Ok(Persisted {
            id: id.into(),
            created_at,
            value: Embedding {
                md5_hash: md5::Digest(md5_hash),
                value,
                size,
            },
        })
    }
}

impl sqlx::FromRow<'_, sqlx::sqlite::SqliteRow> for Persisted<feeds::Translation> {
    fn from_row(row: &'_ sqlx::sqlite::SqliteRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;

        let id: u32 = row.try_get("id")?;
        let created_at = row.try_get("created_at")?;

        let value: String = row.try_get("value")?;
        let md5_hash: Vec<u8> = row.try_get("md5_hash")?;
        let md5_hash: [u8; 16] = md5_hash
            .try_into()
            .map_err(|_| sqlx::Error::Decode(Box::new(InvalidMd5HashLength)))?;

        Ok(Persisted {
            id: id.into(),
            created_at,
            value: feeds::Translation {
                md5_hash: md5::Digest(md5_hash),
                value,
            },
        })
    }
}
