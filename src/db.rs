use crate::feeds;

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
    pub async fn query_enteies_for_date(
        &self,
        date: chrono::NaiveDate,
    ) -> Result<Vec<Persistent<feeds::Entry>>, sqlx::Error> {
        let date = date
            .and_hms_opt(0, 0, 0)
            .expect("failed to create start of day");
        sqlx::query_as("SELECT * FROM entries WHERE published_at >= DATETIME($1, 'start of day') and published_at < DATETIME($1, 'start of day', '+1 day')")
            .bind(date)
            .fetch_all(&self.pool)
            .await
    }

    pub async fn insert_entry(
        &self,
        entry: &feeds::Entry,
    ) -> Result<Persistent<feeds::Entry>, sqlx::Error> {
        sqlx::query("INSERT INTO entries (feed_id, href, title, published_at) VALUES ( ?, ?, ?, ? ) RETURNING id")
            .bind(u32::from(entry.feed_id))
            .bind(entry.href.to_string())
            .bind(&entry.title)
            .bind(entry.published_at)
            .fetch_one(&self.pool).await.and_then(|response| {
                use sqlx::Row;
                let id: u32 = response.try_get("id")?;
                Ok(Persistent(id.into(), entry.clone()))
            })
    }

    pub async fn list_entries_without_embeddings(
        &self,
    ) -> Result<Vec<Persistent<feeds::Entry>>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM entries WHERE id NOT IN (SELECT entry_id FROM embeddings)")
            .fetch_all(&self.pool)
            .await
    }
}

impl Client {
    pub async fn insert_embeddig(
        &self,
        embedding: &Embedding,
    ) -> Result<Persistent<Embedding>, sqlx::Error> {
        sqlx::query("INSERT INTO embeddings (entry_id, value) VALUES ( ?, ? ) RETURNING id")
            .bind(u32::from(embedding.entry_id))
            .bind(serde_json::to_string(&embedding.value).expect("failed to serialize embedding"))
            .fetch_one(&self.pool)
            .await
            .and_then(|response| {
                use sqlx::Row;
                let id: u32 = response.try_get("id")?;
                Ok(Persistent(id.into(), embedding.clone()))
            })
    }

    pub async fn query_embeddings_by_entry_id(
        &self,
        entr_id: Id<feeds::Entry>,
    ) -> Result<Persistent<Embedding>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM embeddings WHERE entry_id = ?")
            .bind(entr_id)
            .fetch_one(&self.pool)
            .await
    }
}

impl Client {
    pub async fn list_feeds(&self) -> Result<Vec<Persistent<feeds::Feed>>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM feeds")
            .fetch_all(&self.pool)
            .await
    }
}

pub struct Id<T>(u32, std::marker::PhantomData<T>);

impl<T> PartialEq for Id<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl<T> Eq for Id<T> {}

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
    pub entry_id: Id<feeds::Entry>,
    pub value: Vec<f32>,
}

impl sqlx::FromRow<'_, sqlx::sqlite::SqliteRow> for Persistent<Embedding> {
    fn from_row(row: &'_ sqlx::sqlite::SqliteRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;
        let id: u32 = row.try_get("id")?;
        let entry_id: u32 = row.try_get("entry_id")?;
        let value: String = row.try_get("value")?;
        let value =
            serde_json::from_str(&value).map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        Ok(Persistent(
            id.into(),
            Embedding {
                entry_id: entry_id.into(),
                value,
            },
        ))
    }
}

#[derive(Debug)]
pub struct Persistent<T>(pub Id<T>, pub T);

impl sqlx::FromRow<'_, sqlx::sqlite::SqliteRow> for Persistent<feeds::Feed> {
    fn from_row(row: &'_ sqlx::sqlite::SqliteRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;

        let href: &str = row.try_get("href")?;
        let href = url::Url::parse(href).map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        let id: u32 = row.try_get("id")?;
        let fetched_at = row.try_get("fetched_at")?;
        let title = row.try_get("fetched_at")?;

        Ok(Persistent(
            id.into(),
            feeds::Feed {
                href,
                title,
                fetched_at,
            },
        ))
    }
}

impl sqlx::FromRow<'_, sqlx::sqlite::SqliteRow> for Persistent<feeds::Entry> {
    fn from_row(row: &'_ sqlx::sqlite::SqliteRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;

        let href: &str = row.try_get("href")?;
        let href = url::Url::parse(href).map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        let id: u32 = row.try_get("id")?;
        let feed_id: u32 = row.try_get("feed_id")?;
        let title = row.try_get("title")?;
        let published_at = row.try_get("published_at")?;

        Ok(Persistent(
            id.into(),
            feeds::Entry {
                href,
                feed_id: feed_id.into(),
                title,
                published_at,
            },
        ))
    }
}
