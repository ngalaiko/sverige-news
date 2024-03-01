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

        sqlx::migrate!("./migrations").run(&pool).await?;
        Ok(Self { pool })
    }
}

impl Client {
    #[tracing::instrument(skip_all, fields(href = %entry.href))]
    pub async fn insert_entry(
        &self,
        entry: &feeds::Entry,
    ) -> Result<Option<Persisted<feeds::Entry>>, sqlx::Error> {
        sqlx::query_as(
            "INSERT OR IGNORE INTO entries (href, feed_id, published_at) VALUES ( ?, ?, ?) RETURNING *",
        )
        .bind(entry.href.to_string())
        .bind(u32::from(entry.feed_id))
        .bind(entry.published_at)
        .fetch_optional(&self.pool)
        .await
    }

    #[tracing::instrument(skip(self))]
    pub async fn find_entry_by_id(
        &self,
        id: Id<feeds::Entry>,
    ) -> Result<Persisted<feeds::Entry>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM entries WHERE id = ?")
            .bind(id)
            .fetch_one(&self.pool)
            .await
    }
}

impl Client {
    #[tracing::instrument(skip_all, fields(entry_id = %field.entry_id, name = %field.name, lang_code = %field.lang_code, md5_hash = ?field.md5_hash))]
    pub async fn insert_field(
        &self,
        field: &feeds::Field,
    ) -> Result<Option<Persisted<feeds::Field>>, sqlx::Error> {
        sqlx::query_as("INSERT OR IGNORE INTO fields (entry_id, name, lang_code, md5_hash) VALUES (?, ?, ?, ?) RETURNING *")
            .bind(u32::from(field.entry_id))
            .bind(field.name.to_string())
            .bind(field.lang_code.to_string())
            .bind(field.md5_hash.to_vec())
            .fetch_optional(&self.pool)
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
    pub async fn list_fields_md5_hash(
        &self,
        md5_hash: &md5::Digest,
    ) -> Result<Vec<Persisted<feeds::Field>>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM fields WHERE md5_hash = ?")
            .bind(md5_hash.to_vec())
            .fetch_all(&self.pool)
            .await
    }
}

impl Client {
    #[tracing::instrument(skip_all, fields(md5_hash = ?embedding.md5_hash, size = %embedding.size))]
    pub async fn insert_embeddig(
        &self,
        embedding: &Embedding,
    ) -> Result<Option<Persisted<Embedding>>, sqlx::Error> {
        sqlx::query_as(
            "INSERT OR IGNORE INTO embeddings (md5_hash, value, size) VALUES ( ?, ?, ? ) RETURNING *",
        )
        .bind(embedding.md5_hash.to_vec())
        .bind(serde_json::to_string(&embedding.value).expect("failed to serialize embedding"))
        .bind(embedding.size)
        .fetch_optional(&self.pool)
        .await
    }

    #[tracing::instrument(skip(self))]
    pub async fn list_embeddings_by_field_name_lang_code_date(
        &self,
        field_name: feeds::FieldName,
        lang_code: feeds::LanguageCode,
        date: chrono::NaiveDate,
    ) -> Result<Vec<Persisted<Embedding>>, sqlx::Error> {
        let date = date
            .and_hms_opt(0, 0, 0)
            .expect("failed to create start of day");

        sqlx::query_as(
            "SELECT embeddings.*
                        FROM embeddings
                        JOIN fields ON
                            fields.md5_hash = embeddings.md5_hash
                            AND fields.lang_code = $1
                            AND fields.name = $2
                        JOIN entries ON
                            entries.id = fields.entry_id
                        WHERE
                            entries.published_at >= DATETIME($3, 'start of day')
                            AND entries.published_at < DATETIME($3, 'start of day', '+1 day')
                        GROUP BY embeddings.md5_hash
                        ",
        )
        .bind(lang_code.to_string())
        .bind(field_name.to_string())
        .bind(date)
        .fetch_all(&self.pool)
        .await
    }

    #[tracing::instrument(skip(self))]
    pub async fn find_embedding_by_id(
        &self,
        id: Id<Embedding>,
    ) -> Result<Persisted<Embedding>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM embeddings WHERE id = ?")
            .bind(id)
            .fetch_one(&self.pool)
            .await
    }
}

impl Client {
    #[tracing::instrument(skip_all, fields(md5_hash = ?transaslation.md5_hash))]
    pub async fn insert_translation(
        &self,
        transaslation: &feeds::Translation,
    ) -> Result<Option<Persisted<feeds::Translation>>, sqlx::Error> {
        sqlx::query_as(
            "INSERT OR IGNORE INTO translations (md5_hash, value) VALUES (?, ?) RETURNING *",
        )
        .bind(transaslation.md5_hash.to_vec())
        .bind(transaslation.value.to_string())
        .fetch_optional(&self.pool)
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
    pub async fn list_translations_without_embeddings(
        &self,
        language_code: feeds::LanguageCode,
    ) -> Result<Vec<Persisted<feeds::Translation>>, sqlx::Error> {
        sqlx::query_as("SELECT translations.* FROM translations JOIN fields on fields.md5_hash = translations.md5_hash AND fields.lang_code = ? WHERE NOT EXISTS (SELECT 1 FROM embeddings WHERE embeddings.md5_hash = translations.md5_hash)")
            .bind(language_code.to_string())
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

impl Client {
    pub async fn insert_report(&self, report: &Report) -> Result<Persisted<Report>, sqlx::Error> {
        use sqlx::{Executor, Row};

        let mut transaction = self.pool.begin().await?;

        let report_insert_result = transaction
            .fetch_one(
                sqlx::query("INSERT INTO reports (score) VALUES (?) RETURNING id, created_at")
                    .bind(report.score),
            )
            .await?;

        let report_id: u32 = report_insert_result.try_get("id")?;
        let report_created_at: chrono::DateTime<chrono::Utc> =
            report_insert_result.try_get("created_at")?;

        for embedding_ids in &report.groups {
            let group_insert_result = transaction
                .fetch_one(
                    sqlx::query("INSERT INTO report_groups (report_id) VALUES (?) RETURNING id")
                        .bind(report_id),
                )
                .await?;
            let group_id: u32 = group_insert_result.try_get("id")?;

            for embedding_id in embedding_ids {
                transaction
                        .execute(
                            sqlx::query("INSERT INTO report_group_embeddings (report_group_id, embedding_id) VALUES (?, ?)")
                                .bind(group_id)
                                .bind(embedding_id),
                        ).await?;
            }
        }

        transaction.commit().await?;

        Ok(Persisted {
            id: Id::from(report_id),
            created_at: report_created_at,
            value: report.clone(),
        })
    }

    pub async fn find_latest_report(&self) -> Result<Persisted<Report>, sqlx::Error> {
        use sqlx::Row;

        let report_result = sqlx::query(
            "SELECT id, created_at, score FROM reports ORDER BY created_at DESC LIMIT 1",
        )
        .fetch_one(&self.pool)
        .await?;

        let report_id: u32 = report_result.try_get("id")?;
        let report_created_at: chrono::DateTime<chrono::Utc> =
            report_result.try_get("created_at")?;
        let score = report_result.try_get("score")?;

        let mut report = Report {
            score,
            groups: vec![],
        };

        let report_groups_result = sqlx::query("SELECT id FROM report_groups WHERE report_id = ?")
            .bind(report_id)
            .fetch_all(&self.pool)
            .await?;

        for group_result in report_groups_result {
            let group_id = group_result.try_get::<u32, _>("id")?;
            let group_embeddings = self.list_group_embedding_ids(group_id).await?;
            report.groups.push(group_embeddings);
        }

        Ok(Persisted {
            id: Id::from(report_id),
            created_at: report_created_at,
            value: report,
        })
    }

    async fn list_group_embedding_ids(
        &self,
        group_id: u32,
    ) -> Result<Vec<Id<Embedding>>, sqlx::Error> {
        use sqlx::Row;

        let group_embeddings_results = sqlx::query(
            "SELECT embedding_id FROM report_group_embeddings WHERE report_group_id = ?",
        )
        .bind(group_id)
        .fetch_all(&self.pool)
        .await?;

        group_embeddings_results
            .into_iter()
            .map(|result| result.try_get::<u32, _>("embedding_id").map(Id::from))
            .collect::<Result<_, _>>()
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

#[derive(Debug, Clone)]
pub struct Report {
    pub score: f32,
    pub groups: Vec<Vec<Id<Embedding>>>,
}
