use crate::{
    clustering::{self, Embedding},
    feeds,
    id::Id,
    md5_hash::Md5Hash,
    persisted::Persisted,
};

#[derive(Clone)]
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
        .bind(entry.feed_id)
        .bind(entry.published_at)
        .fetch_optional(&self.pool)
        .await
    }

    #[tracing::instrument(skip(self))]
    pub async fn find_entry_by_id(
        &self,
        id: &Id<feeds::Entry>,
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
        field: feeds::Field,
    ) -> Result<Option<Persisted<feeds::Field>>, sqlx::Error> {
        sqlx::query_as("INSERT OR IGNORE INTO fields (entry_id, name, lang_code, md5_hash) VALUES (?, ?, ?, ?) RETURNING *")
            .bind(field.entry_id)
            .bind(field.name.to_string())
            .bind(field.lang_code.to_string())
            .bind(field.md5_hash)
            .fetch_optional(&self.pool)
            .await
    }

    pub async fn find_field_by_entry_id_lang_code(
        &self,
        entry_id: &Id<feeds::Entry>,
        lang_code: &feeds::LanguageCode,
    ) -> Result<Option<Persisted<feeds::Field>>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM fields WHERE entry_id = ? AND lang_code = ?")
            .bind(entry_id)
            .bind(lang_code.to_string())
            .fetch_optional(&self.pool)
            .await
    }

    #[tracing::instrument(skip(self))]
    pub async fn list_fields_by_md5_hash(
        &self,
        md5_hash: &Md5Hash,
    ) -> Result<Vec<Persisted<feeds::Field>>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM fields WHERE md5_hash = ?")
            .bind(md5_hash)
            .fetch_all(&self.pool)
            .await
    }
}

impl Client {
    #[tracing::instrument(skip_all, fields(md5_hash = ?embedding.md5_hash, size = %embedding.size))]
    pub async fn insert_embeddig(
        &self,
        embedding: &clustering::Embedding,
    ) -> Result<Option<Persisted<clustering::Embedding>>, sqlx::Error> {
        sqlx::query_as(
            "INSERT OR IGNORE INTO embeddings (md5_hash, value, size) VALUES ( ?, ?, ? ) RETURNING *",
        )
        .bind(&embedding.md5_hash)
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
    ) -> Result<Vec<Persisted<clustering::Embedding>>, sqlx::Error> {
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
        id: &Id<clustering::Embedding>,
    ) -> Result<Persisted<clustering::Embedding>, sqlx::Error> {
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
        transaslation: feeds::Translation,
    ) -> Result<Option<Persisted<feeds::Translation>>, sqlx::Error> {
        sqlx::query_as(
            "INSERT OR IGNORE INTO translations (md5_hash, value) VALUES (?, ?) RETURNING *",
        )
        .bind(transaslation.md5_hash)
        .bind(transaslation.value.to_string())
        .fetch_optional(&self.pool)
        .await
    }

    #[tracing::instrument(skip(self), fields(md5_hash = ?md5_hash))]
    pub async fn find_translation_by_md5_hash(
        &self,
        md5_hash: &Md5Hash,
    ) -> Result<Persisted<feeds::Translation>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM translations WHERE md5_hash = ?")
            .bind(md5_hash)
            .fetch_one(&self.pool)
            .await
    }

    #[tracing::instrument(skip(self))]
    pub async fn list_translations_without_embeddings_by_lang_code_date(
        &self,
        language_code: feeds::LanguageCode,
        date: &chrono::NaiveDate,
    ) -> Result<Vec<Persisted<feeds::Translation>>, sqlx::Error> {
        let date = date
            .and_hms_opt(0, 0, 0)
            .expect("failed to create start of day");
        sqlx::query_as("SELECT translations.*
                        FROM translations
                        JOIN fields
                            ON fields.md5_hash = translations.md5_hash
                            AND fields.lang_code = $2
                        JOIN entries
                            ON entries.id = fields.entry_id
                        WHERE
                            entries.published_at >= DATETIME($1, 'start of day')
                                AND entries.published_at < DATETIME($1, 'start of day', '+1 day')
                                AND NOT EXISTS (SELECT 1 FROM embeddings WHERE embeddings.md5_hash = translations.md5_hash)
                        GROUP BY translations.md5_hash")
            .bind(date)
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

    #[tracing::instrument(skip(self))]
    pub async fn find_feed_by_id(
        &self,
        id: &Id<feeds::Feed>,
    ) -> Result<Persisted<feeds::Feed>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM feeds WHERE id = ?")
            .bind(id)
            .fetch_one(&self.pool)
            .await
    }
}

impl Client {
    #[tracing::instrument(skip_all)]
    pub async fn insert_report_group(
        &self,
        group: clustering::ReportGroup,
    ) -> Result<Persisted<clustering::ReportGroup>, sqlx::Error> {
        use sqlx::{Executor, Row};

        let mut transaction = self.pool.begin().await?;

        let group_insert_result = transaction
            .fetch_one(
                sqlx::query("INSERT INTO report_groups (report_id) VALUES (?) RETURNING id")
                    .bind(group.report_id),
            )
            .await?;
        let group_id = group_insert_result.try_get("id")?;

        for embedding_id in &group.embedding_ids {
            transaction.execute(
                sqlx::query("INSERT INTO report_group_embeddings (report_group_id, embedding_id) VALUES (?, ?)")
                    .bind(group_id)
                    .bind(embedding_id),
            ).await?;
        }

        transaction.commit().await?;

        Ok(Persisted {
            id: group_id,
            created_at: chrono::Utc::now(),
            value: group.clone(),
        })
    }

    #[tracing::instrument(skip(self))]
    pub async fn insert_report(
        &self,
        report: &clustering::Report,
    ) -> Result<Persisted<clustering::Report>, sqlx::Error> {
        sqlx::query_as("INSERT INTO reports (score) VALUES (?) RETURNING *")
            .bind(report.score)
            .fetch_one(&self.pool)
            .await
    }

    #[tracing::instrument(skip(self))]
    pub async fn find_latest_report(&self) -> Result<Persisted<clustering::Report>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM reports ORDER BY created_at DESC LIMIT 1")
            .fetch_one(&self.pool)
            .await
    }

    #[tracing::instrument(skip(self))]
    pub async fn find_group_by_id(
        &self,
        id: &Id<clustering::ReportGroup>,
    ) -> Result<Persisted<clustering::ReportGroup>, sqlx::Error> {
        use sqlx::Row;

        let group_result = sqlx::query("SELECT * FROM report_groups WHERE id = ?")
            .bind(id)
            .fetch_one(&self.pool)
            .await?;

        let id = group_result.try_get("id")?;
        let created_at: chrono::DateTime<chrono::Utc> = group_result.try_get("created_at")?;
        let report_id = group_result.try_get("report_id")?;
        Ok(Persisted {
            id,
            created_at,
            value: clustering::ReportGroup {
                report_id,
                embedding_ids: self.list_embedding_ids_by_group_id(&id).await?,
            },
        })
    }

    #[tracing::instrument(skip(self))]
    pub async fn list_groups_by_report_id(
        &self,
        report_id: &Id<clustering::Report>,
    ) -> Result<Vec<Persisted<clustering::ReportGroup>>, sqlx::Error> {
        use sqlx::Row;

        let groups = sqlx::query("SELECT * FROM report_groups WHERE report_id = ?")
            .bind(report_id)
            .fetch_all(&self.pool)
            .await?;

        let mut result = Vec::with_capacity(groups.len());
        for group in groups {
            let id = group.try_get("id")?;
            let created_at: chrono::DateTime<chrono::Utc> = group.try_get("created_at")?;
            let value = clustering::ReportGroup {
                report_id: *report_id,
                embedding_ids: self.list_embedding_ids_by_group_id(&id).await?,
            };
            result.push(Persisted {
                id,
                created_at,
                value,
            });
        }

        Ok(result)
    }

    #[tracing::instrument(skip(self))]
    pub async fn list_embedding_ids_by_group_id(
        &self,
        group_id: &Id<clustering::ReportGroup>,
    ) -> Result<Vec<Id<clustering::Embedding>>, sqlx::Error> {
        use sqlx::Row;

        let rows = sqlx::query(
            "SELECT embedding_id FROM report_group_embeddings WHERE report_group_id = ?",
        )
        .bind(group_id)
        .fetch_all(&self.pool)
        .await?;

        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let id = row.try_get("embedding_id")?;
            result.push(id);
        }

        Ok(result)
    }
}

impl<'a> sqlx::FromRow<'a, sqlx::sqlite::SqliteRow> for Embedding {
    fn from_row(row: &'a sqlx::sqlite::SqliteRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;

        let md5_hash = row.try_get("md5_hash")?;
        let size: u32 = row.try_get("size")?;

        let value: String = row.try_get("value")?;
        let value =
            serde_json::from_str(&value).map_err(|error| sqlx::Error::Decode(Box::new(error)))?;

        Ok(Embedding {
            md5_hash,
            value,
            size,
        })
    }
}
