use crate::{
    clustering::{self, Embedding, ReportGroup},
    feeds,
    id::Id,
    md5_hash::Md5Hash,
    persisted::Persisted,
    web,
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
    #[tracing::instrument(level = "debug", skip_all, fields(href = %entry.href))]
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

    #[tracing::instrument(level = "debug", skip(self))]
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
    #[tracing::instrument(level = "debug", skip_all, fields(entry_id = %field.entry_id, name = %field.name, lang_code = %field.lang_code, md5_hash = ?field.md5_hash))]
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

    pub async fn find_field_by_entry_id_name_lang_code(
        &self,
        entry_id: &Id<feeds::Entry>,
        name: &feeds::FieldName,
        lang_code: &feeds::LanguageCode,
    ) -> Result<Option<Persisted<feeds::Field>>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM fields WHERE entry_id = ? AND lang_code = ? AND name = ?")
            .bind(entry_id)
            .bind(lang_code)
            .bind(name)
            .fetch_optional(&self.pool)
            .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
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
    #[tracing::instrument(level = "debug", skip_all, fields(md5_hash = ?embedding.md5_hash, size = %embedding.size))]
    pub async fn insert_embeddig(
        &self,
        embedding: &clustering::Embedding,
    ) -> Result<Option<Persisted<clustering::Embedding>>, sqlx::Error> {
        sqlx::query_as(
            "INSERT OR IGNORE INTO embeddings (md5_hash, value, size) VALUES ( ?, ?, ? ) RETURNING *",
        )
        .bind(embedding.md5_hash)
        .bind(serde_json::to_string(&embedding.value).expect("failed to serialize embedding"))
        .bind(embedding.size)
        .fetch_optional(&self.pool)
        .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
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

    #[tracing::instrument(level = "debug", skip(self))]
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
    #[tracing::instrument(level = "debug", skip_all, fields(md5_hash = ?transaslation.md5_hash))]
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

    #[tracing::instrument(level = "debug", skip(self), fields(md5_hash = ?md5_hash))]
    pub async fn find_translation_by_md5_hash(
        &self,
        md5_hash: &Md5Hash,
    ) -> Result<Persisted<feeds::Translation>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM translations WHERE md5_hash = ?")
            .bind(md5_hash)
            .fetch_one(&self.pool)
            .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn list_translations_without_embeddings_by_lang_code_field_name_date(
        &self,
        language_code: feeds::LanguageCode,
        field_name: feeds::FieldName,
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
                            AND fields.name = $3
                        JOIN entries
                            ON entries.id = fields.entry_id
                        WHERE
                            entries.published_at >= DATETIME($1, 'start of day')
                                AND entries.published_at < DATETIME($1, 'start of day', '+1 day')
                                AND NOT EXISTS (SELECT 1 FROM embeddings WHERE embeddings.md5_hash = translations.md5_hash)
                        GROUP BY translations.md5_hash")
            .bind(date)
            .bind(language_code)
            .bind(field_name)
            .fetch_all(&self.pool)
            .await
    }
}

impl Client {
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn insert_report_group(
        &self,
        group: clustering::ReportGroup,
    ) -> Result<Persisted<clustering::ReportGroup>, sqlx::Error> {
        use sqlx::{Executor, Row};

        let mut transaction = self.pool.begin().await?;

        let group_insert_result = transaction
            .fetch_one(
                sqlx::query(
                    "INSERT INTO report_groups (report_id, center_embedding_id) VALUES (?, ?) RETURNING id",
                )
                .bind(group.report_id)
                .bind(group.center_embedding_id),
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

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn insert_report(
        &self,
        report: &clustering::Report,
    ) -> Result<Persisted<clustering::Report>, sqlx::Error> {
        sqlx::query_as(
            "INSERT INTO reports (score, min_points, tolerance, rows, dimentions) VALUES (?, ?, ?, ?, ?) RETURNING *",
        )
        .bind(report.score)
        .bind(report.min_points)
        .bind(report.tolerance)
        .bind(report.rows)
        .bind(report.dimentions)
        .fetch_one(&self.pool)
        .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn list_report_group_entries_by_date_lang_code(
        &self,
        date: chrono::NaiveDate,
        lang_code: &feeds::LanguageCode,
    ) -> Result<Vec<web::GroupEntryView>, sqlx::Error> {
        let date = date
            .and_hms_opt(0, 0, 0)
            .expect("failed to create start of day");
        sqlx::query_as(
            "
            SELECT
                entries.group_id AS group_id,
                entries.is_center AS is_center,
                entries.href AS href,
                entries.published_at AS published_at,
                entries.feed_id AS feed_id,
                translations.value AS title
            FROM
                fields
                    JOIN translations ON translations.md5_hash = fields.md5_hash
                    JOIN (
                            SELECT
                                entries.id AS id,
                                (report_groups.center_embedding_id = embeddings.id) AS is_center,
                                report_group_embeddings.report_group_id AS group_id,
                                entries.href AS href,
                                entries.published_at AS published_at,
                                entries.feed_id AS feed_id
                            FROM
                                report_group_embeddings
                                    JOIN report_groups ON report_group_embeddings.report_group_id = report_groups.id
                                    JOIN embeddings ON embeddings.id = report_group_embeddings.embedding_id
                                    JOIN fields ON fields.md5_hash = embeddings.md5_hash
                                    JOIN entries ON entries.id = fields.entry_id
                            WHERE
                                report_groups.report_id = (
                                    SELECT
                                        id
                                    FROM
                                        reports
                                    WHERE
                                        created_at >= DATETIME($1, 'start of day')
                                            AND created_at < DATETIME($1, 'start of day', '+1 day')
                                    ORDER BY
                                        created_at DESC
                                    LIMIT 1
                                )
                        ) AS entries ON entries.id = fields.entry_id
            WHERE
                fields.lang_code = $2
                AND fields.name = 'title'
            ORDER BY
                entries.published_at DESC
            ",
        )
        .bind(date)
        .bind(lang_code)
        .fetch_all(&self.pool)
        .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn list_report_group_entries_by_id_lang_code(
        &self,
        id: Id<ReportGroup>,
        lang_code: &feeds::LanguageCode,
    ) -> Result<Vec<web::GroupEntryView>, sqlx::Error> {
        sqlx::query_as(
            "
            SELECT
                entries.group_id AS group_id,
                entries.is_center AS is_center,
                entries.href AS href,
                entries.published_at AS published_at,
                entries.feed_id AS feed_id,
                translations.value AS title
            FROM
                fields
                    JOIN translations ON translations.md5_hash = fields.md5_hash
                    JOIN (
                            SELECT
                                entries.id AS id,
                                report_group_embeddings.report_group_id AS group_id,
                                (report_groups.center_embedding_id = embeddings.id) AS is_center,
                                entries.href AS href,
                                entries.published_at AS published_at,
                                entries.feed_id AS feed_id
                            FROM
                                report_group_embeddings
                                    JOIN report_groups ON report_group_embeddings.report_group_id = report_groups.id
                                    JOIN embeddings ON embeddings.id = report_group_embeddings.embedding_id
                                    JOIN fields ON fields.md5_hash = embeddings.md5_hash
                                    JOIN entries ON entries.id = fields.entry_id
                            WHERE
                                report_group_embeddings.report_group_id = ?
                        ) AS entries ON entries.id = fields.entry_id
            WHERE
                fields.lang_code = ?
                AND fields.name = 'title'
            ORDER BY
                entries.published_at DESC
            ",
        )
        .bind(id)
        .bind(lang_code)
        .fetch_all(&self.pool)
        .await
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
