use crate::id::Id;

#[derive(Debug, Clone)]
pub struct Persisted<T> {
    pub id: Id<T>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub value: T,
}

impl<'a, T> sqlx::FromRow<'a, sqlx::sqlite::SqliteRow> for Persisted<T>
where
    T: sqlx::FromRow<'a, sqlx::sqlite::SqliteRow>,
{
    fn from_row(row: &'a sqlx::sqlite::SqliteRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;

        let id = row.try_get("id")?;
        let created_at = row.try_get("created_at")?;
        let value = T::from_row(row)?;

        Ok(Persisted {
            id,
            created_at,
            value,
        })
    }
}
