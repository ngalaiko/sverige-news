#[derive(Clone, Copy)]
pub struct Md5Hash(md5::Digest);

pub fn compute<T: AsRef<[u8]>>(data: T) -> Md5Hash {
    Md5Hash(md5::compute(data))
}

impl std::fmt::Debug for Md5Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl sqlx::Type<sqlx::Sqlite> for Md5Hash {
    fn type_info() -> <sqlx::Sqlite as sqlx::Database>::TypeInfo {
        <Vec<u8> as sqlx::Type<sqlx::Sqlite>>::type_info()
    }
}

impl<'a> sqlx::Encode<'a, sqlx::sqlite::Sqlite> for Md5Hash {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::sqlite::Sqlite as sqlx::database::HasArguments<'a>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        <Vec<u8> as sqlx::Encode<'a, sqlx::sqlite::Sqlite>>::encode(self.0.to_vec(), buf)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid MD5 hash length: {0} bytes, expected 16 bytes.")]
struct InvalidMd5HashLength(usize);

impl sqlx::Decode<'_, sqlx::sqlite::Sqlite> for Md5Hash {
    fn decode(
        value: sqlx::sqlite::SqliteValueRef<'_>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let bytes = <Vec<u8> as sqlx::Decode<sqlx::sqlite::Sqlite>>::decode(value)?;
        let bytes_len = bytes.len();
        let md5_hash: [u8; 16] = bytes
            .try_into()
            .map_err(|_| sqlx::Error::Decode(Box::new(InvalidMd5HashLength(bytes_len))))?;
        Ok(Md5Hash(md5::Digest(md5_hash)))
    }
}
