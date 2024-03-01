DELETE FROM embeddings
WHERE id IN (
        SELECT
            embeddings.id
        FROM
            embeddings
            JOIN fields ON fields.md5_hash = embeddings.md5_hash
            JOIN entries ON fields.entry_id = entries.id
        WHERE
            entries.feed_id = 6);

DELETE FROM translations
WHERE id IN (
        SELECT
            translations.id
        FROM
            translations
            JOIN fields ON fields.md5_hash = translations.md5_hash
            JOIN entries ON fields.entry_id = entries.id
        WHERE
            entries.feed_id = 6);

DELETE FROM feeds
WHERE id = 6;

DELETE FROM entries
WHERE feed_id = 6;
