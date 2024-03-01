CREATE TABLE IF NOT EXISTS reports (
    id integer PRIMARY KEY AUTOINCREMENT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    /* --- */
    score real NOT NULL
);

CREATE TABLE IF NOT EXISTS report_groups (
    id integer PRIMARY KEY AUTOINCREMENT,
    report_id integer NOT NULL
);

CREATE TABLE IF NOT EXISTS report_group_embeddings (
    report_group_id integer NOT NULL,
    embedding_id integer NOT NULL,
    PRIMARY KEY (report_group_id, embedding_id)
);
