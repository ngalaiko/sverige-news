CREATE TABLE IF NOT EXISTS feeds (
    id integer PRIMARY KEY AUTOINCREMENT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    /* --- */
    href text NOT NULL,
    title text NOT NULL
);

INSERT INTO feeds (title, href)
    VALUES ('SVT Nyheter', 'https://www.svt.se/rss.xml'),
    ('Dagens Nyheter', 'https://www.dn.se/rss/'),
    ('Svenska Dagbladet', 'https://www.svd.se/feed/articles.rss'),
    ('Aftonbladet', 'https://rss.aftonbladet.se/rss2/small/pages/sections/senastenytt/'),
    ('Expressen', 'https://feeds.expressen.se/nyheter/'),
    ('Nya Wermlands-Tidningen', 'https://nwt.se/feed/'),
    ('Dagen', 'https://dagen.se/arc/outboundfeeds/rss'),
    ('Nyheter Idag', 'https://nyheteridag.se/feed'),
    ('SVT Nyheter', 'https://svt.se/rss.xml'),
    ('TV4', 'https://www.tv4.se:443/rss'),
    ('TT Nyhetsbyråns', 'https://blogg.tt.se/feed'),
    ('ABC News', 'https://abcnyheter.se/feed'),
    ('Nkpg News', 'https://nkpg.news/feed/'),
    ('Skaraborgs Nyheter RSS Feed', 'https://skaraborgsnyheter.se/feed');

CREATE TABLE IF NOT EXISTS entries (
    id integer PRIMARY KEY AUTOINCREMENT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    href text NOT NULL UNIQUE,
    feed_id integer NOT NULL,
    published_at DATETIME NOT NULL
);

CREATE TABLE IF NOT EXISTS fields (
    id integer PRIMARY KEY AUTOINCREMENT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    /* --- */
    entry_id integer NOT NULL,
    name text NOT NULL,
    lang_code text NOT NULL,
    md5_hash BINARY NOT NULL,
    UNIQUE(entry_id, name, lang_code)
);

CREATE TABLE IF NOT EXISTS translations (
    id integer PRIMARY KEY AUTOINCREMENT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    /* --- */
    md5_hash BINARY NOT NULL UNIQUE,
    value text NOT NULL
);

CREATE TABLE IF NOT EXISTS embeddings (
    id integer PRIMARY KEY AUTOINCREMENT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    /* --- */
    md5_hash BINARY NOT NULL UNIQUE,
    value text NOT NULL,
    size integer NOT NULL
);
