ALTER TABLE reports
    ADD COLUMN min_points INTEGER NOT NULL DEFAULT -1;

ALTER TABLE reports
    ADD COLUMN tolerance REAL NOT NULL DEFAULT 0.0;
