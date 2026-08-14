-- `IF NOT EXISTS` because databases created before migrations were introduced
-- already have this table but no `_sqlx_migrations` bookkeeping.
CREATE TABLE IF NOT EXISTS jobs (
    id              TEXT PRIMARY KEY,
    url             TEXT NOT NULL,
    mode            TEXT NOT NULL CHECK (mode IN ('video','audio')),
    status          TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'done','failed','expired')),
    error           TEXT,
    file            TEXT,
    created_at      INTEGER NOT NULL DEFAULT (strftime('%s','now')),
    completed_at    INTEGER,
    last_download_at INTEGER
);