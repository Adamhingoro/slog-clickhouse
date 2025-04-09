CREATE DATABASE IF NOT EXISTS logging;

CREATE TABLE IF NOT EXISTS logging.logs (
   timestamp DateTime,
   level String,
   message String,
   hostname String,
   attributes Map(String, String),
   namespace String,
   service String,
   uid Nullable(Int32) DEFAULT NULL,
   request_id  Nullable(String) DEFAULT NULL
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp)
TTL timestamp + INTERVAL 1 WEEK DELETE;

