CREATE TABLE inbox (
    consumer_name varchar(128) NOT NULL,
    message_id    varchar(64) NOT NULL,
    payload_hash  varchar(64) NOT NULL,
    received_at   timestamptz NOT NULL DEFAULT now(),
    completed_at  timestamptz,
    PRIMARY KEY (consumer_name, message_id)
);