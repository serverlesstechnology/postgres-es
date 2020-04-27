-- a single table is used for all events in the cqrs system
CREATE TABLE events
(
    aggregate_type text                         NOT NULL,
    aggregate_id   text                         NOT NULL,
    sequence       bigint CHECK (sequence >= 0) NOT NULL,
    payload        jsonb                        NOT NULL,
    metadata       jsonb                        NOT NULL,
    timestamp      timestamp with time zone DEFAULT (CURRENT_TIMESTAMP),
    PRIMARY KEY (aggregate_type, aggregate_id, sequence)
);

-- one view table should be created for every `GenericQueryRepository` used
-- replace name with the value used in `GenericQueryRepository::new(query_name: String)`
CREATE TABLE test_query
(
    query_instance_id text                        NOT NULL,
    version           bigint CHECK (version >= 0) NOT NULL,
    payload           jsonb                       NOT NULL,
    PRIMARY KEY (query_instance_id)
);

CREATE USER test_user WITH ENCRYPTED PASSWORD 'test_pass';
GRANT ALL PRIVILEGES ON DATABASE postgres TO test_user;