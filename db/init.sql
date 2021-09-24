-- a single table is used for all events in the cqrs system
CREATE TABLE events
(
    aggregate_type text                         NOT NULL,
    aggregate_id   text                         NOT NULL,
    sequence       bigint CHECK (sequence >= 0) NOT NULL,
    payload        text                         NOT NULL,
    metadata       text                         NOT NULL,
    timestamp      timestamp with time zone DEFAULT (CURRENT_TIMESTAMP),
    PRIMARY KEY (aggregate_type, aggregate_id, sequence)
);

-- this table is only needed if snapshotting is employed
CREATE TABLE snapshots
(
    aggregate_type   text                                 NOT NULL,
    aggregate_id     text                                 NOT NULL,
    last_sequence    bigint CHECK (last_sequence >= 0)    NOT NULL,
    current_snapshot bigint CHECK (current_snapshot >= 0) NOT NULL,
    payload          text                                 NOT NULL,
    timestamp        timestamp with time zone DEFAULT (CURRENT_TIMESTAMP),
    PRIMARY KEY (aggregate_type, aggregate_id, last_sequence)
);

-- one view table should be created for every `GenericQueryRepository` used
-- replace name with the value used in `GenericQueryRepository::new(query_name: String)`
CREATE TABLE test_query
(
    query_instance_id text                        NOT NULL,
    version           bigint CHECK (version >= 0) NOT NULL,
    payload           text                        NOT NULL,
    PRIMARY KEY (query_instance_id)
);

CREATE USER test_user WITH ENCRYPTED PASSWORD 'test_pass';
GRANT ALL PRIVILEGES ON DATABASE postgres TO test_user;