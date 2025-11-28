-- Your SQL goes here
CREATE TABLE users (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    surname VARCHAR(255) NOT NULL,
    username VARCHAR(255) NOT NULL,
    telegram_username VARCHAR(255),
    mail VARCHAR(255) NOT NULL,
    idada VARCHAR(255) UNIQUE NOT NULL,  
    starred_routes JSONB NOT NULL,
    auto JSONB
);

CREATE TABLE offers (
    session_id BIGINT PRIMARY KEY,
    driver_id BIGINT NOT NULL REFERENCES users(id),
    passengers_id BIGINT[] NOT NULL DEFAULT '{}',  
    start JSONB NOT NULL,
    arrival JSONB NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    arrival_time TIMESTAMPTZ NOT NULL,
    route JSONB NOT NULL,
    auto JSONB,
    seats_available SMALLINT NOT NULL,  
    stops JSONB[] NOT NULL DEFAULT '{}'::JSONB[]
);

CREATE TABLE requests (
    session_id BIGINT PRIMARY KEY,
    passenger_id BIGINT NOT NULL REFERENCES users(id),  -- Singular, foreign key
    driver_id BIGINT REFERENCES users(id),  -- Optional, foreign key
    start JSONB NOT NULL,
    arrival JSONB NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    route JSONB NOT NULL,
    driver_start JSONB,
    driver_arrival JSONB
);

CREATE TABLE ride_history (
    session_id BIGINT PRIMARY KEY,
    driver_id BIGINT NOT NULL REFERENCES users(id),  -- Foreign key
    passengers_id BIGINT[] NOT NULL,  -- Array of user IDs
    route JSONB NOT NULL,
    stops JSONB[] NOT NULL DEFAULT '{}'::JSONB[],  -- Array of stops
    start JSONB NOT NULL,
    arrival JSONB NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    arrival_time TIMESTAMPTZ NOT NULL
);
