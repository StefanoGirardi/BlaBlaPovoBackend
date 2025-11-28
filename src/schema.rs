// @generated automatically by Diesel CLI.

diesel::table! {
    offers (session_id) {
        session_id -> Int8,
        driver_id -> Int8,
        passengers_id -> Array<Nullable<Int8>>,
        start -> Jsonb,
        arrival -> Jsonb,
        start_time -> Timestamptz,
        arrival_time -> Timestamptz,
        route -> Jsonb,
        auto -> Nullable<Jsonb>,
        seats_available -> Int2,
        stops -> Array<Nullable<Jsonb>>,
    }
}

diesel::table! {
    requests (session_id) {
        session_id -> Int8,
        passenger_id -> Int8,
        driver_id -> Nullable<Int8>,
        start -> Jsonb,
        arrival -> Jsonb,
        start_time -> Timestamptz,
        route -> Jsonb,
        driver_start -> Nullable<Jsonb>,
        driver_arrival -> Nullable<Jsonb>,
    }
}

diesel::table! {
    ride_history (session_id) {
        session_id -> Int8,
        driver_id -> Int8,
        passengers_id -> Array<Nullable<Int8>>,
        route -> Jsonb,
        stops -> Array<Nullable<Jsonb>>,
        start -> Jsonb,
        arrival -> Jsonb,
        start_time -> Timestamptz,
        arrival_time -> Timestamptz,
    }
}

diesel::table! {
    users (id) {
        id -> Int8,
        #[max_length = 255]
        name -> Varchar,
        #[max_length = 255]
        surname -> Varchar,
        #[max_length = 255]
        username -> Varchar,
        #[max_length = 255]
        telegram_username -> Nullable<Varchar>,
        #[max_length = 255]
        mail -> Varchar,
        #[max_length = 255]
        idada -> Varchar,
        starred_routes -> Jsonb,
        auto -> Nullable<Jsonb>,
    }
}

diesel::joinable!(offers -> users (driver_id));
diesel::joinable!(ride_history -> users (driver_id));

diesel::allow_tables_to_appear_in_same_query!(offers, requests, ride_history, users,);
