pub mod utils;
pub mod schema;
pub mod test;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use dashmap::DashMap;
use tokio::sync::Mutex;
use crate::utils::background_tasks;
use crate::utils::models::offer::Offer;
use crate::utils::models::request::Request;
use crate::utils::sse_endpoint::{self, OfferBroadcaster, RequestBroadcaster};
use crate::utils::telegram_serv::TelegramService;
use rocket::data::ToByteUnit;
use rocket::launch;
use rocket::routes;
use rocket_cors::CorsOptions;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::AsyncPgConnection;
use bb8::Pool;



pub type ResourceMapOffers = DashMap<i64,Arc<Mutex<Offer>>>;
pub type ResourceMapRequests = DashMap<i64,Arc<Mutex<Request>>>;
pub type PgPool = Pool<AsyncDieselConnectionManager<AsyncPgConnection>>;


async fn create_pool(database_url: &str) -> PgPool {
    let config = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    Pool::builder()
        .max_size(100)
        .connection_timeout(Duration::from_secs(30))
        .build(config)
        .await
        .expect("Failed to create pool")
}

#[launch]
async fn rocket() -> _ {
    dotenvy::dotenv().ok();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    
    let pool = create_pool(database_url.as_str()).await;
    

    let _background_task = background_tasks::start_cleanup_task(pool.clone()).await;  
    let resource_map_request: ResourceMapRequests = DashMap::new();
    let resource_map_offer: ResourceMapOffers = DashMap::new();
    let web_socket= Arc::new(sse_endpoint::WebSocketManager::new());

    let bot_token = std::env::var("TELEGRAM_BOT_TOKEN")
        .expect("TELEGRAM_BOT_TOKEN must be set");
    
    let telegram_service = TelegramService::new(bot_token);
    telegram_service.start_bot().await;

    let tls_config = rocket::config::TlsConfig::from_paths("perms/cert.pem", "perms/key.pem");
    
    let cors = CorsOptions::default()
        .to_cors()
        .expect("CORS failed");

    let config = rocket::Config {
        address: "127.0.0.1".parse().unwrap(),
        // port: 8002,
        port: 8000,
        // tls: Some(tls_config),
        limits: rocket::data::Limits::new()
            .limit("json", 100.mebibytes())
            .limit("data", 100.mebibytes()),
        ..rocket::Config::default()
    };

    rocket::build()
        .attach(cors)
        .manage(pool)
        .manage(resource_map_offer)
        .manage(resource_map_request)
        .manage(web_socket)
        .manage(telegram_service)
        .mount("/api", 
            routes![
                utils::models::user::create_new_user,
                utils::models::user::get_user_on_id,
                utils::models::user::get_req_on_id,
                utils::models::user::resign_driver,
                utils::models::user::delete_user,
                utils::models::user::new_starred_route,
                utils::models::user::patch_route,
                utils::models::user::modify_user_car,
                utils::models::user::modify_username,
                utils::models::user::modify_telegram_username,
                utils::models::user::auth_login,
                utils::models::user::login,
                utils::models::user::get_username,
                utils::models::user::get_telegram_username,
                utils::models::user::get_user_full_name,
                utils::models::user::test_re,
                utils::models::user::get_user_info,
                utils::models::request::create_request,
                utils::models::request::assign_driver,
                utils::models::request::get_all_request,
                utils::models::request::get_all_my_request,
                utils::models::request::get_all_my_request_to_take,
                utils::models::request::modify_request_time,
                utils::models::request::modify_start,
                utils::models::request::modify_arrival,
                utils::models::request::modify_driver_start,
                utils::models::request::modify_driver_arrival,
                utils::models::request::delete_request,
                utils::models::request::take_request,
                utils::models::request::get_request_by_id,
                utils::models::request::resign_driver,
                utils::models::request::renounce_driver,
                utils::models::offer::create_offer,
                utils::models::offer::get_all_offers,
                utils::models::offer::get_all_my_offers,
                utils::models::offer::get_all_offers_to_take,
                utils::models::offer::take_offer,
                utils::models::offer::my_offers,
                utils::models::offer::modify_offer_time,
                utils::models::offer::modify_route,
                utils::models::offer::modify_stops,
                utils::models::offer::get_offer_by_id,
                utils::models::offer::check_and_decrease,
                utils::models::offer::increase_seat,
                utils::models::offer::renounce_seat,
                utils::models::offer::delete_offer,
                utils::sse_endpoint::offers_sse,
                utils::sse_endpoint::requests_sse,
                utils::telegram_serv::initiate_telegram_chat,
                utils::models::ride_history::get_all_request_history,
                utils::models::ride_history::get_all_offers_history,
            ]
        )
        .configure(config)

}