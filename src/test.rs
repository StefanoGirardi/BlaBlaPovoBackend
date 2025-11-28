pub mod offer_test;
pub mod request_test;
use std::sync::Arc;
use crate::utils::*;
use crate::{utils::{models::user::create_new_user, sse_endpoint::WebSocketManager,jwt_management::Tokens}, PgPool, ResourceMapOffers, ResourceMapRequests};
use rocket_cors::Cors;
use rocket::{http::{ContentType,Header,Status}, local::asynchronous::Client, routes, serde::json::Json};
use diesel_async::{pooled_connection::AsyncDieselConnectionManager, AsyncConnection, AsyncPgConnection};
use bb8::Pool;

use diesel::prelude::*;
use diesel_async::RunQueryDsl;

#[rocket::async_test]
#[serial_test::serial(delete_scope)]
    pub async fn delete_all() {
        let (_,b) = setup_test_client_and_pool().await;

        let a = clear_all_tables(&b.clone()).await;
        assert!(a.is_ok());
    }

#[rocket::async_test]
#[serial_test::serial(offers_scope)]
    pub async fn offers() {

        let (id1, id2) = (1,2);

        let (a,b) = setup_test_client_and_pool().await;
        let (c,d) = setup_test_user_and_token(&a,id1).await;
        let (c1,d1) = setup_test_user_and_token(&a,id2).await;

        user_name("Yoshi".to_string(),&a,d.clone()).await;
        user_name("Mario".to_string(),&a,d1.clone()).await;
        
        let (c,d) = get_test_user_and_token(&a,id1).await;
        let (c1,d1) = get_test_user_and_token(&a,id2).await;

        offer_test::offer_test::test_create_offer(&a, d.clone(),id1).await;
        offer_test::offer_test::test_get_all_offers(&a, d1.clone(), c1.clone(),id2).await;
        offer_test::offer_test::test_get_offer_by_id(&a, &b.clone(), d.clone(),id1).await;
        offer_test::offer_test::test_check_and_decrease(&a, d1.clone(),id2).await;
        offer_test::offer_test::test_take_offer(&a,d.clone(),d1.clone(),id2).await;
        offer_test::offer_test::test_my_offers(&a,d.clone(),c.clone(),id1).await;
        offer_test::offer_test::test_all_my_offers(&a,d.clone(),c.clone(),id1).await;
        offer_test::offer_test::test_all_offers_to_take(&a,d1.clone(),c1.clone(),id2).await;
        offer_test::offer_test::test_renounce(&a,d1.clone(),id2).await;
        offer_test::offer_test::test_modify_route(&a,d.clone(),id1).await;
        offer_test::offer_test::test_modify_time(&a,d.clone(),id1).await;
        offer_test::offer_test::test_check_and_decrease(&a,d1.clone(),id2).await;
        offer_test::offer_test::test_increase(&a,d1.clone(),id2).await;
        offer_test::offer_test::test_delete_offer(&a,d.clone(),id1).await;
        
        let res = clear_all_tables(&b.clone()).await;
        assert!(res.is_ok());
    } 

#[rocket::async_test]
#[serial_test::serial(request_scope)]
    pub async fn requests() {

        let (id1, id2) = (3,4);
        let (a,b) = setup_test_client_and_pool().await;
        let (c,d) = setup_test_user_and_token(&a,id1).await;
        let (c1,d1) = setup_test_user_and_token(&a,id2).await;

        user_name("Yoshi".to_string(),&a,d.clone()).await;
        user_name("Mario".to_string(),&a,d1.clone()).await;
        
        let (c,d) = get_test_user_and_token(&a,id1).await;
        let (c1,d1) = get_test_user_and_token(&a,id2).await;

        request_test::request_test::test_create_request(&a, d.clone(),id1).await;
        request_test::request_test::test_get_all_requests(&a, d1.clone(), c1.clone(),id2).await;
        request_test::request_test::test_take_request(&a,d.clone(),d1.clone(),id2).await;
        request_test::request_test::test_all_my_requests(&a,d.clone(),c.clone(),id1).await;
        request_test::request_test::test_all_requests_to_take(&a,d1.clone(),c1.clone(),id2).await;
        request_test::request_test::test_modify_start(&a,d.clone(),id1).await;
        request_test::request_test::test_modify_arrival(&a,d.clone(),id1).await;
        request_test::request_test::test_modify_time(&a,d.clone(),id1).await;
        request_test::request_test::test_delete_request(&a,d.clone(),id1).await;
        
        let res = clear_all_tables(&b.clone()).await;
        assert!(res.is_ok());
    } 

#[rocket::async_test]
#[serial_test::serial(conc_scope)]

    pub async fn concurrency() {
        use tokio::task::JoinSet;
        use std::sync::Arc;

        let (aa, b) = setup_test_client_and_pool().await;
        let a = Arc::new(aa);
        let mut vec: Vec<(crate::utils::models::user::User, Tokens)> = Vec::new();
        for i in 100000..100004 {
            let (c, d) = setup_test_user_and_token(&a, i).await;
            user_name(format!("Yoshi{}", i), &a, d.clone()).await;
            let (c, d) = get_test_user_and_token(&a, i).await;
            if i==1 {
                offer_test::offer_test::test_create_offer(&a, d.clone(),1).await;
            }
            vec.push((c, d));
        }
        
        let mut join_set = JoinSet::new();
        
        for (i, (_, token)) in vec.iter().enumerate() {
            if i!=0 {
                let token_clone = token.clone();
                let client_clone= Arc::clone(&a);
                join_set.spawn(async move {
                    println!("Running test_check_and_decrease for user {}", i + 1);
                    offer_test::offer_test::test_check_and_decrease(&client_clone, token_clone, (i+1)as i64).await;
                    format!("User {} completed check_and_decrease", i + 1)
                });
            }
        }
        
        if vec.len() >= 2 {
            let token1 = vec[0].1.clone();
            let token2 = vec[1].1.clone();
            let client_clone = Arc::clone(&a);
            join_set.spawn(async move {
                println!("Running test_take_offer between user 1 and user 2");
                offer_test::offer_test::test_take_offer(&client_clone, token1, token2, 2).await;
                "Take offer completed".to_string()
            });
        }
        
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(msg) => println!("Task completed: {}", msg),
                Err(e) => eprintln!("Task failed: {}", e),
            }
        }
        
        let res = clear_all_tables(&b.clone()).await;
        assert!(res.is_ok());

    }


#[rocket::async_test]
async fn test_auth_login() {
    let (id1, id2) = (1,2);

    let (a,b) = setup_test_client_and_pool().await;
    let (c,d) = setup_test_user_and_token(&a,id1).await;
    user_name("Yoshi".to_string(),&a,d.clone()).await;
    
    let (c,d) = get_test_user_and_token(&a,id1).await;

    let req = a.get("/api/auth/saml_handle")
        .header(Header::new("HTTP_GIVENNAME","Stefano"))
        .header(Header::new("HTTP_SN","Girardi"))
        .header(Header::new("HTTP_MAIL","stefano.girardi.4@studenti.unitn.it"))
        .header(Header::new("HTTP_IDADA","PER2452456"))
        .dispatch()
        .await;
    eprintln!("{:?}",req.into_string().await);
    assert_eq!(1,2);
}

async fn create_pool(database_url: &str) -> PgPool {
    let config = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    Pool::builder()
        .max_size(100)
        .connection_timeout(std::time::Duration::from_secs(30))
        .build(config)
        .await
        .expect("Failed to create pool")
}

pub async fn build_connection () -> (PgPool,ResourceMapOffers,ResourceMapRequests,Arc<WebSocketManager>,Cors) {
    dotenvy::dotenv().ok();
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = create_pool(database_url.as_str()).await;
    let resource_map_request: ResourceMapRequests = dashmap::DashMap::new();
    let resource_map_offer: ResourceMapOffers = dashmap::DashMap::new();
    let web_socket= Arc::new(crate::utils::sse_endpoint::WebSocketManager::new());
    let cors = rocket_cors::CorsOptions::default()
        .to_cors()
        .expect("CORS failed");
    (pool,resource_map_offer,resource_map_request,web_socket,cors)
}

pub fn build_user_1 () -> crate::utils::models::user::User {
    crate::utils::models::user::User::new(
        1,
        "John".to_string(),
        "Doe".to_string(),
        "jd@gmail.com".to_string(),
        "PER1234".to_string()
    )
}

pub fn build_user_2 () -> crate::utils::models::user::User {
    crate::utils::models::user::User::new(
        2,
        "John2".to_string(),
        "Doe2".to_string(),
        "jd2@gmail.com".to_string(),
        "PER1235".to_string()
    )
}

pub fn build_user_n(id: i64) -> crate::utils::models::user::User {
    let name = format!("John{}",id);
    let surname = format!("Doe{}",id);
    let mail = format!("johndoe{}",id);
    let idada = format!("PER{}",id+20000);
    crate::utils::models::user::User::new(
        id,
        name,
        surname,
        mail,
        idada
    )
}


async fn setup_test_client_and_pool() -> (Client, crate::PgPool) {
    let (pool, resource_map_offer, resource_map_request, web_socket, cors) = build_connection().await;
    
    let rocket = rocket::build()
        .manage(pool.clone())
        .manage(resource_map_offer)
        .manage(resource_map_request)
        .manage(web_socket)
        .attach(cors)
        .mount("/api", routes![
                crate::utils::models::user::create_new_user,
                crate::utils::models::user::get_user_on_id,
                // crate::utils::models::user::get_all_users, 
                // crate::utils::models::user::get_all_request,
                crate::utils::models::user::get_req_on_id,
                crate::utils::models::user::resign_driver,
                crate::utils::models::user::delete_user,
                crate::utils::models::user::new_starred_route,
                crate::utils::models::user::patch_route,
                crate::utils::models::user::modify_user_car,
                crate::utils::models::user::auth_login,
                crate::utils::models::user::get_username,
                crate::utils::models::user::get_telegram_username,
                crate::utils::models::user::get_user_full_name,
                crate::utils::models::request::create_request,
                crate::utils::models::request::assign_driver,
                crate::utils::models::request::get_all_request,
                crate::utils::models::request::get_all_my_request,
                crate::utils::models::request::get_all_my_request_to_take,
                crate::utils::models::request::modify_request_time,
                crate::utils::models::request::modify_start,
                crate::utils::models::request::modify_arrival,
                crate::utils::models::request::delete_request,
                crate::utils::models::request::take_request,
                crate::utils::models::request::get_request_by_id,
                crate::utils::models::request::resign_driver,
                crate::utils::models::request::renounce_driver,
                crate::utils::models::offer::create_offer,
                crate::utils::models::offer::get_all_offers,
                crate::utils::models::offer::get_all_my_offers,
                crate::utils::models::offer::get_all_offers_to_take,
                crate::utils::models::offer::take_offer,
                crate::utils::models::offer::my_offers,
                crate::utils::models::offer::modify_offer_time,
                crate::utils::models::offer::modify_route,
                crate::utils::models::offer::get_offer_by_id,
                crate::utils::models::offer::check_and_decrease,
                crate::utils::models::offer::increase_seat,
                crate::utils::models::offer::renounce_seat,
                crate::utils::models::offer::delete_offer,
                crate::utils::sse_endpoint::offers_sse,
                crate::utils::sse_endpoint::requests_sse,
                // crate::utils::telegram_serv::initiate_telegram_chat,
                crate::utils::models::ride_history::get_all_request_history,
                crate::utils::models::ride_history::get_all_offers_history,
        ]);

    let client = Client::tracked(rocket)
        .await
        .expect("Valid Rocket instance");

    (client, pool)
}

async fn setup_test_user_and_token(client: &Client, id: i64) -> (crate::utils::models::user::User, crate::utils::jwt_management::Tokens) {
    let mut user = build_user_n(id);

    let test_userreq = crate::utils::models::user::CreateUserRequest::new(
        id, 
        user.get_name(),
        user.get_surname(),
        user.get_mail(),
        format!("PER{}",id+20000),
    );

    let create_response = client.post("/api/users")
        .header(ContentType::JSON)
        .body(rocket::serde::json::serde_json::to_string(&test_userreq).expect("Failed to serialize user request"))
        .dispatch()
        .await;

    assert_eq!(create_response.status(), rocket::http::Status::Created, "User creation failed");
    drop(create_response);
    
    let get_user_response = client.get(format!("/api/users/{}", id))
        .header(ContentType::JSON)
        .dispatch()
        .await;
    assert_eq!(get_user_response.status(), rocket::http::Status::Ok, "User retrieval failed");
    let body = get_user_response.into_string().await.expect("Failed to get response body");
    

    
    let (user_get, tokens): (crate::utils::models::user::User, crate::utils::jwt_management::Tokens) = 
        rocket::serde::json::serde_json::from_str(&body).expect("Failed to parse user and tokens from response");

    (user_get, tokens)
}

async fn get_test_user_and_token(client: &Client, id: i64) -> (crate::utils::models::user::User, crate::utils::jwt_management::Tokens) {
    let get_user_response = client.get(format!("/api/users/{}", id))
        .header(ContentType::JSON)
        .dispatch()
        .await;
    
    assert_eq!(get_user_response.status(), rocket::http::Status::Ok, "User retrieval failed");
    let body = get_user_response.into_string().await.expect("Failed to get response body");
    

    
    let (user_get, tokens): (crate::utils::models::user::User, crate::utils::jwt_management::Tokens) = 
        rocket::serde::json::serde_json::from_str(&body).expect("Failed to parse user and tokens from response");

    (user_get, tokens)
}


pub async fn clear_all_tables(pool: &crate::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = pool.get().await.expect("Failed to get DB connection");
    
    conn.transaction::<_, diesel::result::Error, _>(|conn| Box::pin(async move {
        
        diesel::delete(crate::schema::ride_history::table)
            .execute(conn)
            .await?;
        
        diesel::delete(crate::schema::offers::table)
            .execute(conn)
            .await?;
        
        diesel::delete(crate::schema::requests::table)
            .execute(conn)
            .await?;
        
        diesel::delete(crate::schema::users::table)
            .execute(conn)
            .await?;
        
        Ok(())
    })).await?;
    
    println!("All tables cleared successfully");
    Ok(())
}

pub async fn user_name(name: String,client: &Client, tokens: Tokens) {
    let response = client.patch(format!("/api/patch_username/{}",name))
        .header(ContentType::JSON)
        .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
        .dispatch()
        .await;

    assert_eq!(response.status(), Status::Ok);
}