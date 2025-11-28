#[cfg(test)]
pub mod request_test {
    use chrono::{NaiveDate, Utc};
    use rocket::{http::{Status,Header,ContentType},local::asynchronous::Client};

    use crate::{PgPool,test::{clear_all_tables, setup_test_client_and_pool, setup_test_user_and_token},
                utils::models::{auto::Auto, request::{CreateRequest,ModifyTimeReq,ModifyStopReq,RequestGetter,TakeReq}, 
                user::User, routing::{Place, Route}}};
    use crate::utils::jwt_management::Tokens;
    use super::*;
    use diesel::prelude::*;
    use diesel_async::{RunQueryDsl};
    use diesel_async::methods::{ExecuteDsl,LoadQuery};

    pub async fn test_create_request(a: &Client, d: Tokens, id: i64){
        let crr = CreateRequest::new(id, Place::new(46.1020, 11.003), Place::new(46.1021, 11.003), Utc::now());

        let response = a.post("/api/requests")
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", d.access_token)))
            .body(rocket::serde::json::serde_json::to_string(&crr).expect(""))
            .dispatch()
            .await;

        assert_eq!(response.status(),Status::Created);
    }

    pub async fn test_get_all_requests(client: &Client, tokens: Tokens, user: User, id: i64) {
        let response = client.get(format!("/api/get_all_requests/{}", id))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().await.expect("Failed to get response body");
        let requests: Vec<RequestGetter> = rocket::serde::json::serde_json::from_str(&body).expect("Failed to parse requests");
        // assert!(requests.iter().all(|request| request.passenger_id != user.get_id()));
    }


    pub async fn test_take_request(client: &Client, driver_tokens: Tokens, pass_tokens: Tokens, id: i64) {

        let requests = client.get(format!("/api/get_all_requests/{}",id))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", pass_tokens.access_token)))
            .dispatch()
            .await;

        let body = requests.into_string().await.expect("Failed to get response body");
        let request: Vec<RequestGetter> = rocket::serde::json::serde_json::from_str(&body).expect("Failed to parse request");
        let session_id = request[0].session_id;

        let take_req = TakeReq::new(
            session_id,
            Place::new(45.4645, 9.1905),
            Place::new(45.4655, 9.1915),
            Route::new(vec![
                Place::new(45.4645, 9.1905),
                Place::new(45.4655, 9.1915),
            ])
        );

        let response = client.patch("/api/take_request")
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", driver_tokens.access_token)))
            .body(rocket::serde::json::serde_json::to_string(&take_req).expect(""))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

    }

    pub async fn test_all_my_requests(client: &Client, tokens: Tokens, user: User, id: i64) {
        let response = client.get(format!("/api/all_my_requests/{}",id))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().await.expect("Failed to get response body");
        let requests: Vec<RequestGetter> = rocket::serde::json::serde_json::from_str(&body).expect("Failed to parse requests");
        // assert!(requests.iter().all(|request| request.passenger_id == user.get_id()));
    }

    pub async fn test_all_requests_to_take(client: &Client, tokens: Tokens, user: User, id: i64) {
        let response = client.get(format!("/api/all_requests_to_take/{}",id))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().await.expect("Failed to get response body");
        let requests: Vec<RequestGetter> = rocket::serde::json::serde_json::from_str(&body).expect("Failed to parse requests");
        // assert!(requests.iter().all(|request| request.passenger_id != user.get_id()));
    }

    pub async fn test_modify_time(client: &Client, tokens: Tokens, id: i64) {
        let requests = client.get(format!("/api/all_my_requests/{}",id))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .dispatch()
            .await;

        let body = requests.into_string().await.expect("Failed to get response body");
        let request: Vec<RequestGetter> = rocket::serde::json::serde_json::from_str(&body).expect("Failed to parse request");
        let session_id = request[0].session_id;
        let body = ModifyTimeReq::new(session_id,id,Utc::now());
        let response = client.patch(format!("/api/modify_request_time"))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .body(rocket::serde::json::serde_json::to_string(&body).expect("Failed"))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);
    }

    pub async fn test_modify_start(client: &Client, tokens: Tokens, id: i64) {
        let requests = client.get(format!("/api/all_my_requests/{}",id))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .dispatch()
            .await;

        let body = requests.into_string().await.expect("Failed to get response body");
        let request: Vec<RequestGetter> = rocket::serde::json::serde_json::from_str(&body).expect("Failed to parse request");
        let session_id = request[0].session_id;
        let body = ModifyStopReq::new(session_id, id, Place::new(46.0340,11.2022));
        let response = client.patch(format!("/api/modify_request_start"))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .body(rocket::serde::json::serde_json::to_string(&body).expect("Failed"))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);
    }

    pub async fn test_modify_arrival(client: &Client, tokens: Tokens, id: i64) {
        let requests = client.get(format!("/api/all_my_requests/{}",id))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .dispatch()
            .await;

        let body = requests.into_string().await.expect("Failed to get response body");
        let request: Vec<RequestGetter> = rocket::serde::json::serde_json::from_str(&body).expect("Failed to parse request");
        let session_id = request[0].session_id;
        let body = ModifyStopReq::new(session_id, id, Place::new(46.0340,11.2022));
        let response = client.patch(format!("/api/modify_request_arrival"))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .body(rocket::serde::json::serde_json::to_string(&body).expect("Failed"))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);
    }

    pub async fn test_delete_request(client: &Client, tokens: Tokens, id: i64) {
        let requests = client.get(format!("/api/all_my_requests/{}",id))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .dispatch()
            .await;

        let body = requests.into_string().await.expect("Failed to get response body");
        let request: Vec<RequestGetter> = rocket::serde::json::serde_json::from_str(&body).expect("Failed to parse request");
        let session_id = request[0].session_id;
        let response = client.delete(format!("/api/delete_request/{}",session_id))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);
    }

}