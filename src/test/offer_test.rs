#[cfg(test)]
pub mod offer_test {


    use chrono::{NaiveDate, Utc};
    use rocket::{http::{Status,Header,ContentType},local::asynchronous::Client};

    use crate::{PgPool,test::{clear_all_tables, setup_test_client_and_pool, setup_test_user_and_token}, utils::models::{auto::Auto, offer::{CreateOfferRequest, OfferGetter, TakeOfferReq,ModifyRouteReq,ModifyTimeReq}, user::User, routing::{Place, Route}}};
    use crate::utils::jwt_management::Tokens;
    use super::*;
    use diesel::prelude::*;
    use diesel_async::{RunQueryDsl};
    use diesel_async::methods::{ExecuteDsl,LoadQuery};


    pub async fn test_create_offer(a: &Client, d: Tokens, id: i64){
        let cor = CreateOfferRequest::new(id, Place::new(46.1020, 11.003), Place::new(46.1021, 11.003), Utc::now(), Utc::now(), Route::new(Vec::new()), None, 4);

        let response = a.post("/api/offers")
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", d.access_token)))
            .body(rocket::serde::json::serde_json::to_string(&cor).expect(""))
            .dispatch()
            .await;

        assert_eq!(response.status(),Status::Created);
    }

    pub async fn test_get_all_offers(client: &Client, tokens: Tokens, user: User, id: i64) {
        let response = client.get(format!("/api/all_offers/{}", id))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().await.expect("Failed to get response body");
        let offers: Vec<OfferGetter> = rocket::serde::json::serde_json::from_str(&body).expect("Failed to parse offers");
        // assert!(offers.iter().all(|offer| offer.driver_name != user.get_username()));
    }


    pub async fn test_get_offer_by_id(client: &Client, pool: &PgPool, tokens: Tokens, id: i64) {    
        let mut conn = pool.get().await.expect("Failed to get DB connection");
        let offers = crate::schema::offers::table
            .filter(crate::schema::offers::driver_id.eq(id))
            .select(crate::schema::offers::session_id) 
            .load::<i64>(&mut conn) 
            .await
            .expect("Failed to load session_ids");
    
        let session_id = offers[0];
    
        let response = client.get(format!("/api/get_offer/{}", session_id))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .dispatch()
            .await;
    
        assert_eq!(response.status(), Status::Ok);
    
        let body = response.into_string().await.expect("Failed to get response body");
        let offer: OfferGetter = rocket::serde::json::serde_json::from_str(&body).expect("Failed to parse offer");
    
        assert_eq!(offer.session_id, session_id);
    }

    pub async fn test_take_offer(client: &Client, driver_tokens: Tokens, pass_tokens: Tokens, id: i64) {

        let offers = client.get(format!("/api/all_offers/{}",id))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", pass_tokens.access_token)))
            .dispatch()
            .await;

        let body = offers.into_string().await.expect("Failed to get response body");
        let offer: Vec<OfferGetter> = rocket::serde::json::serde_json::from_str(&body).expect("Failed to parse offer");
        let session_id = offer[0].session_id;

        let take_req = TakeOfferReq::new(
            session_id,
            id,
            Place::new(45.4645, 9.1905),
            Place::new(45.4655, 9.1915),
            1,
        );

        let response = client.patch("/api/take_offer")
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", pass_tokens.access_token)))
            .body(rocket::serde::json::serde_json::to_string(&take_req).expect(""))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

    }

    pub async fn test_my_offers(client: &Client, tokens: Tokens, user: User, id: i64) {
        let response = client.get(format!("/api/my_offers/{}",id))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().await.expect("Failed to get response body");
        let offers: Vec<OfferGetter> = rocket::serde::json::serde_json::from_str(&body).expect("Failed to parse offers");
        println!("{:?}",offers.clone());
        println!("{:?}",user.get_username());
        // assert!(offers.iter().all(|offer| offer.driver_name == user.get_username()));
    }

    pub async fn test_all_my_offers(client: &Client, tokens: Tokens, user: User, id: i64) {
        let response = client.get(format!("/api/all_my_offers/{}",id))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().await.expect("Failed to get response body");
        let offers: Vec<OfferGetter> = rocket::serde::json::serde_json::from_str(&body).expect("Failed to parse offers");
        // assert!(offers.iter().all(|offer| offer.driver_name == user.get_username()));
    }

    pub async fn test_all_offers_to_take(client: &Client, tokens: Tokens, user: User, id: i64) {
        let response = client.get(format!("/api/all_offers_to_take/{}",id))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().await.expect("Failed to get response body");
        let offers: Vec<OfferGetter> = rocket::serde::json::serde_json::from_str(&body).expect("Failed to parse offers");
        // assert!(offers.iter().all(|offer| offer.driver_name != user.get_username()));
    }

    pub async fn test_increase(client: &Client, tokens: Tokens, id: i64) {
        let offers = client.get(format!("/api/all_offers/{}",id))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .dispatch()
            .await;

        let body = offers.into_string().await.expect("Failed to get response body");
        let offer: Vec<OfferGetter> = rocket::serde::json::serde_json::from_str(&body).expect("Failed to parse offer");
        let session_id = offer[0].session_id;

        let response = client.patch(format!("/api/increase_seat/{}",session_id))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);
    }


    pub async fn test_check_and_decrease(client: &Client, tokens: Tokens, id: i64) {
        let offers = client.get(format!("/api/all_offers/{}",id))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .dispatch()
            .await;

        let body = offers.into_string().await.expect("Failed to get response body");
        let offer: Vec<OfferGetter> = rocket::serde::json::serde_json::from_str(&body).expect("Failed to parse offer");
        let session_id = offer[0].session_id;

        let response = client.patch(format!("/api/check_and_decrease/{}",session_id))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);
    }

    pub async fn test_renounce(client: &Client, tokens: Tokens, id: i64) {
        let offers = client.get(format!("/api/all_offers_to_take/{}",id))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .dispatch()
            .await;

        let body = offers.into_string().await.expect("Failed to get response body");
        let offer: Vec<OfferGetter> = rocket::serde::json::serde_json::from_str(&body).expect("Failed to parse offer");
        let session_id = offer[0].session_id;

        let response = client.patch(format!("/api/renounce_seat/{}",session_id))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);
    }

    pub async fn test_modify_time(client: &Client, tokens: Tokens, id: i64) {
        let offers = client.get(format!("/api/all_my_offers/{}",id))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .dispatch()
            .await;

        let body = offers.into_string().await.expect("Failed to get response body");
        let offer: Vec<OfferGetter> = rocket::serde::json::serde_json::from_str(&body).expect("Failed to parse offer");
        let session_id = offer[0].session_id;
        let body = ModifyTimeReq::new(session_id,id,Utc::now());
        let response = client.patch(format!("/api/modify_offer_time"))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .body(rocket::serde::json::serde_json::to_string(&body).expect("Failed"))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);
    }

    pub async fn test_modify_route(client: &Client, tokens: Tokens, id: i64) {
        let offers = client.get(format!("/api/all_my_offers/{}",id))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .dispatch()
            .await;

        let body = offers.into_string().await.expect("Failed to get response body");
        let offer: Vec<OfferGetter> = rocket::serde::json::serde_json::from_str(&body).expect("Failed to parse offer");
        let session_id = offer[0].session_id;
        let body = ModifyRouteReq::new(session_id, id, Route::new(vec![Place::new(46.013010,11.0130),Place::new(46.013010,11.0130),Place::new(46.013010,11.0130),Place::new(46.013010,11.0132),Place::new(46.013010,11.014),Place::new(46.0132,11.014)]));
        let response = client.patch(format!("/api/modify_offer_route"))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .body(rocket::serde::json::serde_json::to_string(&body).expect("Failed"))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);
    }

    pub async fn test_delete_offer(client: &Client, tokens: Tokens, id: i64) {
        let offers = client.get(format!("/api/all_my_offers/{}",id))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .dispatch()
            .await;

        let body = offers.into_string().await.expect("Failed to get response body");
        let offer: Vec<OfferGetter> = rocket::serde::json::serde_json::from_str(&body).expect("Failed to parse offer");
        let session_id = offer[0].session_id;
        let response = client.delete(format!("/api/delete_offer/{}",session_id))
            .header(ContentType::JSON)
            .header(Header::new("Authorization", format!("Bearer {}", tokens.access_token)))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);
    }
}