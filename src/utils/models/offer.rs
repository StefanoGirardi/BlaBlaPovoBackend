use std::{collections::HashMap, sync::Arc};
use chrono::{DateTime,Utc};
use diesel::{prelude::*};
use diesel::serialize::{ToSql,Output,IsNull};
use diesel::deserialize::{FromSql, Queryable};
use diesel::pg::{Pg, PgValue};
use rand::{random, Rng};
use rocket::{data::ToByteUnit, futures::future::join_all, delete, get, http::Status, patch, post, response::status::Custom, serde::json::{self, Json}, Data, State};
use serde::{Deserialize,Serialize};
use tokio::sync::Mutex;
use crate::utils::sse_endpoint::{OfferBroadcaster, WebSocketManager, BroadcastResource};
use std::io::Write;
use super::{routing::*,auto::*};
use super::super::super::{ResourceMapOffers,schema::{offers,users},PgPool};
use diesel_async::RunQueryDsl;
use diesel_async::methods::*;
use crate::utils::jwt_management::Claims;

#[derive(Debug,Deserialize,Serialize,Clone,Selectable,Insertable)]
#[diesel(table_name = offers)]
pub struct Offer {
    session_id: i64,
    driver_id: i64,
    passengers_id: Vec<i64>,
    start: Place,
    arrival: Place,
    start_time: DateTime<Utc>,
    arrival_time: DateTime<Utc>,
    route: Route,
    auto: Option<Auto>,
    seats_available: i16,
    stops: Vec<Stop>,
}

impl Queryable<offers::SqlType, diesel::pg::Pg> for Offer {
    type Row = (
        i64, i64, 
        Vec<Option<i64>>,      
        Place, Place,
        chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>,
        Route,
        Option<Auto>,
        i16,
        Vec<Option<Stop>>,     
    );
    
    fn build(row: Self::Row) -> diesel::deserialize::Result<Self> {
        let passengers_id: Vec<i64> = row.2.into_iter().flatten().collect();
        let stops: Vec<Stop> = row.10.into_iter().flatten().collect();
        Ok(
            Self {
                session_id: row.0,
                driver_id: row.1,
                passengers_id,
                start: row.3,
                arrival: row.4,
                start_time: row.5,
                arrival_time: row.6,
                route: row.7,
                auto: row.8,
                seats_available: row.9,
                stops,
            }
        )
    }
}

impl Offer {
    fn new_offer(session_id: i64, driver_id: i64, passengers_id: Vec<i64>, start: Place, arrival: Place, route: Route, start_time: DateTime<Utc>, arrival_time: DateTime<Utc>, seats_available: i16, stops: Vec<Stop>,auto: Option<Auto>)->Self {
        Self { session_id, 
            driver_id, 
            passengers_id, 
            start, 
            arrival, 
            start_time, 
            arrival_time, 
            route, 
            auto, 
            seats_available, 
            stops 
        }
    }

    pub fn session_id(&self) -> i64 {
        self.session_id
    }

    pub fn driver_id(&self) -> i64 {
        self.driver_id
    }

    pub fn passenger_id(&self) -> &Vec<i64> {
        &self.passengers_id
    }

    pub fn start(&self) -> &Place {
        &self.start
    }

    pub fn arrival(&self) -> &Place {
        &self.arrival
    }

    pub fn start_time(&self) -> DateTime<Utc> {
        self.start_time
    }

    pub fn arrival_time(&self) -> DateTime<Utc> {
        self.arrival_time
    }

    pub fn route(&self) -> &Route {
        &self.route
    }

    pub fn auto(&self) -> &Option<Auto> {
        &self.auto
    }

    pub fn seats_available(&self) -> i16 {
        self.seats_available
    }

    pub fn stops(&self) -> &Vec<Stop> {
        &self.stops
    }

    pub fn passenger_id_mut(&mut self) -> &mut Vec<i64> {
        &mut self.passengers_id
    }

    pub fn start_mut(&mut self) -> &mut Place {
        &mut self.start
    }

    pub fn arrival_mut(&mut self) -> &mut Place {
        &mut self.arrival
    }

    pub fn route_mut(&mut self) -> &mut Route {
        &mut self.route
    }

    pub fn auto_mut(&mut self) -> &mut Option<Auto> {
        &mut self.auto
    }

    pub fn stops_mut(&mut self) -> &mut Vec<Stop> {
        &mut self.stops
    }

    pub fn set_seats_available(&mut self, seats: i16) {
        self.seats_available = seats;
    }
}

#[derive(Debug,Serialize,Deserialize)]
pub struct CreateOfferRequest {
    driver_id: i64,
    start: Place,
    arrival: Place,
    start_time: DateTime<Utc>,
    arrival_time: DateTime<Utc>,
    route: Route,
    auto: Option<Auto>,
    seats_available: i16,
}

impl CreateOfferRequest {
    pub fn new(
        driver_id: i64,
        start: Place,
        arrival: Place,
        start_time: DateTime<Utc>,
        arrival_time: DateTime<Utc>,
        route: Route,
        auto: Option<Auto>,
        seats_available: i16,
    ) -> Self {
        Self {
            driver_id,
            start,
            arrival,
            start_time,
            arrival_time,
            route,
            auto,
            seats_available,
        }
    }
}

#[post("/offers", format = "application/json", data="<data>")]
pub async fn create_offer(data: Data<'_>, claims: Claims, db: &State<PgPool>, broadcaster: &State<Arc<WebSocketManager>>)->Result<Status,Status> {
    let limits = data.open(100.mebibytes());
    let bytes = limits.into_bytes().await.expect("Failed to get JSON");
    
    let offer_req: CreateOfferRequest = json::from_slice(&bytes).expect("Failed to parse JSON");
    println!("{:?}",offer_req);
    
    let mut session_id: i64 = rand::rng().random_range(1..i64::MAX);
    let passengers_id: Vec<i64> = Vec::new();
    let stops: Vec<Stop> = Vec::new();
    
    let id = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);
    
    if let Ok(driver_id) = id {

        let mut conn = db.inner().get().await.expect("Failed to connect to DB");
    
        let check_db = offers::table
            .filter(offers::session_id.eq(session_id))
            .select(offers::session_id)
            .first::<i64>(&mut conn)
            .await
            .optional()
            .expect("DB error");
        
        while let Some(_) = offers::table
            .filter(offers::session_id.eq(session_id))
            .select(offers::session_id)
            .first::<i64>(&mut conn)
            .await
            .optional()
            .expect("DB error")  
        {
            session_id = rand::rng().random_range(1..i64::MAX);
        }
    
        let offer = Offer::new_offer(
            session_id, driver_id, passengers_id,
            offer_req.start, offer_req.arrival, offer_req.route,
            offer_req.start_time, offer_req.arrival_time,
            offer_req.seats_available, stops, offer_req.auto
        );
    
        let response = diesel::insert_into(offers::table)
            .values(offer)
            .execute(&mut conn)
            .await
            .expect("Failed to insert new offer");
        
        if response == 1 {
            let broadcast = BroadcastResource::Created(session_id);
            if let Err(e) = broadcaster.broadcast_offer(broadcast) {
                eprintln!("Failed to broadcast request: {}", e);
            }
            Ok(Status::Created)
        } else {
            Err(Status::BadRequest)
        }
    } else {
        Err(Status::Unauthorized)
    }
}


#[derive(Debug,Serialize,Deserialize,Clone)]
pub struct OfferGetter {
    pub session_id: i64,
    pub driver_id: i64,
    pub start: Place,
    pub arrival: Place,
    pub route: Route,
    pub stops: Vec<Stop>,
    pub start_time: DateTime<Utc>,
    pub seats_available: i16,
    pub auto: Option<Auto>,
}

#[get("/all_offers/<id>", format = "application/json")]
pub async fn get_all_offers(id: i64, db: &State<PgPool>, claims: Claims) -> Result<Json<Vec<OfferGetter>>,Status> {
    let mut conn = db.get().await.expect("Failed to get DB connection");
    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(id)=auth {
        let offers_res = offers::table
            .filter(offers::driver_id.ne(id))
            .load::<Offer>(&mut conn)
            .await;
        
        let mut offers = Vec::new();
        match offers_res {
            Ok(res) => offers = res,
            Err(e) => {
                println!("Error fetching offers: {:?}", e);
            }
        }
        
        let result: Vec<OfferGetter> = offers.into_iter()
            .map(|offer| {
                OfferGetter {
                    session_id: offer.session_id,
                    driver_id: offer.driver_id,  
                    start: offer.start,
                    arrival: offer.arrival,
                    route: offer.route,
                    stops: offer.stops,
                    start_time: offer.start_time,
                    seats_available: offer.seats_available,
                    auto: offer.auto
                }
            })
            .collect();
        println!("{:?}",result);
        Ok(Json(result))        
    } else {
        Err(Status::Unauthorized)
    }

}

pub async fn get_driver_name(id: i64, db: &State<PgPool>) -> Result<String, ()> {
    let mut conn = db.get().await.expect("Failed to connect to DB");

    let name = users::table
        .filter(users::id.eq(id))
        .select(users::username)
        .first::<String>(&mut conn)
        .await
        .optional()
        .expect("Failed to get username");

    name.ok_or(()) 
}


#[get("/get_offer/<id>",format = "application/json")]
pub async fn get_offer_by_id(id: i64, db: &State<PgPool>, claims: Claims)->Result<Json<OfferGetter>,Status> {
    
    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(_) = auth {

        let mut conn = db.get().await.expect("Failed to connect to DB");
        
        let res = offers::table
            .find(id)
            .first::<Offer>(&mut conn)
            .await
            .optional()
            .expect("Failed to get specific Offer");
    
        
    
        if let Some(offer)=res {
            Ok(
                Json(
                    OfferGetter {
                        session_id: offer.session_id,
                        driver_id: offer.driver_id,  
                        start: offer.start,
                        arrival: offer.arrival,
                        route: offer.route,
                        stops: offer.stops,
                        start_time: offer.start_time,
                        seats_available: offer.seats_available,
                        auto: offer.auto
                    }
                )
            )
        } else {
            Err(Status::NotFound)
        }
    } else {
        Err(Status::Unauthorized)
    }
    
}

#[derive(Debug,Serialize,Deserialize,Clone)]
pub struct TakeOfferReq {
    session_id: i64,
    passenger_id: i64,
    pickup_spot: Place,
    dismount_spot: Place,
    n_seat_req: i16,
}

impl TakeOfferReq {
    pub fn new(
        session_id: i64,
        passenger_id: i64,
        pickup_spot: Place,
        dismount_spot: Place,
        n_seat_req: i16,
    ) -> Self {
        Self { session_id, passenger_id, pickup_spot, dismount_spot, n_seat_req }
    }
}

#[patch("/take_offer", format = "application/json", data = "<req>")]
pub async fn take_offer(
    req: Json<TakeOfferReq>, 
    db: &State<PgPool>, 
    resource_map: &State<ResourceMapOffers>,
    broadcaster: &State<Arc<WebSocketManager>>,
    claims: Claims
) -> Result<Status, Status> {
    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(id)  = auth {
        let mut conn = db.get().await.expect("Failed to connect to DB");
    
        if let Some(offer) = resource_map.get(&req.session_id) {
            take_offer_helper(&offer, req, broadcaster, db).await
        } else {
            let _offer = offers::table
                .filter(offers::session_id.eq(req.session_id))
                .first::<Offer>(&mut conn)
                .await
                .expect("FAiled to get offer");
            println!("{:?}",_offer);
            let session_id = _offer.session_id;
            let guard = Arc::new(tokio::sync::Mutex::new(_offer));
            
            
            resource_map.insert(session_id, guard.clone());
            
            take_offer_helper(&guard, req, broadcaster, db).await
        }
    } else {
        Err(Status::Unauthorized)
    }
}

async fn take_offer_helper(
    offer: &Arc<Mutex<Offer>>, 
    req: Json<TakeOfferReq>,
    broadcaster: &State<Arc<WebSocketManager>>,
    db: &State<PgPool>
) -> Result<Status, Status> {
    let update_data = {
        let offer_guard = offer.lock().await;
        
        if offer_guard.passengers_id.contains(&req.passenger_id) {
            return Err(Status::BadRequest);
        }
        
        let mut new_passenger_id = offer_guard.passengers_id.clone();
        new_passenger_id.push(req.passenger_id);
        
        let mut new_stops = offer_guard.stops.clone();
        new_stops.push(Stop::new(req.passenger_id, req.pickup_spot.clone()));
        new_stops.push(Stop::new(req.passenger_id, req.dismount_spot.clone()));
        
        
        (new_passenger_id, new_stops, offer_guard.session_id)
    };
    
    let (new_passenger_id, new_stops, session_id) = update_data;
    let mut conn = db.get().await.expect("Failed to connect to DB");
    let result = diesel::update(offers::table.find(session_id))
        .set((
            offers::passengers_id.eq(&new_passenger_id),
            offers::stops.eq(&new_stops),
        ))
        .execute(&mut conn)
        .await;

    match result {
        Ok(1) => {
            if let Ok(mut offer_guard) = offer.try_lock() {
                offer_guard.passengers_id = new_passenger_id.clone();
                offer_guard.stops = new_stops.clone();
            }
            let broadcast = BroadcastResource::Modified(req.session_id);
            if let Err(e) = broadcaster.broadcast_offer(broadcast) {
                eprintln!("Failed to broadcast request: {}", e);
            }
            Ok(Status::Ok)
        },
        Ok(0) => {
            eprintln!("No rows affected - session_id not found: {}", session_id);
            Err(Status::BadRequest)
        },
        Ok(_) => {
            eprintln!("Unexpected number of rows affected");
            Err(Status::InternalServerError)
        },
        Err(e) => {
            eprintln!("Database error: {}", e);
            Err(Status::InternalServerError)
        }
    }
}

#[get("/my_offers/<id>", format = "application/json")]
pub async fn my_offers(id: i64, db: &State<PgPool>,claims: Claims) -> Result<Json<Vec<OfferGetter>>,Status> {

    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(id) = auth {

        let mut conn = db.get().await.expect("Failed to connect to DB");
    
        let offers_res = offers::table
            .filter(offers::driver_id.eq(id))
            .load::<Offer>(&mut conn)
            .await
            .optional()
            .expect("Failed to get Offer");
    
        let mut  offers = Vec::new();
        if let Some(res) = offers_res {
            offers = res;
        }else {
            println!("{:?}",offers_res);
        }
        let result: Vec<OfferGetter> = offers.into_iter()
            .map(|offer|  {
                OfferGetter {
                    session_id: offer.session_id,
                    driver_id: offer.driver_id,
                    start: offer.start,
                    arrival: offer.arrival,
                    route: offer.route,
                    stops: offer.stops,
                    start_time: offer.start_time,
                    seats_available: offer.seats_available,
                    auto: offer.auto
                }
            })
            .collect();
        Ok(Json(result))        
    } else {
        Err(Status::Unauthorized)
    }

}

#[get("/all_my_offers/<id>", format = "application/json")]
pub async fn get_all_my_offers(id: i64, db: &State<PgPool>, claims: Claims) -> Result<Json<Vec<OfferGetter>>,Status> {

    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(id) = auth {
        let mut conn = db.get().await.expect("Failed to connect to DB");
    
        let offers_res = offers::table
            .filter(offers::driver_id.eq(id))
            .load::<Offer>(&mut conn)
            .await
            .optional()
            .expect("Failed to get Offer");
    
        let mut  offers = Vec::new();
        if let Some(res) = offers_res {
            offers = res;
        }else {
            println!("{:?}",offers_res);
        }
        
        let result: Vec<OfferGetter> = offers.into_iter()
            .map(|offer|  {
                OfferGetter {
                    session_id: offer.session_id,
                    driver_id: offer.driver_id,  
                    start: offer.start,
                    arrival: offer.arrival,
                    route: offer.route,
                    stops: offer.stops,
                    start_time: offer.start_time,
                    seats_available: offer.seats_available,
                    auto: offer.auto
                }
            })
            .collect();
        Ok(Json(result)) 
    } else {
        Err(Status::Unauthorized)
    }
}

#[get("/all_offers_to_take/<id>", format = "application/json")]
pub async fn get_all_offers_to_take(id: i64, db: &State<PgPool>, claims: Claims) -> Result<Json<Vec<OfferGetter>>,Status> {

    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(id) = auth {

        let mut conn = db.get().await.expect("Failed to get DB connection");
    
        let offers_res = offers::table
            .filter(offers::passengers_id.contains(vec![id]))  
            .load::<Offer>(&mut conn)
            .await;
    
        let mut offers = Vec::new();
        match offers_res {
            Ok(res) => offers = res,
            Err(e) => {
                println!("Error fetching offers: {:?}", e);
            }
        }
    
        let result: Vec<OfferGetter> = offers.into_iter()
            .map(|offer| {
                OfferGetter {
                    session_id: offer.session_id,
                    driver_id: offer.driver_id,  
                    start: offer.start,
                    arrival: offer.arrival,
                    route: offer.route,
                    stops: offer.stops,
                    start_time: offer.start_time,
                    seats_available: offer.seats_available,
                    auto: offer.auto
                }
            })
            .collect();
    
        Ok(Json(result))    
    } else {
        Err(Status::Unauthorized)
    }

}

#[patch("/renounce_seat/<session_id>", format="application/json")]
pub async fn renounce_seat(
    session_id: i64, 
    db: &State<PgPool>,
    claims: Claims,
    resource_map: &State<ResourceMapOffers>,
    broadcaster: &State<Arc<WebSocketManager>>,
) -> Result<Status,Status>{
    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(user_id) = auth {
        let mut conn = db.get().await.expect("Failed to connect to DB");
    
        if let Some(mut offer) = resource_map.get(&session_id) {
            let mut guard = offer.lock().await;
            
            // Remove user from passengers list
            let mut vec = guard.passengers_id.clone();
            vec.retain(|&i| i != user_id);
            guard.passengers_id = vec.clone();
            
            // Remove stops where stop.id == user_id
            let mut vec2 = guard.stops.clone();
            vec2.retain(|stop| !stop.get_stop(user_id).is_some());
            guard.stops = vec2.clone();

            let seats = guard.seats_available + 1;
            guard.seats_available = seats;
            drop(guard);

            let result = diesel::update(offers::table.find(session_id))
                .set((
                    offers::passengers_id.eq(vec.clone()),
                    offers::seats_available.eq(seats),
                    offers::stops.eq(vec2.clone()),
                ))
                .execute(&mut conn)
                .await;
            let broadcast = BroadcastResource::Modified(session_id);
            if let Err(e) = broadcaster.broadcast_offer(broadcast) {
                eprintln!("Failed to broadcast request: {}", e);
            }
            Ok(Status::Ok)
        } else {
            let _offer = offers::table
                .filter(offers::session_id.eq(session_id))
                .first::<Offer>(&mut conn)
                .await
                .expect("Failed to get offer");
            
            let session_id = _offer.session_id;
            
            let guard = Arc::new(tokio::sync::Mutex::new(_offer));
            
            resource_map.insert(session_id, guard.clone());
            
            if let Some(mut offer) = resource_map.get(&session_id) {
                let mut guard = offer.lock().await;
                
                // Remove user from passengers list
                let mut vec = guard.passengers_id.clone();
                vec.retain(|&i| i != user_id);
                guard.passengers_id = vec.clone();

                // Remove stops where stop.id == user_id
                let mut vec2 = guard.stops.clone();
                vec2.retain(|stop| !stop.get_stop(user_id).is_some());
                guard.stops = vec2.clone();

                let seats = guard.seats_available + 1;
                guard.seats_available = seats;
                drop(guard);

                let result = diesel::update(offers::table.find(session_id))
                    .set((
                        offers::passengers_id.eq(vec.clone()),
                        offers::seats_available.eq(seats),
                        offers::stops.eq(vec2.clone()),
                    ))
                    .execute(&mut conn)
                    .await;
                let broadcast = BroadcastResource::Modified(session_id);
                if let Err(e) = broadcaster.broadcast_offer(broadcast) {
                    eprintln!("Failed to broadcast request: {}", e);
                }
                Ok(Status::Ok)
            } else {
                Err(Status::InternalServerError)
            }
        }
    } else {
        Err(Status::Unauthorized)
    }
}

// To use when user deletes account to delete him from the offer if it has offers active as a passenger
pub async fn remove_passenger_id(
    id: i64, 
    db: &State<PgPool>
) -> Result<usize, diesel::result::Error> {
    let mut conn = db.get().await.expect("Failed to connect to DB");

    
    let mut offers = offers::table
        .filter(offers::passengers_id.contains(vec![id]))
        .load::<Offer>(&mut conn)
        .await?;

    let mut total_updated = 0;

    for offer in &mut offers {
        
        offer.passengers_id.retain(|&passenger_id| passenger_id != id);
        
       
        let updated = diesel::update(
            offers::table.filter(offers::session_id.eq(offer.session_id))
        )
        .set(offers::passengers_id.eq(&offer.passengers_id))
        .execute(&mut conn)
        .await?;
        
        total_updated += updated;
    }

    Ok(total_updated)
}


#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct ModifyTimeReq {
    session_id: i64,
    driver_id: i64,
    start_time: DateTime<Utc>,
}

impl ModifyTimeReq {
    pub fn new(
        session_id: i64,
        driver_id: i64,
        start_time: DateTime<Utc>
    ) -> Self {
        Self { session_id, driver_id, start_time }
    }
}

#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct ModifyRouteReq {
    session_id: i64,
    driver_id: i64,
    route: Route,
}

impl ModifyRouteReq {
    pub fn new(
        session_id: i64,
        driver_id: i64,
        route: Route
    ) -> Self {
        Self { session_id, driver_id, route }
    }
}

#[patch("/modify_offer_time", format="application/json", data="<req>")]
pub async fn modify_offer_time(
    req: Json<ModifyTimeReq>, 
    resource_map: &State<ResourceMapOffers>, 
    broadcaster: &State<Arc<WebSocketManager>>, 
    claims: Claims,
    db: &State<PgPool>
) -> Result<Status, Custom<String>> {

    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(id) = auth {
        let mut conn = db.get().await.expect("FAiled to connect to DB");
        match resource_map.get(&req.session_id) {
            Some(offer) => {
                {
                    let mut offer_guard = offer.lock().await;
                    offer_guard.start_time = req.start_time;
                }
            }
            None => {
                let offer = offers::table
                    .filter(offers::session_id.eq(req.session_id))
                    .first::<Offer>(&mut conn)
                    .await
                    .expect("Failed to get Offer");
                let offer_arc = Arc::new(tokio::sync::Mutex::new(offer));
                resource_map.insert(req.session_id, offer_arc);
            }
        }
    
        let res = diesel::update(
            offers::table
                .filter(offers::session_id.eq(req.session_id))
                .filter(offers::driver_id.eq(req.driver_id))
        )
        .set(offers::start_time.eq(req.start_time))
        .execute(&mut conn)
        .await  
        .map_err(|e| {
            eprintln!("Error updating offer: {:?}", e);
            Status::InternalServerError
        });
    
        if res.is_ok() {
            if let Some(offer) = resource_map.get(&req.session_id) {
                let broadcast = BroadcastResource::Modified(req.session_id);
                if let Err(e) = broadcaster.broadcast_offer(broadcast) {
                    eprintln!("Failed to broadcast request: {}", e);
                }
            }
            Ok(Status::Ok)
        } else {
            Err(Custom(Status::BadRequest, "Couldn't update the database".to_string()))
        }

    }else{
        Err(Custom(Status::Unauthorized, format!("User noth authorized")))
    }

}

#[patch("/modify_offer_route", format="application/json", data="<req>")]
pub async fn modify_route(
    req: Json<ModifyRouteReq>, 
    resource_map: &State<ResourceMapOffers>, 
    broadcaster: &State<Arc<WebSocketManager>>, 
    db: &State<PgPool>,
    claims: Claims
) -> Result<Status, Custom<String>> {
    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(id) = auth {
        let mut conn = db.get().await.expect("Failed to connect to DB");
        match resource_map.get(&req.session_id) {
            Some(offer) => {
                {
                    let mut offer_guard = offer.lock().await;
                    offer_guard.route = req.route.clone();
                }
            }
            None => {
                let mut offer = offers::table
                    .find(req.session_id)
                    .first::<Offer>(&mut conn)
                    .await
                    .expect("Failed to get Offer");
                offer.route = req.route.clone();
                let offer_arc = Arc::new(tokio::sync::Mutex::new(offer));
                resource_map.insert(req.session_id, offer_arc);
            }
        }
    
        
        let res = diesel::update(
            offers::table
                .find(req.session_id)
        )
        .set(offers::route.eq(req.route.clone()))
        .execute(&mut conn)
        .await
        .map_err(|e| {
            eprintln!("Error updating offer: {:?}", e);
            Status::InternalServerError
        });
    
        if res.is_ok() {
            if let Some(offer) = resource_map.get(&req.session_id) {
                let broadcast = BroadcastResource::Modified(req.session_id);
                if let Err(e) = broadcaster.broadcast_offer(broadcast) {
                    eprintln!("Failed to broadcast request: {}", e);
                }
            }
            Ok(Status::Ok)
        } else {
            Err(Custom(Status::BadRequest, format!("Couldn't update the database: {:?}", res.err())))
        }

    } else {
        Err(Custom(Status::Unauthorized, format!("User noth authorized")))
    }
    
}

pub async fn decrease_seat(
    offer_guard: Arc<Mutex<Offer>>,
    conn: &mut bb8::PooledConnection<'_, diesel_async::pooled_connection::AsyncDieselConnectionManager<diesel_async::AsyncPgConnection>>,
    broadcaster: &State<Arc<WebSocketManager>>,
 )->Result<Status,Status> {
    if let Ok(mut offer) = offer_guard.try_lock() {
        if offer.seats_available > 0{
            offer.seats_available -= 1;
            let res = diesel::update(
                offers::table
                    .filter(offers::session_id.eq(offer.session_id))
                )
                .set(offers::seats_available.eq(offer.seats_available))
                .execute(conn)
                .await
                .expect("Failed to update DB");
            let broadcast = BroadcastResource::Modified(offer.session_id);
            if let Err(e) = broadcaster.broadcast_offer(broadcast) {
                eprintln!("Failed to broadcast request: {}", e);
            }
            Ok(Status::Ok)
        } else{
            Err(Status::BadRequest)
        }
    } else {
        Err(Status::BadRequest)
    }
}

#[patch("/increase_seat/<id>", format = "application/json")]
pub async fn increase_seat(
    id: i64, 
    resource_map: &State<ResourceMapOffers>,
    db: &State<PgPool>,
    claims: Claims,
    broadcaster: &State<Arc<WebSocketManager>>
) -> Result<Status, Status> {
    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(_) = auth {

        let mut conn = db.get().await.map_err(|_| Status::InternalServerError)?;

        if let Some(offer_arc) = resource_map.get(&id) {
            let mut offer = offer_arc.lock().await;
            offer.seats_available += 1;
            
            diesel::update(
                offers::table
                    .filter(offers::session_id.eq(id))
                )
                .set(offers::seats_available.eq(offer.seats_available))
                .execute(&mut conn)
                .await
                .map_err(|_| Status::InternalServerError)?;
            let broadcast = BroadcastResource::Modified(id);
            if let Err(e) = broadcaster.broadcast_offer(broadcast) {
                eprintln!("Failed to broadcast request: {}", e);
            }    
            return Ok(Status::Ok);
        }
    
        let offer_result = offers::table
            .filter(offers::session_id.eq(id))
            .first::<Offer>(&mut conn)
            .await;
    
        match offer_result {
            Ok(mut offer) => {
                offer.seats_available += 1;
                
                diesel::update(
                    offers::table
                        .filter(offers::session_id.eq(id))
                    )
                    .set(offers::seats_available.eq(offer.seats_available))
                    .execute(&mut conn)
                    .await
                    .map_err(|_| Status::InternalServerError)?;
               
                resource_map.insert(id, Arc::new(Mutex::new(offer)));
                let broadcast = BroadcastResource::Modified(id);
                if let Err(e) = broadcaster.broadcast_offer(broadcast) {
                    eprintln!("Failed to broadcast request: {}", e);
                }
                Ok(Status::Ok)
            }
            Err(_) => Err(Status::NotFound)
        }
    } else {
        Err(Status::Unauthorized)
    }

}

#[patch("/check_and_decrease/<id>")]
pub async fn check_and_decrease(
    id: i64, 
    resource_map: &State<ResourceMapOffers>, 
    db: &State<PgPool>,claims: Claims, 
    broadcaster: &State<Arc<WebSocketManager>>
)->Result<Status,Status> {
    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(_) = auth{
        let mut conn = db.get().await.expect("Fsiled to connect to DB"); 
        if let Some(offer)=resource_map.get(&id) {
            let guard = Arc::clone(offer.value());
            decrease_seat(guard,&mut conn,broadcaster).await
        } else {
            let res = offers::table
                .find(id)
                .first::<Offer>(&mut conn)
                .await
                .optional()
                .expect("Failed to get Offer");
    
            if let Some(offer)=res{
                let guard = Arc::new(Mutex::new(offer));
                resource_map.insert(id,guard);
                if let Some(offer)=resource_map.get(&id) {
                    let guard = Arc::clone(offer.value());
                    decrease_seat(guard, &mut conn,broadcaster).await
                }else {
                    Err(Status::InternalServerError)
                }
            } else {
                Err(Status::NotFound)
            }
        }
    } else {
        Err(Status::Unauthorized)
    }

}

#[delete("/delete_offer/<session_id>", format="application/json")]
pub async fn delete_offer(session_id: i64,claims: Claims, db: &State<PgPool>, resource_map: &State<ResourceMapOffers>,  broadcaster: &State<Arc<WebSocketManager>>)->Result<Status,Status> {
    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(id) = auth {
        let mut conn = db.get().await.expect("Failed to connect toDB");
        let del_res = resource_map.remove(&session_id);
        if del_res.is_some() {
            println!("Offer deleted from resource map");
        }

        let res = diesel::delete(
            offers::table
                .find(session_id)
            )
            .filter(offers::driver_id.eq(&id))
            .execute(&mut conn)
            .await
            .expect("Failed to delete from offer table");

        if res == 0{
            Err(Status::InternalServerError)
        } else {
            let broadcast = BroadcastResource::Deleted(session_id);
            if let Err(e) = broadcaster.broadcast_offer(broadcast) {
                eprintln!("Failed to broadcast request: {}", e);
            }
            Ok(Status::Ok)
        }
    } else {
        Err(Status::Unauthorized)
    }
}

#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct ModifyStopsReq {
    session_id: i64,
    stop1: Place,
    stop2: Place,
}


#[patch("/modify_stops", format="application/json", data="<req>")]
pub async fn modify_stops(
    req: Json<ModifyStopsReq>, 
    resource_map: &State<ResourceMapOffers>,  
    broadcaster: &State<Arc<WebSocketManager>>, 
    db: &State<PgPool>,
    claims: Claims
) -> Result<Status, Custom<String>> {
    let user_id = claims.sub.parse::<i64>()
        .map_err(|_| Custom(Status::BadRequest, "Invalid user ID".to_string()))?;

    let mut conn = db.get().await
        .map_err(|e| Custom(Status::InternalServerError, format!("Database connection failed: {}", e)))?;
    let mut stops: Vec<Stop> = Vec::new();
    let request_arc = match resource_map.get(&req.session_id) {
        Some(existing) => existing.clone(),
        None => {
            let mut request = offers::table
                .filter(offers::session_id.eq(req.session_id))
                .filter(offers::passengers_id.contains(vec![user_id]))
                .first::<Offer>(&mut conn)
                .await
                .map_err(|e| Custom(Status::NotFound, format!("Request not found: {}", e)))?;
            let request_arc = Arc::new(tokio::sync::Mutex::new(request));
            resource_map.insert(req.session_id, request_arc.clone());
            request_arc
        }
    };

    {
        let mut request_guard = request_arc.lock().await;
        request_guard.stops.retain(|s| !s.get_stop(user_id).is_some());
        let stop1 = Stop::new(user_id, req.stop1.clone());
        let stop2 = Stop::new(user_id, req.stop2.clone());
        request_guard.stops.push(stop1);
        request_guard.stops.push(stop2);
        stops = request_guard.stops.clone();
    }

    let updated_rows = diesel::update(
        offers::table
            .filter(offers::session_id.eq(req.session_id))
            .filter(offers::passengers_id.contains(vec![user_id]))
    )
    .set(offers::stops.eq(stops.clone()))
    .execute(&mut conn)
    .await
    .map_err(|e| Custom(Status::InternalServerError, format!("Database update failed: {}", e)))?;

    if updated_rows == 0 {
        return Err(Custom(Status::NotFound, "No request found to update".to_string()));
    }

    let broadcast = BroadcastResource::Modified(req.session_id);
    if let Err(e) = broadcaster.broadcast_offer(broadcast) {
        eprintln!("Failed to broadcast request: {}", e);
    }

    Ok(Status::Ok)
}