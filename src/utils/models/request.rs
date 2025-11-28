use std::{sync::Arc,io::Write};
use diesel::{deserialize::FromSql, pg::{Pg,PgValue}, prelude::*, serialize::{ToSql,Output,IsNull}};
use rand::{random, Rng};
use rocket::{data::{Data, ToByteUnit}, delete, futures::future::join_all, get, http::Status, patch, post, response::status::{self, Custom}, serde::json::{self, Json}, State};
use serde::{Serialize,Deserialize};
use chrono::{DateTime, NaiveTime, TimeZone, Utc};
use tokio::sync::Mutex;
use crate::{schema::{requests,users}, utils::sse_endpoint::{RequestBroadcaster, WebSocketManager, BroadcastResource}, utils::jwt_management::Claims, PgPool, ResourceMapRequests};
use super::routing::*;
use diesel_async::{methods::*,RunQueryDsl};
use futures_util::join;

#[derive(Debug,Serialize,Deserialize,Clone,Selectable,Insertable)]
#[diesel(table_name = requests)]
pub struct Request {
    session_id: i64,
    passenger_id: i64,
    driver_id: Option<i64>,
    start: Place,
    arrival: Place,
    start_time: DateTime<Utc>,  
    route: Route,
    driver_start: Option<Place>,
    driver_arrival: Option<Place>,
}   


impl Queryable<requests::SqlType, diesel::pg::Pg> for Request {
    type Row = (
        i64,                    // session_id
        i64,                    // passenger_id
        Option<i64>,            // driver_id
        Place,                  // start
        Place,                  // arrival
        DateTime<Utc>,          // start_time
        Route,                  // route
        Option<Place>,          // driver_start
        Option<Place>           // driver_arrival
    );
    
    fn build(row: Self::Row) -> diesel::deserialize::Result<Self> {
        
        Ok(Self {
            session_id: row.0,
            passenger_id: row.1,
            driver_id: row.2,
            start: row.3,
            arrival: row.4,
            start_time: row.5,
            route: row.6,
            driver_start: row.7,            
            driver_arrival: row.8            
        })
    }
}

impl Request {
    
    pub fn session_id(&self) -> i64 {
        self.session_id
    }

    pub fn passenger_id(&self) -> i64 {
        self.passenger_id
    }

    pub fn driver_id(&self) -> Option<i64> {
        self.driver_id
    }

    pub fn start(&self) -> &Option<Place> {
        &self.driver_start
    }

    pub fn start_pass(&self) -> &Place {
        &self.start
    }

    pub fn arrival(&self) -> &Option<Place> {
        &self.driver_arrival
    }

    pub fn arrival_pass(& self) -> & Place {
        &self.arrival
    }

    pub fn start_time(&self) -> DateTime<Utc> {
        self.start_time
    }

    pub fn route(&self) -> &Route {
        &self.route
    }

    pub fn route_mut(&mut self) -> &mut Route {
        &mut self.route
    }

    pub fn has_driver(&self) -> bool {
        self.driver_id.is_some()
    }

    pub fn new(
        session_id: i64,
        passenger_id: i64,
        driver_id: Option<i64>,
        start: Place,
        arrival: Place,
        start_time: DateTime<Utc>,
        route: Route,
        driver_start: Option<Place>,
        driver_arrival: Option<Place>,
    ) -> Self {
        Self { 
            session_id, 
            passenger_id, 
            driver_id, 
            start: start.clone(), 
            arrival: arrival.clone(), 
            start_time, 
            route, 
            driver_start: driver_start.clone(),
            driver_arrival: driver_arrival.clone(),
        }
    }
}

#[derive(Debug,Serialize,Deserialize,Clone)]
pub struct CreateRequest {
    passenger_id: i64,
    start: Place,
    arrival: Place,
    start_time: DateTime<Utc>, 
}

impl CreateRequest {
    pub fn new(
        passenger_id: i64,
        start: Place,
        arrival: Place,
        start_time: DateTime<Utc>
    ) -> Self {
        Self{
            passenger_id,
            start,
            arrival,
            start_time
        }
    }
}


#[post("/requests", format = "application/json", data = "<req>")]
pub async fn create_request(
    req: Json<CreateRequest>,
    db: &State<PgPool>,
    claims: Claims,
    broadcaster: &State<Arc<WebSocketManager>>
) -> Result<Status, Status> {

    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(id) = auth {
        let mut session_id: i64 = rand::rng().random_range(1..i64::MAX);
        let driver_id: Option<i64> = None;
        let route_points: Vec<Place> = Vec::new();
        let route = Route::new(route_points);
        
        let mut conn = db.get().await.expect("Failed to connect to DB");
        
        while let Some(_) = requests::table
            .filter(requests::session_id.eq(session_id))
            .select(requests::session_id)
            .first::<i64>(&mut conn)
            .await
            .optional()
            .expect("DB error")  
        {
            session_id = rand::rng().random_range(1..i64::MAX);
        }
    
        let new_request = Request::new(
            session_id,
            req.passenger_id,
            driver_id,
            req.start.clone(),
            req.arrival.clone(),
            req.start_time,
            route,
            None,
            None
        );
    
        let result = diesel::insert_into(requests::table)
            .values(&new_request)
            .execute(&mut conn)
            .await;
    
        match result {
            Ok(rows_affected) => {
                println!("Inserted {} row(s)", rows_affected);
                let broadcast = BroadcastResource::Created(session_id);
                if let Err(e) = broadcaster.broadcast_request(broadcast) {
                    eprintln!("Failed to broadcast request: {}", e);
                }
                Ok(Status::Created)
            }
            Err(e) => {
                println!("ERR {:?}", e);
                Err(Status::BadRequest)
            }
        }

    } else {
        Err(Status::Unauthorized)
    }
    
}

#[patch("/check_and_assign_driver/<session_id>")]
pub async fn assign_driver(
    session_id: i64, 
    db: &State<PgPool>,
    resource_map: &State<ResourceMapRequests>,
    claims: Claims,
    broadcaster: &State<Arc<WebSocketManager>>
) -> Result<Status, Status> {
    let auth = claims.sub.parse::<i64>().map_err(|_| Status::Unauthorized);

    if let Ok(driver_id) = auth {
        let mut conn = db.get().await.expect("Failed to connect to DB");
        
        if let Some(request) = resource_map.get(&session_id) {
            let guard = Arc::clone(request.value());
            assign_driver_to_request(guard, driver_id, &mut conn,broadcaster).await
        } else {
            let res = requests::table
                .find(session_id)
                .first::<Request>(&mut conn)
                .await
                .optional()
                .expect("Failed to get Request");

            if let Some(request) = res {
                let guard = Arc::new(Mutex::new(request));
                resource_map.insert(session_id, guard);
                if let Some(request) = resource_map.get(&session_id) {
                    let guard = Arc::clone(request.value());
                    assign_driver_to_request(guard, driver_id, &mut conn,broadcaster).await
                } else {
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

async fn assign_driver_to_request(
    request_guard: Arc<Mutex<Request>>,
    driver_id: i64,
    conn: &mut bb8::PooledConnection<'_, diesel_async::pooled_connection::AsyncDieselConnectionManager<diesel_async::AsyncPgConnection>>,
    broadcaster: &State<Arc<WebSocketManager>>
) -> Result<Status, Status> {
    let mut request = request_guard.lock().await;
    
    if request.driver_id.is_some() {
        return Err(Status::BadRequest); 
    }
    
    let result = diesel::update(requests::table.find(request.session_id))
        .set(requests::driver_id.eq(driver_id))
        .execute(conn)
        .await
        .map_err(|_| Status::InternalServerError)?;
    
    if result > 0 {
        request.driver_id = Some(driver_id);
        let broadcast = BroadcastResource::Modified(request.session_id);
        if let Err(e) = broadcaster.broadcast_request(broadcast) {
            eprintln!("Failed to broadcast request: {}", e);
        }
        Ok(Status::Ok)
    } else {
        Err(Status::NotFound)
    }
}

#[patch("/resign_driver/<session_id>")]
pub async fn resign_driver(
    session_id: i64, 
    resource_map: &State<ResourceMapRequests>,
    claims: Claims, 
    db: &State<PgPool>,
    broadcaster: &State<Arc<WebSocketManager>>
) -> Result<Status, Status> {
    let auth = claims.sub.parse::<i64>().map_err(|_| Status::Unauthorized);

    if let Ok(driver_id) = auth {
        let mut conn = db.get().await.expect("Failed to connect to DB");
        
        if let Some(request) = resource_map.get(&session_id) {
            let guard = Arc::clone(request.value());
            resign_driver_from_request(guard, driver_id, &mut conn,broadcaster).await
        } else {
            let res = requests::table
                .find(session_id)
                .first::<Request>(&mut conn)
                .await
                .optional()
                .expect("Failed to get Request");

            if let Some(request) = res {
                let guard = Arc::new(Mutex::new(request));
                resource_map.insert(session_id, guard);
                if let Some(request) = resource_map.get(&session_id) {
                    let guard = Arc::clone(request.value());
                    resign_driver_from_request(guard, driver_id, &mut conn,broadcaster).await
                } else {
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

async fn resign_driver_from_request(
    request_guard: Arc<Mutex<Request>>,
    driver_id: i64,
    conn: &mut bb8::PooledConnection<'_, diesel_async::pooled_connection::AsyncDieselConnectionManager<diesel_async::AsyncPgConnection>>,
    broadcaster: &State<Arc<WebSocketManager>>
) -> Result<Status, Status> {
    let mut request = request_guard.lock().await;
    
    if request.driver_id != Some(driver_id) {
        return Err(Status::Forbidden); 
    }
    
    if request.driver_id.is_none() {
        return Err(Status::Conflict); 
    }

    let result = diesel::update(requests::table.find(request.session_id))
        .set(requests::driver_id.eq(None::<i64>))
        .execute(conn)
        .await
        .map_err(|_| Status::InternalServerError)?;
    
    if result > 0 {
        request.driver_id = None;
        let broadcast = BroadcastResource::Modified(request.session_id);
        if let Err(e) = broadcaster.broadcast_request(broadcast) {
            eprintln!("Failed to broadcast request: {}", e);
        }
        Ok(Status::Ok)
    } else {
        Err(Status::NotFound)
    }
}



#[patch("/renounce_driver/<session_id>")]
pub async fn renounce_driver(
    session_id: i64, 
    resource_map: &State<ResourceMapRequests>,
    claims: Claims, 
    db: &State<PgPool>,
    broadcaster: &State<Arc<WebSocketManager>>
) -> Result<Status, Status> {
    let auth = claims.sub.parse::<i64>().map_err(|_| Status::Unauthorized);

    if let Ok(driver_id) = auth {
        let mut conn = db.get().await.expect("Failed to connect to DB");
        
        if let Some(request) = resource_map.get(&session_id) {
            let guard = Arc::clone(request.value());
            resign_driver_from_request(guard, driver_id, &mut conn,broadcaster).await
        } else {
            let res = requests::table
                .find(session_id)
                .first::<Request>(&mut conn)
                .await
                .optional()
                .expect("Failed to get Request");

            if let Some(request) = res {
                let guard = Arc::new(Mutex::new(request));
                resource_map.insert(session_id, guard);
                if let Some(request) = resource_map.get(&session_id) {
                    let guard = Arc::clone(request.value());
                    renounce_driver_from_request(guard, driver_id, &mut conn, broadcaster).await
                } else {
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

async fn renounce_driver_from_request(
    request_guard: Arc<Mutex<Request>>,
    driver_id: i64,
    conn: &mut bb8::PooledConnection<'_, diesel_async::pooled_connection::AsyncDieselConnectionManager<diesel_async::AsyncPgConnection>>,
    broadcaster: &State<Arc<WebSocketManager>>
) -> Result<Status, Status> {
    let mut request = request_guard.lock().await;
    
    if request.driver_id != Some(driver_id) {
        return Err(Status::Forbidden); 
    }
    
    if request.driver_id.is_none() {
        return Err(Status::Conflict); 
    }
    let vec: Vec<Place> = Vec::new();
    let result = diesel::update(requests::table.find(request.session_id))
        .set((
            requests::driver_id.eq(None::<i64>),
            requests::driver_start.eq(None::<Place>),
            requests::driver_arrival.eq(None::<Place>)
        ))
        .execute(conn)
        .await
        .map_err(|_| Status::InternalServerError)?;
    
    if result > 0 {
        request.driver_id = None;
        let broadcast = BroadcastResource::Modified(request.session_id);
        if let Err(e) = broadcaster.broadcast_request(broadcast) {
            eprintln!("Failed to broadcast request: {}", e);
        }
        Ok(Status::Ok)
    } else {
        Err(Status::NotFound)
    }
}

#[derive(Debug,Serialize,Deserialize,Clone)]
pub struct RequestGetter {
    pub session_id: i64,
    pub passenger_id: i64,
    pub driver_id: Option<i64>,
    pub start: Place,
    pub arrival: Place,
    pub route: Route,
    pub start_time: DateTime<Utc>,
    pub driver_start: Option<Place>,
    pub driver_arrival: Option<Place>,
}

#[get("/get_request/<id>",format = "application/json")]
pub async fn get_request_by_id(id: i64, db: &State<PgPool>, claims: Claims)->Result<Json<RequestGetter>,Status> {
    
    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(_) = auth {

        let mut conn = db.get().await.expect("Failed to connect to DB");
        
        let res = requests::table
            .find(id)
            .first::<Request>(&mut conn)
            .await
            .optional()
            .expect("Failed to get specific Offer");
    
        
    
        if let Some(request)=res {
            Ok(
                Json(
                    RequestGetter {
                        session_id: request.session_id,
                        passenger_id: request.passenger_id,  
                        driver_id: request.driver_id,
                        start: request.start,
                        arrival: request.arrival,
                        route: request.route,
                        start_time: request.start_time,
                        driver_start: request.driver_start,
                        driver_arrival: request.driver_arrival
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

#[get("/get_all_requests/<id>", format = "application/json")]
pub async fn get_all_request(id: i64, db: &State<PgPool>, claims: Claims) -> Result<Json<Vec<RequestGetter>>,Status> {
    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(id) = auth {
        let mut conn = db.get().await.expect("Failed to connect to DB");
    
        let requests = requests::table
            .filter(requests::passenger_id.ne(id))
            .load::<Request>(&mut conn)
            .await
            .unwrap_or_else(|e| {
                println!("Error fetching requests: {:?}", e);
                Vec::new()
            });
    
        let result: Vec<RequestGetter> = requests.into_iter()
            .map(|request| {
                RequestGetter {
                    session_id: request.session_id,
                    passenger_id: request.passenger_id,  
                    driver_id: request.driver_id,
                    start: request.start,
                    arrival: request.arrival,
                    route: request.route,
                    start_time: request.start_time,
                    driver_start: request.driver_start,
                    driver_arrival: request.driver_arrival
                }
            })
            .collect();
        
        Ok(Json(result))

    } else {
        Err(Status::Unauthorized)
    }

}

#[get("/all_my_requests/<id>", format = "application/json")]
pub async fn get_all_my_request(id: i64, db: &State<PgPool>, claims : Claims) -> Result<Json<Vec<RequestGetter>>,Status> {

    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(id) = auth {
        let mut conn = db.get().await.expect("Failed to connect to DB");
    
        let requests = requests::table
            .filter(requests::passenger_id.eq(id))
            .load::<Request>(&mut conn)
            .await
            .unwrap_or_else(|e| {
                println!("Error fetching requests: {:?}", e);
                Vec::new()
            });
        
        let result: Vec<RequestGetter> = requests.into_iter()
            .map(|request| {
                RequestGetter {
                    session_id: request.session_id,
                    passenger_id: request.passenger_id,  
                    driver_id: request.driver_id,
                    start: request.start,
                    arrival: request.arrival,
                    route: request.route,
                    start_time: request.start_time,
                    driver_start: request.driver_start,
                    driver_arrival: request.driver_arrival
                }
            })
            .collect();
        
        Ok(Json(result))
    } else {
        Err(Status::Unauthorized)
    }

}

#[get("/all_requests_to_take/<id>", format = "application/json")]
pub async fn get_all_my_request_to_take(id: i64, db: &State<PgPool>, claims: Claims) -> Result<Json<Vec<RequestGetter>>,Status> {
    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(id) = auth {
        let mut conn = db.get().await.expect("Failed to connect to DB");
    
        let requests = requests::table
            .filter(requests::driver_id.eq(id))
            .load::<Request>(&mut conn)
            .await
            .unwrap_or_else(|e| {
                println!("Error fetching requests: {:?}", e);
                Vec::new()
            });

        let result: Vec<RequestGetter> = requests.into_iter()
            .map(|request| {
                RequestGetter {
                    session_id: request.session_id,
                    passenger_id: request.passenger_id,  
                    driver_id: request.driver_id,
                    route: request.route,
                    start: request.start,
                    arrival: request.arrival,
                    start_time: request.start_time,
                    driver_start: request.driver_start,
                    driver_arrival: request.driver_arrival
                }
            })
            .collect();
        
        Ok(Json(result))
    } else {
        Err(Status::Unauthorized)
    }

}  

async fn get_passenger_name(id: i64, db: &State<PgPool>) -> Result<String, ()> {
    let mut conn = db.get().await.expect("Failed to connect to DB");

    let name = users::table
        .filter(users::id.eq(id))
        .select(users::username)
        .first::<String>(&mut conn)
        .await
        .ok();

    name.ok_or(())
}


#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct ModifyTimeReq {
    session_id: i64,
    passenger_id: i64,
    start_time: DateTime<Utc>,
}

impl ModifyTimeReq {
    pub fn new(
        session_id: i64,
        passenger_id: i64,
        start_time: DateTime<Utc>
    ) -> Self {
        Self {
            session_id,
            passenger_id,
            start_time
        }
    }
}

#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct ModifyStopReq {
    session_id: i64,
    passenger_id: i64,
    stop: Place,
}

impl ModifyStopReq {
    pub fn new(
        session_id: i64,
        passenger_id: i64,
        stop: Place,
    ) -> Self {
        Self {
            session_id,
            passenger_id,
            stop: stop.clone()
        }
    }
}

#[patch("/modify_request_time", format="application/json", data="<req>")]
pub async fn modify_request_time(
    req: Json<ModifyTimeReq>, 
    resource_map: &State<ResourceMapRequests>, 
    broadcaster: &State<Arc<WebSocketManager>>,
    db: &State<PgPool>,
    claims: Claims
) -> Result<Status, Custom<String>> {
    let user_id = claims.sub.parse::<i64>()
        .map_err(|_| Custom(Status::BadRequest, "Invalid user ID".to_string()))?;

    let mut conn = db.get().await
        .map_err(|e| Custom(Status::InternalServerError, format!("Database connection failed: {}", e)))?;

    let request_arc = match resource_map.get(&req.session_id) {
        Some(existing) => existing.clone(),
        None => {
            let request = requests::table
                .filter(requests::session_id.eq(req.session_id))
                .first::<Request>(&mut conn)
                .await
                .map_err(|e| Custom(Status::NotFound, format!("Request not found: {}", e)))?;
            
            let request_arc = Arc::new(tokio::sync::Mutex::new(request));
            resource_map.insert(req.session_id, request_arc.clone());
            request_arc
        }
    };

    {
        let mut request_guard = request_arc.lock().await;
        request_guard.start_time = req.start_time;
    }

    let updated_rows = diesel::update(
        requests::table
            .filter(requests::session_id.eq(req.session_id))
            .filter(requests::passenger_id.eq(req.passenger_id))
    )
    .set(requests::start_time.eq(req.start_time))
    .execute(&mut conn)
    .await
    .map_err(|e| Custom(Status::InternalServerError, format!("Database update failed: {}", e)))?;

    if updated_rows == 0 {
        return Err(Custom(Status::NotFound, "No request found to update".to_string()));
    }

    let request_guard = request_arc.lock().await;
    let name = get_passenger_name(request_guard.passenger_id, db).await
        .unwrap_or_else(|_| "Unknown".to_string());
    
    let broadcast = BroadcastResource::Modified(request_guard.session_id);
    
    drop(request_guard);
    
    if let Err(e) = broadcaster.broadcast_offer(broadcast) {
        eprintln!("Failed to broadcast request: {}", e);
    }
    Ok(Status::Ok)
}

#[patch("/modify_request_start", format="application/json", data="<req>")]
pub async fn modify_start(
    req: Json<ModifyStopReq>, 
    resource_map: &State<ResourceMapRequests>,  
    broadcaster: &State<Arc<WebSocketManager>>, 
    db: &State<PgPool>,
    claims: Claims
) -> Result<Status, Custom<String>> {

    let user_id = claims.sub.parse::<i64>()
        .map_err(|_| Custom(Status::BadRequest, "Invalid user ID".to_string()))?;

    let mut conn = db.get().await
        .map_err(|e| Custom(Status::InternalServerError, format!("Database connection failed: {}", e)))?;

    let request_arc = match resource_map.get(&req.session_id) {
        Some(existing) => existing.clone(),
        None => {
            let mut request = requests::table
                .filter(requests::session_id.eq(req.session_id))
                .first::<Request>(&mut conn)
                .await
                .map_err(|e| Custom(Status::NotFound, format!("Request not found: {}", e)))?;
            
            request.start = req.stop.clone();
            let request_arc = Arc::new(tokio::sync::Mutex::new(request));
            resource_map.insert(req.session_id, request_arc.clone());
            request_arc
        }
    };

    {
        let mut request_guard = request_arc.lock().await;
        request_guard.start = req.stop.clone();
    }

    let updated_rows = diesel::update(
        requests::table
            .filter(requests::session_id.eq(req.session_id))
            .filter(requests::passenger_id.eq(req.passenger_id))
    )
    .set(requests::start.eq(&req.stop))
    .execute(&mut conn)
    .await
    .map_err(|e| Custom(Status::InternalServerError, format!("Database update failed: {}", e)))?;

    if updated_rows == 0 {
        return Err(Custom(Status::NotFound, "No request found to update".to_string()));
    }

    let request_guard = request_arc.lock().await;
    let name = get_passenger_name(request_guard.passenger_id, db).await
        .unwrap_or_else(|_| "Unknown".to_string());
    
    let broadcast = BroadcastResource::Modified(req.session_id);
    
    drop(request_guard);
    
    if let Err(e) = broadcaster.broadcast_offer(broadcast) {
        eprintln!("Failed to broadcast request: {}", e);
    }
    Ok(Status::Ok)
}


#[patch("/modify_request_arrival", format="application/json", data="<req>")]
pub async fn modify_arrival(
    req: Json<ModifyStopReq>, 
    resource_map: &State<ResourceMapRequests>,  
    broadcaster: &State<Arc<WebSocketManager>>, 
    db: &State<PgPool>,
    claims: Claims,
) -> Result<Status, Custom<String>> {
    let user_id = claims.sub.parse::<i64>()
        .map_err(|_| Custom(Status::BadRequest, "Invalid user ID".to_string()))?;

    let mut conn = db.get().await
        .map_err(|e| Custom(Status::InternalServerError, format!("Database connection failed: {}", e)))?;

    let request_arc = match resource_map.get(&req.session_id) {
        Some(existing) => existing.clone(),
        None => {
            let mut request = requests::table
                .filter(requests::session_id.eq(req.session_id))
                .first::<Request>(&mut conn)
                .await
                .map_err(|e| Custom(Status::NotFound, format!("Request not found: {}", e)))?;
            
            request.arrival = req.stop.clone();
            let request_arc = Arc::new(tokio::sync::Mutex::new(request));
            resource_map.insert(req.session_id, request_arc.clone());
            request_arc
        }
    };

    {
        let mut request_guard = request_arc.lock().await;
        request_guard.arrival = req.stop.clone();
    }

    let updated_rows = diesel::update(
        requests::table
            .filter(requests::session_id.eq(req.session_id))
            .filter(requests::passenger_id.eq(req.passenger_id))
    )
    .set(requests::arrival.eq(&req.stop))
    .execute(&mut conn)
    .await
    .map_err(|e| Custom(Status::InternalServerError, format!("Database update failed: {}", e)))?;

    if updated_rows == 0 {
        return Err(Custom(Status::NotFound, "No request found to update".to_string()));
    }

    let passenger_id = {
        let request_guard = request_arc.lock().await;
        request_guard.passenger_id
    };

    let name = get_passenger_name(passenger_id, db).await
        .unwrap_or_else(|_| "Unknown".to_string());

    let broadcast = BroadcastResource::Modified(req.session_id);
    
    if let Err(e) = broadcaster.broadcast_offer(broadcast) {
        eprintln!("Failed to broadcast request: {}", e);
    }
    Ok(Status::Ok)
}

#[patch("/modify_driver_start", format="application/json", data="<req>")]
pub async fn modify_driver_start(
    req: Json<ModifyStopReq>, 
    resource_map: &State<ResourceMapRequests>,  
    broadcaster: &State<Arc<WebSocketManager>>, 
    db: &State<PgPool>,
    claims: Claims
) -> Result<Status, Custom<String>> {
    let user_id = claims.sub.parse::<i64>()
        .map_err(|_| Custom(Status::BadRequest, "Invalid user ID".to_string()))?;

    let mut conn = db.get().await
        .map_err(|e| Custom(Status::InternalServerError, format!("Database connection failed: {}", e)))?;

    let request_arc = match resource_map.get(&req.session_id) {
        Some(existing) => existing.clone(),
        None => {
            let mut request = requests::table
                .filter(requests::session_id.eq(req.session_id))
                .first::<Request>(&mut conn)
                .await
                .map_err(|e| Custom(Status::NotFound, format!("Request not found: {}", e)))?;
            
            request.driver_start = Some(req.stop.clone());
            let request_arc = Arc::new(tokio::sync::Mutex::new(request));
            resource_map.insert(req.session_id, request_arc.clone());
            request_arc
        }
    };

    {
        let mut request_guard = request_arc.lock().await;
        request_guard.arrival = req.stop.clone();
    }

    let updated_rows = diesel::update(
        requests::table
            .filter(requests::session_id.eq(req.session_id))
            .filter(requests::driver_id.eq(Some(user_id)))
    )
    .set(requests::driver_start.eq(Some(req.stop.clone())))
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


#[patch("/modify_driver_arrival", format="application/json", data="<req>")]
pub async fn modify_driver_arrival(
    req: Json<ModifyStopReq>, 
    resource_map: &State<ResourceMapRequests>,  
    broadcaster: &State<Arc<WebSocketManager>>, 
    db: &State<PgPool>,
    claims: Claims
) -> Result<Status, Custom<String>> {
    let user_id = claims.sub.parse::<i64>()
        .map_err(|_| Custom(Status::BadRequest, "Invalid user ID".to_string()))?;

    let mut conn = db.get().await
        .map_err(|e| Custom(Status::InternalServerError, format!("Database connection failed: {}", e)))?;

    let request_arc = match resource_map.get(&req.session_id) {
        Some(existing) => existing.clone(),
        None => {
            let mut request = requests::table
                .filter(requests::session_id.eq(req.session_id))
                .first::<Request>(&mut conn)
                .await
                .map_err(|e| Custom(Status::NotFound, format!("Request not found: {}", e)))?;
            
            request.driver_arrival = Some(req.stop.clone());
            let request_arc = Arc::new(tokio::sync::Mutex::new(request));
            resource_map.insert(req.session_id, request_arc.clone());
            request_arc
        }
    };

    {
        let mut request_guard = request_arc.lock().await;
        request_guard.arrival = req.stop.clone();
    }

    let updated_rows = diesel::update(
        requests::table
            .filter(requests::session_id.eq(req.session_id))
            .filter(requests::driver_id.eq(Some(user_id)))
    )
    .set(requests::driver_arrival.eq(Some(req.stop.clone())))
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

#[delete("/delete_request/<id>")]
pub async fn delete_request(
    id: i64,
    db: &State<PgPool>, 
    resource_map: &State<ResourceMapRequests>,
    broadcaster: &State<Arc<WebSocketManager>>,
    claims: Claims
) -> Result<Json<&'static str>, Status> {

    let user_id = claims.sub.parse::<i64>()
        .map_err(|_| Status::BadRequest)?;

    let mut conn = db.get().await
        .map_err(|_| Status::InternalServerError)?;

    resource_map.remove(&id);

    let rows_affected = diesel::delete(
        requests::table
            .filter(requests::session_id.eq(id))
            .filter(requests::passenger_id.eq(user_id)) 
    )
    .execute(&mut conn)
    .await
    .map_err(|_| Status::InternalServerError)?;

    match rows_affected {
        0 => Err(Status::NotFound),
        _ => {
            let broadcast = BroadcastResource::Deleted(id);
            if let Err(e) = broadcaster.broadcast_offer(broadcast) {
                eprintln!("Failed to broadcast request: {}", e);
            }
            Ok(Json("Request deleted successfully"))
        }
    }
}

#[derive(Debug,Serialize,Deserialize,Clone)]
pub struct TakeReq {
    session_id: i64,
    start: Place,
    arrival: Place,
    route: Route,
}

impl TakeReq {
    pub fn new(
        session_id: i64,
        start: Place,
        arrival: Place,
        route: Route,
    ) -> Self {
        Self {
            session_id,
            start,
            arrival,
            route
        }
    }
}


#[patch("/take_request", format = "application/json", data = "<req>")]
pub async fn take_request(
    req: Json<TakeReq>,
    db: &State<PgPool>,
    resource_map: &State<ResourceMapRequests>,
    claims: Claims,
    broadcaster: &State<Arc<WebSocketManager>>
) -> Result<Status, Custom<String>> {
    let driver_id = claims.sub.parse::<i64>()
        .map_err(|_| Custom(Status::BadRequest, "Invalid user ID".to_string()))?;

    let request_arc = match resource_map.get(&req.session_id) {
        Some(existing) => existing.clone(),
        None => {
            let mut conn = db.get().await
                .map_err(|e| Custom(Status::InternalServerError, format!("Database connection failed: {}", e)))?;
            
            let request = requests::table
                .filter(requests::session_id.eq(req.session_id))
                .first::<Request>(&mut conn)
                .await
                .map_err(|e| Custom(Status::NotFound, format!("Request not found: {}", e)))?;

            let request_arc = Arc::new(tokio::sync::Mutex::new(request));
            resource_map.insert(req.session_id, request_arc.clone());
            request_arc
        }
    };

    take_request_helper(&request_arc, req, db, driver_id, broadcaster).await
}

async fn take_request_helper(
    request: &Arc<Mutex<Request>>,
    req: Json<TakeReq>,
    db: &State<PgPool>,
    driver_id: i64,
    broadcaster: &State<Arc<WebSocketManager>>
) -> Result<Status, Custom<String>> {
    let mut conn = db.get().await
        .map_err(|e| Custom(Status::InternalServerError, format!("Database connection failed: {}", e)))?;

    let updated_rows = diesel::update(
        requests::table.find(req.session_id),
    )
    .filter(requests::driver_id.eq(Some(driver_id)))
    .set((
        requests::driver_start.eq(&Some(req.start.clone())),
        requests::driver_arrival.eq(&Some(req.arrival.clone())),
        requests::route.eq(&req.route.clone()),
    ))
    .execute(&mut conn)
    .await
    .map_err(|e| Custom(Status::InternalServerError, format!("Database update failed: {}", e)))?;

    if updated_rows == 0 {
        return Err(Custom(Status::NotFound, "Request not found in database".to_string()));
    }

    {
        let mut request_guard = request.lock().await;
        request_guard.driver_start = Some(req.start.clone());
        request_guard.driver_arrival = Some(req.arrival.clone());
        request_guard.route = req.route.clone();

        let broadcast = BroadcastResource::Modified(request_guard.session_id);
    
        if let Err(e) = broadcaster.broadcast_offer(broadcast) {
            eprintln!("Failed to broadcast request: {}", e);
        }
    }

    Ok(Status::Ok)
}