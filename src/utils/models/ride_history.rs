use std::collections::HashMap;
use chrono::{DateTime, Local, Utc};
use diesel::prelude::Queryable;
use rocket::http::Status;
use rocket::{get, State, serde::json};
use serde::{Serialize,Deserialize};
use rocket::serde::json::Json;
use crate::utils::models::routing::{Route,Place,Stop};
use diesel::prelude::*;
use diesel::pg::{Pg,PgValue};
use diesel::deserialize::FromSql;
use diesel::serialize::{IsNull,ToSql,Output};
use diesel_async::{methods::*,RunQueryDsl};
use crate::schema::ride_history;
use std::io::Write;
use crate::PgPool;
use crate::utils::jwt_management::Claims;


#[derive(Debug,Clone,Serialize,Deserialize,Selectable,Insertable)]
#[diesel(table_name = ride_history)]
pub struct Ride {
  session_id: i64,
  driver_id: i64,
  passengers_id: Vec<i64>,
  route: Route,
  stops: Vec<Stop>,
  start: Place,
  arrival: Place,
  start_time: DateTime<Utc>,
  arrival_time: DateTime<Utc>,
}

impl Queryable<ride_history::SqlType, Pg> for Ride {
    type Row = (
        i64,                    // session_id
        i64,                    // driver_id
        Vec<Option<i64>>,       // passengers_id
        Route,                  // route
        Vec<Option<Stop>>,      // stops
        Place,                  // start
        Place,                  // arrival
        DateTime<Utc>,          // start_time
        DateTime<Utc>,          // arrival_time
    );
    
    fn build(row: Self::Row) -> diesel::deserialize::Result<Self> {
        let passengers_id: Vec<i64> = row.2.into_iter().flatten().collect();
        let stops: Vec<Stop> = row.4.into_iter().flatten().collect();
        
        Ok(Self {
            session_id: row.0,
            driver_id: row.1,
            passengers_id,  // Now matches
            route: row.3,
            stops,
            start: row.5,
            arrival: row.6,
            start_time: row.7,
            arrival_time: row.8,
        })
    }
}

impl Ride {
    pub fn new(
        session_id: i64,
        driver_id: i64,
        passengers_id: Vec<i64>,
        route: Route,
        stops: Vec<Stop>,
        start: Place,
        arrival: Place,
        start_time: DateTime<Utc>,
        arrival_time: DateTime<Utc>,
    ) -> Self {
        Self {
            session_id,
            driver_id,
            passengers_id,
            start,
            arrival,
            route,
            stops,
            start_time,
            arrival_time,
        }
    }
}

pub async fn create_new_ride(ride: Ride, db: &PgPool) -> Result<usize, diesel::result::Error> {
    let mut conn = db.get().await.expect("Failed to connect to DB");

    let result = diesel::insert_into(ride_history::table)
        .values(&ride)
        .execute(&mut conn)
        .await?;

    Ok(result)
}

#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct RequestRideGetter {
    session_id: i64,
    driver_id: i64,
    passengers_id: i64,
    start: Stop,
    arrival: Stop,
    route: Route,
    day: DateTime<Utc>,
}

#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct OfferRideGetter {
    session_id: i64,
    driver_id: i64,
    passengers_id: Vec<i64>,
    start: Stop,
    arrival: Stop,
    route: Route,
    stops: Vec<Stop>,
    day: DateTime<Utc>,
}

#[get("/history/requests/<id>", format="application/json")]
pub async fn get_all_request_history(
    id: i64,
    db: &State<PgPool>,
    claims: Claims
) -> Result<Json<Vec<RequestRideGetter>>, Status> {
    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(id) = auth {
        let mut conn = db.get().await.map_err(|_| Status::InternalServerError)?;
    
        // Get rides where the user is a passenger but not the driver
        let rides = ride_history::table
            .filter(ride_history::driver_id.ne(id))
            .filter(ride_history::passengers_id.contains(vec![id]))
            .load::<Ride>(&mut conn)
            .await
            .map_err(|e| {
                eprintln!("❌ Database error: {e}");
                Status::InternalServerError
            })?;
    
        if rides.is_empty() {
            return Err(Status::NotFound);
        }
    
        let vec: Vec<RequestRideGetter> = rides.into_iter()
            .filter_map(|ride| {
                let stops_vec = ride.stops
                    .into_iter()
                    .filter_map(|s| s.get_stop(id))
                    .collect::<Vec<_>>();
                
                if stops_vec.len()>0 {
                    if let (Some(start), Some(arrival)) = (Some(stops_vec[0].clone()), stops_vec.last()) {
                        Some(RequestRideGetter {
                            session_id: ride.session_id,
                            driver_id: ride.driver_id,
                            passengers_id: id,
                            route: ride.route,
                            start: start.clone(),
                            arrival: arrival.clone(),
                            day: ride.start_time,
                        })
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
    
        if vec.is_empty() {
            Err(Status::NotFound)
        } else {
            Ok(Json(vec))
        }
    } else {
        Err(Status::Unauthorized)
    }

}

#[get("/history/offers/<id>", format = "application/json")]
pub async fn get_all_offers_history(
    id: i64,
    db: &State<PgPool>,
    claims: Claims
) -> Result<Json<Vec<OfferRideGetter>>, Status> {

    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(id) = auth {
        let mut conn = db.get().await.map_err(|_| Status::InternalServerError)?;
    
        // Get rides where the user is the driver
        let rides = ride_history::table
            .filter(ride_history::driver_id.eq(id))
            .load::<Ride>(&mut conn)
            .await
            .map_err(|e| {
                eprintln!("❌ Database error in get_all_offers_history: {e}");
                Status::InternalServerError
            })?;
    
        if rides.is_empty() {
            return Err(Status::NotFound);
        }
    
        let vec: Vec<OfferRideGetter> = rides.into_iter()
            .map(|ride| {
                OfferRideGetter {
                    session_id: ride.session_id,
                    driver_id: ride.driver_id,
                    passengers_id: ride.passengers_id.clone(),
                    route: ride.route,
                    start: Stop::new(id, ride.start.clone()),
                    arrival: Stop::new(id, ride.arrival.clone()),
                    stops: ride.stops.clone(),
                    day: ride.start_time,
                }
            })
            .collect();
    
        Ok(Json(vec))
    } else {
        Err(Status::Unauthorized)
    }
    
}

pub async fn remove_passengers_id(id: i64, db: &State<PgPool>) -> Result<usize, diesel::result::Error> {
    let mut conn = db.get().await.expect("Failed to connect to DB");

    let mut rides_with_passenger = ride_history::table
        .filter(ride_history::passengers_id.contains(vec![id]))
        .load::<Ride>(&mut conn)
        .await?;

    let mut total_updated = 0;

    for ride in &mut rides_with_passenger {
        if let Some(index) = ride.passengers_id.iter().position(|&pid| pid == id) {
            ride.passengers_id.remove(index);
            
            let updated = diesel::update(
                ride_history::table
                    .filter(ride_history::session_id.eq(ride.session_id))
            )
            .set(ride_history::passengers_id.eq(&ride.passengers_id))
            .execute(&mut conn)
            .await?;
            
            total_updated += updated;
        }
    }

    Ok(total_updated)
}