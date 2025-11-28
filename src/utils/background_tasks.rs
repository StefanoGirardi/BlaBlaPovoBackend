use std::time::Duration;
use rocket::{serde::json, State};
use tokio::time;
use chrono::{Utc, Duration as ChronoDuration};
use diesel::prelude::*;
use diesel_async::{RunQueryDsl,methods::*};

use crate::{PgPool, schema::{offers, requests, ride_history}, utils::models::{offer::Offer, request::Request, ride_history::{Ride, create_new_ride}, routing::Stop}};

pub async fn start_cleanup_task(pool: PgPool) {
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(7200)); // Run every two hour
        
        println!("🚀 Background cleanup task started. Will run every hour.");
        
        loop {
            interval.tick().await;
            
            match cleanup_expired_requests(&pool).await {
                Ok(count) if count > 0 => {
                    println!("✅ Automatically deleted {} expired ride requests", count);
                }
                Ok(_) => {
                    println!("ℹ️  No expired requests to delete");
                }
                Err(e) => {
                    eprintln!("❌ Error in automated cleanup: {}", e);
                }
            }

            match cleanup_expired_offers(&pool).await {
                Ok(count) if count > 0 => {
                    println!("✅ Automatically deleted {} expired ride offers", count);
                }
                Ok(_) => {
                    println!("ℹ️  No expired offers to delete");
                }
                Err(e) => {
                    eprintln!("❌ Error in automated cleanup: {}", e);
                }
            }
        }
    });
}

async fn cleanup_expired_requests(pool: &PgPool) -> Result<usize, diesel::result::Error> {
    
    let mut conn = pool.get().await.expect("Failed to get DB connection");
    let one_hour_ago = Utc::now() - ChronoDuration::hours(1);

    let expired_requests = diesel::delete(
        requests::table.filter(requests::start_time.lt(one_hour_ago))
    )
    .get_results::<Request>(&mut conn)
    .await?;

    let mut created_count = 0;

    for request in expired_requests.clone() {
        if let Some(driver_id) = request.driver_id(){ // if None request was not accepted so no need to create a ride history
            let stops = vec![
                Stop::new(request.passenger_id(), request.start_pass().clone()),
                Stop::new(request.passenger_id(), request.arrival_pass().clone())
            ];

            let ride = Ride::new(
                request.session_id(),
                driver_id,
                vec![request.passenger_id()],
                request.route().clone(),
                stops.clone(),
                request.start().clone().unwrap(),
                request.arrival().clone().unwrap(),
                request.start_time().clone(),
                request.start_time().clone(),
            );

            let res = diesel::insert_into(ride_history::table)
                .values(&ride)
                .execute(&mut conn)
                .await?;

            if res > 0 {
                println!("Ride created from expired request");
                created_count += 1;
            } else {
                println!("Ride not created from expired request");
            }
        }
    }

    Ok(created_count as usize)
}

async fn cleanup_expired_offers(pool: &PgPool) -> Result<usize, diesel::result::Error> {
    
    let mut conn = pool.get().await.expect("Failed to get DB connection");
    let one_hour_ago = Utc::now() - ChronoDuration::hours(1);

    // Delete expired offers and return them in one query
    let expired_offers = diesel::delete(
        offers::table.filter(offers::start_time.lt(one_hour_ago))
    )
    .get_results::<Offer>(&mut conn)
    .await?;

    let mut created_count = 0;
    for offer in expired_offers.clone() {
        if !offer.passenger_id().is_empty() {  // if there are no passengers no need to create a ride history
            let ride = Ride::new(
                offer.session_id(),
                offer.driver_id(),
                offer.passenger_id().clone(),
                offer.route().clone(),
                offer.stops().clone(),
                offer.start().clone(),
                offer.arrival().clone(),
                offer.start_time().clone(),
                offer.arrival_time().clone(),
            );

            let res = diesel::insert_into(ride_history::table)
                .values(&ride)
                .execute(&mut conn)
                .await?;

            if res > 0 {
                println!("Ride created from expired offer");
                created_count += 1;
            } else {
                println!("Ride not created from expired offer");
            }
        }
    }

    Ok(created_count as usize)
}