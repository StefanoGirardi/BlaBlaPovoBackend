use rocket::{State, get};
use rocket::response::stream::TextStream;
use rocket::serde::{Serialize, json::Json};
use tokio::sync::broadcast;
use tokio_stream::{wrappers::BroadcastStream, StreamExt};
use std::sync::Arc;

use crate::utils::models::offer::OfferGetter;
use crate::utils::models::request::RequestGetter;

#[derive(Clone,Debug,serde::Serialize,serde::Deserialize)]
pub enum BroadcastResource {
    Modified(i64),
    Deleted(i64),
    Created(i64),
}

#[derive(Clone)]
pub struct WebSocketBroadcaster<T: Clone + Send + Sync + 'static> {
    sender: broadcast::Sender<T>,
}

impl<T: Clone + Send + Sync + 'static> WebSocketBroadcaster<T> {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { sender: tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<T> {
        self.sender.subscribe()
    }

    pub fn broadcast(&self, message: T) -> Result<usize, broadcast::error::SendError<T>> {
        self.sender.send(message)
    }
}

pub type OfferBroadcaster = WebSocketBroadcaster<BroadcastResource>;
pub type RequestBroadcaster = WebSocketBroadcaster<BroadcastResource>;

pub struct WebSocketManager {
    pub offer_broadcaster: OfferBroadcaster,
    pub request_broadcaster: RequestBroadcaster,
}

impl WebSocketManager {
    pub fn new() -> Self {
        Self {
            offer_broadcaster: OfferBroadcaster::new(100),
            request_broadcaster: RequestBroadcaster::new(100),
        }
    }

    pub fn broadcast_offer(&self, offer: BroadcastResource) -> Result<usize, broadcast::error::SendError<BroadcastResource>> {
        self.offer_broadcaster.broadcast(offer)
    }

    pub fn broadcast_request(&self, request: BroadcastResource) -> Result<usize, broadcast::error::SendError<BroadcastResource>> {
        self.request_broadcaster.broadcast(request)
    }
}

#[get("/sse/offers")]
pub fn offers_sse(manager: &State<Arc<WebSocketManager>>) -> TextStream![String] {
    let receiver = manager.offer_broadcaster.subscribe();
    let mut stream = BroadcastStream::new(receiver);

    TextStream! {
        yield "data: {\"status\": \"connected\"}\n\n".to_string();
        
        while let Some(result) = stream.next().await {
            match result {
                Ok(offer) => {
                    if let Ok(json) = rocket::serde::json::to_string(&offer) {
                        yield format!("data: {}\n\n", json);
                    }
                }
                Err(_) => {
                    break;
                }
            }
        }
        
        yield "data: {\"status\": \"disconnected\"}\n\n".to_string();
    }
}

#[get("/sse/requests")]
pub fn requests_sse(manager: &State<Arc<WebSocketManager>>) -> TextStream![String] {
    let receiver = manager.request_broadcaster.subscribe();
    let mut stream = BroadcastStream::new(receiver);

    TextStream! {
        yield "data: {\"status\": \"connected\"}\n\n".to_string();
        
        while let Some(result) = stream.next().await {
            match result {
                Ok(request) => {
                    if let Ok(json) = rocket::serde::json::to_string(&request) {
                        yield format!("data: {}\n\n", json);
                    }
                }
                Err(_) => {
                    break;
                }
            }
        }
        
        yield "data: {\"status\": \"disconnected\"}\n\n".to_string();
    }
}