use diesel::expression::AsExpression;
use diesel::prelude::*;
use diesel::deserialize::{FromSql, FromSqlRow};
use diesel::serialize::{ToSql, Output, IsNull};
use diesel::pg::{Pg, PgValue};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use rocket::serde::json;

#[derive(Debug, Serialize, Deserialize, Clone, AsExpression, FromSqlRow)]
#[diesel(sql_type = diesel::sql_types::Jsonb)]
pub struct Place {
    lat: f64,
    lng: f64,
}

impl Place {
    pub fn new(lat: f64, lng: f64) -> Self {
        Self { lat, lng }
    }

    pub fn get_place(&self) -> Self {
        self.clone()
    }
}

impl FromSql<diesel::sql_types::Jsonb, Pg> for Place {
    fn from_sql(bytes: PgValue) -> diesel::deserialize::Result<Self> {
        let bytes = bytes.as_bytes();
        if bytes.is_empty() || bytes[0] != 1 {
            return Err("Invalid JSONB format".into());
        }
        Ok(json::serde_json::from_slice(&bytes[1..])?)
    }
}

impl ToSql<diesel::sql_types::Jsonb, Pg> for Place {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> diesel::serialize::Result {
        out.write_all(&[1])?;
        let json_bytes = json::serde_json::to_vec(self)?;
        out.write_all(&json_bytes)?;
        Ok(IsNull::No)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, AsExpression, FromSqlRow)]
#[diesel(sql_type = diesel::sql_types::Jsonb)]
pub struct Route {
    route: Vec<Place>
}

impl Route {
    pub fn get_route(&self) -> Vec<Place> {
        self.route.clone()
    }
    
    pub fn new(points: Vec<Place>) -> Self {
        Self { route: points }
    }
}

impl FromSql<diesel::sql_types::Jsonb, Pg> for Route {
    fn from_sql(bytes: PgValue) -> diesel::deserialize::Result<Self> {
        let bytes = bytes.as_bytes();
        if bytes.is_empty() || bytes[0] != 1 {
            return Err("Invalid JSONB format".into());
        }
        Ok(json::serde_json::from_slice(&bytes[1..])?)
    }
}

impl ToSql<diesel::sql_types::Jsonb, Pg> for Route {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> diesel::serialize::Result {
        out.write_all(&[1])?;
        let json_bytes = json::serde_json::to_vec(self)?;
        out.write_all(&json_bytes)?;
        Ok(IsNull::No)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, AsExpression, FromSqlRow)]
#[diesel(sql_type = diesel::sql_types::Jsonb)]
pub struct Stop {
    id: i64,
    stop: Place,
}

impl Stop {
    pub fn new(id: i64, stop: Place) -> Self {
        Self { id, stop }
    }

    pub fn get_stop(&self, id: i64) -> Option<Self> {
        if id == self.id {
            Some(self.clone())
        } else {
            None
        }
    }
}

impl FromSql<diesel::sql_types::Jsonb, Pg> for Stop {
    fn from_sql(bytes: PgValue) -> diesel::deserialize::Result<Self> {
        let bytes = bytes.as_bytes();
        if bytes.is_empty() || bytes[0] != 1 {
            return Err("Invalid JSONB format".into());
        }
        Ok(json::serde_json::from_slice(&bytes[1..])?)
    }
}

impl ToSql<diesel::sql_types::Jsonb, Pg> for Stop {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> diesel::serialize::Result {
        out.write_all(&[1])?;
        let json_bytes = json::serde_json::to_vec(self)?;
        out.write_all(&json_bytes)?;
        Ok(IsNull::No)
    }
}