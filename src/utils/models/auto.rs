use std::io::Write;
use diesel::expression::AsExpression;
use diesel::{deserialize::{FromSql, FromSqlRow}, pg::{Pg, PgValue}, serialize::{ToSql, Output, IsNull}};
use serde::{Deserialize, Serialize};
use rocket::serde::json;

#[derive(Debug, Serialize, Deserialize, Clone, AsExpression, FromSqlRow)]
#[diesel(sql_type = diesel::sql_types::Jsonb)]
pub struct Auto {
    brand: String,
    model: String,
}

impl Auto {
    pub fn new(brand: String, model: String) -> Self {
        Self { brand, model }
    }

    pub fn get_brand(&self) -> String {
        self.brand.clone()
    }

    pub fn get_model(&self) -> String {
        self.model.clone()
    }
}

impl FromSql<diesel::sql_types::Jsonb, Pg> for Auto {
    fn from_sql(bytes: PgValue) -> diesel::deserialize::Result<Self> {
        let bytes = bytes.as_bytes();
        if bytes.is_empty() || bytes[0] != 1 {
            return Err("Invalid JSONB format".into());
        }
        Ok(json::serde_json::from_slice(&bytes[1..])?)
    }
}

impl ToSql<diesel::sql_types::Jsonb, Pg> for Auto {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> diesel::serialize::Result {
        out.write_all(&[1])?;
        let json_bytes = json::serde_json::to_vec(self)?;
        out.write_all(&json_bytes)?;
        Ok(IsNull::No)
    }
}