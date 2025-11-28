use chrono::Duration;
use jsonwebtoken::{EncodingKey,DecodingKey,Validation,encode,decode,Header};
use rocket::{request::{FromRequest,Outcome,Request,},http::{Status}};
use serde::{Serialize,Deserialize}; 

#[derive(Serialize,Deserialize,Debug,Clone)]
pub struct Claims {
    pub sub: String,
    pub email: String,
    pub exp: usize,
    pub iat: usize
}

#[derive(Debug,Serialize,Deserialize,Clone)]
pub struct Tokens {
    pub access_token: String,
}

pub fn generate_jwt_tokens(user_id: &str, email: &str) -> Result<Tokens, Status> {
    dotenvy::dotenv().ok();
    let now = chrono::Utc::now();
    let access_exp = (now + Duration::hours(1)).timestamp() as usize;

    let access_claims = Claims {
        sub: user_id.to_string(),
        email: email.to_string(),
        exp: access_exp,
        iat: now.timestamp() as usize,
    };
    let secret = std::env::var("JWT_SECRET").expect("JWT_SECRET MUST BE SET");
    let encoding_key = EncodingKey::from_secret(secret.as_ref());

    let access_token = encode(&Header::default(), &access_claims, &encoding_key)
        .map_err(|_| Status::InternalServerError)?;


    Ok(Tokens {
        access_token,
    })
}

pub fn validate_jwt(token: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
    dotenvy::dotenv().ok();
    let secret = std::env::var("JWT_SECRET").expect("JWT_SECRET MUST BE SET");
    let decoding_key = DecodingKey::from_secret(secret.as_ref());
    let validation = Validation::default();
    
    decode::<Claims>(token, &decoding_key, &validation)
        .map(|data| data.claims)
}


#[rocket::async_trait]
impl <'r> FromRequest <'r> for Claims {
    type Error = ();

    async fn from_request(request: &'r Request<'_>) ->Outcome<Self,Self::Error>{
        let header = request.headers().get_one("Authorization");

        match header {
            Some(hdr) if hdr.starts_with("Bearer ") => {
                let tkn = hdr["Bearer ".len()..].trim();
                match validate_jwt(tkn) {
                    Ok(clm) => Outcome::Success(clm),
                    Err(_) => Outcome::Error((Status::Unauthorized,())),
                }
            },
            _=> Outcome::Error((Status::Unauthorized,())),
        }    
    }
}