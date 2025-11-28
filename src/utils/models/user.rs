use std::collections::HashMap;
use rocket::{Data, FromForm, FromFormField, State, data::ToByteUnit, delete, get, http::{Status, ext::IntoCollection}, patch, post, put, request::{FromRequest,Outcome}, response::{Redirect, content::RawHtml, status}, serde::json::{self, Json}};
use serde::{Deserialize, Serialize};
use crate::utils::{jwt_management::Tokens, models::{auto::{self, Auto},request::Request, routing::Route}};
use diesel::{deserialize::FromSqlRow, expression::AsExpression, prelude::*};
use diesel::serialize::{ToSql,Output,IsNull};
use diesel::deserialize::{FromSql, Queryable};
use diesel::pg::{Pg, PgValue};
use std::io::Write;
use super::super::super::{schema::{offers,users,requests,ride_history},PgPool};
use diesel_async::{RunQueryDsl,methods::*};
use diesel_async::AsyncConnection;
use crate::utils::jwt_management::Claims;
use log::{error,info};



#[derive(Debug,Serialize,Deserialize,Clone,AsExpression,FromSqlRow)]
#[diesel(sql_type = diesel::sql_types::Jsonb)]
pub struct StarredRoutes(HashMap<String,Route>);

impl FromSql<diesel::sql_types::Jsonb, Pg> for StarredRoutes {
    fn from_sql(bytes: PgValue) -> diesel::deserialize::Result<Self> {
        let bytes = bytes.as_bytes();
        if bytes.is_empty() {
            return Ok(StarredRoutes(HashMap::new()));
        }
        if bytes[0] != 1 {
            return Err("Invalid JSONB version".into());
        }
        let map: HashMap<String, Route> = json::serde_json::from_slice(&bytes[1..])?;
        Ok(StarredRoutes(map))
    }
}

impl ToSql<diesel::sql_types::Jsonb, Pg> for StarredRoutes {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> diesel::serialize::Result {
        out.write_all(&[1])?;
        let json_bytes = json::serde_json::to_vec(&self.0)?;
        out.write_all(&json_bytes)?;
        Ok(IsNull::No)
    }
}

#[derive(Debug,Serialize,Deserialize)]
pub struct CreateUserRequest {
    id: i64,
    name: String,
    surname: String,
    mail: String,
    idada: String,
}

impl CreateUserRequest {
    pub fn new(
        id: i64,
        name: String,
        surname: String,
        mail: String,
        idada: String,
    )->Self {
        Self { id, name, surname, mail, idada }
    }
}

#[derive(Serialize,Deserialize,Debug,Selectable,Insertable,Clone)]
#[diesel(table_name = users)]
pub struct User {
    id: i64,
    name: String,
    surname: String,
    username: String,
    telegram_username: Option<String>,
    mail: String,
    idada: String,
    starred_routes: StarredRoutes,
    auto: Option<Auto>,
}

impl Queryable<users::SqlType, Pg> for User {
    type Row = (
        i64,                    // id
        String,                 // name
        String,                 // surname  
        String,                 // username
        Option<String>,         // telegram_username
        String,                 // mail
        String,                    // idada
        StarredRoutes,      // starred_routes 
        Option<Auto>, // auto 
    );

    fn build(row: Self::Row) -> diesel::deserialize::Result<Self> {
        let (id, name, surname, username, telegram_username, mail, idada, starred_routes, auto) = row;
        Ok(User {
            id,
            name,
            surname, 
            username,
            telegram_username,
            mail,
            idada,
            starred_routes: starred_routes,
            auto,
        })
    }
}

impl User {
    
    pub fn new (id: i64, name: String, surname: String, mail: String, idada: String) -> Self{
        Self {
            id,
            name,
            surname,
            mail,
            idada,
            username: "".to_string(),
            telegram_username: None,
            starred_routes: StarredRoutes(HashMap::new()),
            auto: None,
        }
    }

    pub fn get_name(&self)->String{
        self.name.clone()
    }

    pub fn get_surname(&self)->String{
        self.surname.clone()
    }

    pub fn get_username(&self)->String{
        self.username.clone()
    }

    pub fn get_mail(&self)->String{
        self.mail.clone()
    }
}

#[derive(Debug,Serialize,Deserialize,Clone)]
pub struct UserInfo {
    id: i64,
    name: String,
    surname: String,
    username: String,
    telegram_username: Option<String>,
    mail: String,
    starred_routes: StarredRoutes,
    auto: Option<Auto>,
}
impl UserInfo {

    pub fn new (id: i64, name: String, surname: String, mail: String, username: String, telegram_username: Option<String>, starred_routes: StarredRoutes, auto: Option<Auto>) -> Self{
        Self {
            id,
            name,
            surname,
            mail,
            username,
            telegram_username,
            starred_routes,
            auto,
        }
    }
}

#[derive(Debug,Serialize,Deserialize,Clone)]
pub struct SamlAttr {
    given_name: Option<String>,
    sn: Option<String>,
    email: Option<String>,
    idada: Option<String>,
}

impl SamlAttr {
    fn read_vars() -> Self {
        let given_name = std::env::var("givenname")
            .or_else(|_| std::env::var("HTTP_GIVENNAME"))
            .or_else(|_| std::env::var("X-Shib-GivenName"))
            .ok();

        let sn = std::env::var("sn")
            .or_else(|_| std::env::var("HTTP_SN"))
            .or_else(|_| std::env::var("X-Shib-sn"))
            .ok();

        let email = std::env::var("mail")
            .or_else(|_| std::env::var("HTTP_MAIL"))
            .or_else(|_| std::env::var("X-Shib-Mail"))
            .ok();

        let idada = std::env::var("idada")
            .or_else(|_| std::env::var("HTTP_IDADA"))
            .or_else(|_| std::env::var("X-Shib-idada"))
            .ok();

        info!("{:?}\n{:?}\n{:?}\n{:?}",given_name,sn,email,idada);

        Self {
            given_name,
            sn, 
            email,
            idada
        }
    }
}

#[rocket::async_trait]
impl <'r> FromRequest <'r> for SamlAttr {
    type Error = SamlErr;

    async fn from_request(request: &'r rocket::request::Request<'_>) ->Outcome<Self,Self::Error>{
        let saml_attr = match get_headers(request) {
            Ok(attrs) => attrs,
            Err(e) => return Outcome::Error((Status::BadRequest, e)),
        };

        Outcome::Success(saml_attr)
    }
}

fn get_headers(request: &rocket::request::Request) -> Result<SamlAttr, SamlErr> {
    use std::env;
    
    // Log all available headers for debugging
    info!("Available headers:");
    for header in request.headers().iter() {
        info!("  {}: {}", header.name(), header.value());
    }
    
    // Log relevant environment variables
    info!("Relevant environment variables:");
    for (key, value) in env::vars() {
        if key.to_lowercase().contains("given") || key.to_lowercase().contains("sn") || 
           key.to_lowercase().contains("mail") || key.to_lowercase().contains("idada") ||
           key.contains("SHIB") {
            info!("  {}: {}", key, value);
        }
    }
    
    // Use get_one() which is case-insensitive
    let given_name = request.headers().get_one("X-Shib-GivenName")
        .or_else(|| request.headers().get_one("Given-Name"))
        .or_else(|| request.headers().get_one("givenname"))
        .or_else(|| request.headers().get_one("HTTP_GIVENNAME"))
        .map(|s| s.to_string())
        .or_else(|| env::var("givenname").ok());

    let sn = request.headers().get_one("X-Shib-Surname")
        .or_else(|| request.headers().get_one("Surname"))
        .or_else(|| request.headers().get_one("sn"))
        .or_else(|| request.headers().get_one("HTTP_SN"))
        .map(|s| s.to_string())
        .or_else(|| env::var("sn").ok());

    let email = request.headers().get_one("X-Shib-Mail")
        .or_else(|| request.headers().get_one("Email"))
        .or_else(|| request.headers().get_one("mail"))
        .or_else(|| request.headers().get_one("HTTP_MAIL"))
        .map(|s| s.to_string())
        .or_else(|| env::var("mail").ok());

    let idada = request.headers().get_one("X-Shib-Idada")
        .or_else(|| request.headers().get_one("Idada"))
        .or_else(|| request.headers().get_one("idada"))
        .or_else(|| request.headers().get_one("HTTP_IDADA"))
        .map(|s| s.to_string())
        .or_else(|| env::var("idada").ok());

    info!("Extracted attributes: given_name={:?}, sn={:?}, email={:?}, idada={:?}", 
          given_name, sn, email, idada);

    Ok(SamlAttr { given_name, sn, email, idada })
}

#[derive(Debug,Serialize,Deserialize,Clone)]
pub enum SamlErr {
    Invalid,
    Missing(String)
}

#[derive(Debug,Serialize,Deserialize,Clone,FromForm)]
pub struct AuthState {
    state: String
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for AuthState {
    type Error = ();

    async fn from_request(request: &'r rocket::request::Request<'_>) -> Outcome<Self, Self::Error> {
        if let Some(state_result) = request.query_value::<String>("state") {
            match state_result {
                Ok(state) => Outcome::Success(AuthState { state }),
                Err(_) => Outcome::Error((Status::BadRequest, ())),
            }
        } else {
            Outcome::Error((Status::BadRequest, ()))
        }
    }
}

#[get("/login")]
pub async fn login() -> Redirect{
    Redirect::to(format!("http://mp.disi.unitn.it/blablaunitn/api/auth/saml_handle"))
}

#[get("/test/redirect?<user_json_str>&<token_res>")]
pub async fn test_re(user_json_str: String,token_res: String) ->Redirect {
    Redirect::to(format!("com.example.app://auth/callback?userinfo={}&token={}&success=true",user_json_str,token_res))
    // Redirect::to(format!("com.example.app://auth/callback?&success=false"))   
}


#[get("/auth/saml_handle")]
pub async fn auth_login(saml_attr: SamlAttr, db: &State<PgPool>) -> RawHtml<String> {
    info!("Init auth login - consolidated version");
    info!("{:?}", saml_attr.clone());

    // Check if required attributes are present
    if saml_attr.given_name.is_none() || saml_attr.sn.is_none() ||
       saml_attr.email.is_none() || saml_attr.idada.clone().is_none() {
        error!("Missing required SAML attributes");
        return RawHtml(r#"
            <html>
                <body>
                    <script>
                        window.location.href = "com.example.app://auth/callback?success=false";
                        setTimeout(function() {
                            window.close();
                        }, 100);
                    </script>
                </body>
            </html>
        "#.to_string());
    }

    let mut conn = match db.get().await {
        Ok(conn) => conn,
        Err(e) => {
            error!("Failed to connect to DB: {}", e);
            return RawHtml(r#"
                <html>
                    <body>
                        <script>
                            window.location.href = "com.example.app://auth/callback?success=false";
                            setTimeout(function() {
                                window.close();
                            }, 100);
                        </script>
                    </body>
                </html>
            "#.to_string());
        }
    };

    // Check if user exists
    let user_result = users::table
        .filter(users::idada.eq(saml_attr.idada.clone().unwrap()))
        .select(users::idada)
        .first::<String>(&mut conn)
        .await
        .optional();

    let user = match user_result {
        Ok(user) => user,
        Err(e) => {
            error!("Failed to fetch user's info: {}", e);
            return RawHtml(r#"
                <html>
                    <body>
                        <script>
                            window.location.href = "com.example.app://auth/callback?success=false";
                            setTimeout(function() {
                                window.close();
                            }, 100);
                        </script>
                    </body>
                </html>
            "#.to_string());
        }
    };

    match user {
        Some(u) => {
            // User exists - get user info and generate token
            let user_res = users::table
                .filter(users::idada.eq(u))
                .first::<User>(&mut conn)
                .await
                .optional()
                .expect("Query failed");

            if let Some(user) = user_res {
                let token = crate::utils::jwt_management::generate_jwt_tokens(
                    user.id.to_string().as_str(), 
                    user.mail.as_str()
                );
                
                if let Ok(token_res) = token {
                    let userinfo = UserInfo::new(
                        user.id,
                        user.name,
                        user.surname,
                        user.mail,
                        user.username,
                        user.telegram_username,
                        user.starred_routes,
                        user.auto
                    );
                    let user_json_str = json::serde_json::to_string(&userinfo)
                        .expect("Failed to parse retry login");
                    
                    info!("User fetched successfully");
                    
                    let redirect_url = format!(
                        "com.example.app://auth/callback?userinfo={}&token={}&success=true",
                        urlencoding::encode(&user_json_str),
                        urlencoding::encode(&token_res.access_token)
                    );
                    
                    return RawHtml(format!(r#"
                        <html>
                            <body>
                                <script>
                                    // Redirect to the app
                                    window.location.href = "{}";
                                    // Close the webview after a short delay
                                    setTimeout(function() {{
                                        window.close();
                                    }}, 100);
                                </script>
                            </body>
                        </html>
                    "#, redirect_url));
                } else {
                    error!("Token generation failed");
                    return RawHtml(r#"
                        <html>
                            <body>
                                <script>
                                    window.location.href = "com.example.app://auth/callback?success=false";
                                    setTimeout(function() {
                                        window.close();
                                    }, 100);
                                </script>
                            </body>
                        </html>
                    "#.to_string());
                }
            } else {
                error!("User not found after existence check");
                return RawHtml(r#"
                    <html>
                        <body>
                            <script>
                                window.location.href = "com.example.app://auth/callback?success=false";
                                setTimeout(function() {
                                    window.close();
                                }, 100);
                            </script>
                        </body>
                    </html>
                "#.to_string());
            }
        },
        None => {
            // User doesn't exist - create new user
            let mut id = rand::random_range(1..=i64::MAX);
            
            // Generate unique ID
            loop {
                let id_check = users::table
                    .find(id)
                    .select(users::id)
                    .first::<i64>(&mut conn)
                    .await
                    .optional()
                    .expect("Error in db query");
                
                if id_check.is_none() {
                    break;
                }
                id = rand::random_range(1..=i64::MAX);
            }

            // Create new user
            let new_user = User::new(
                id,
                saml_attr.given_name.unwrap(),
                saml_attr.sn.unwrap(),
                saml_attr.email.unwrap(),
                saml_attr.idada.clone().unwrap(),
            );

            let result = diesel::insert_into(users::table)
                .values(&new_user)
                .execute(&mut conn)
                .await;

            match result {
                Ok(_) => {
                    // After creating user, get user info and generate token
                    let user_res = users::table
                        .filter(users::idada.eq(saml_attr.idada.clone().unwrap()))
                        .first::<User>(&mut conn)
                        .await
                        .optional()
                        .expect("Query failed");

                    if let Some(user) = user_res {
                        let token = crate::utils::jwt_management::generate_jwt_tokens(
                            user.id.to_string().as_str(), 
                            user.mail.as_str()
                        );
                        
                        if let Ok(token_res) = token {
                            let userinfo = UserInfo::new(
                                user.id,
                                user.name,
                                user.surname,
                                user.mail,
                                user.username,
                                user.telegram_username,
                                user.starred_routes,
                                user.auto
                            );
                            let user_json_str = json::serde_json::to_string(&userinfo)
                                .expect("Failed to parse retry login");
                            
                            info!("User created successfully");
                            
                            let redirect_url = format!(
                                "com.example.app://auth/callback?userinfo={}&token={}&success=true",
                                urlencoding::encode(&user_json_str),
                                urlencoding::encode(&token_res.access_token)
                            );
                            
                            return RawHtml(format!(r#"
                                <html>
                                    <body>
                                        <script>
                                            // Redirect to the app
                                            window.location.href = "{}";
                                            // Close the webview after a short delay
                                            setTimeout(function() {{
                                                window.close();
                                            }}, 100);
                                        </script>
                                    </body>
                                </html>
                            "#, redirect_url));
                        } else {
                            error!("Token generation failed for new user");
                            return RawHtml(r#"
                                <html>
                                    <body>
                                        <script>
                                            window.location.href = "com.example.app://auth/callback?success=false";
                                            setTimeout(function() {
                                                window.close();
                                            }, 100);
                                        </script>
                                    </body>
                                </html>
                            "#.to_string());
                        }
                    } else {
                        error!("User not found after creation");
                        return RawHtml(r#"
                            <html>
                                <body>
                                    <script>
                                        window.location.href = "com.example.app://auth/callback?success=false";
                                        setTimeout(function() {
                                            window.close();
                                        }, 100);
                                    </script>
                                </body>
                            </html>
                        "#.to_string());
                    }
                },
                Err(e) => {
                    error!("Failed to create user: {}", e);
                    return RawHtml(r#"
                        <html>
                            <body>
                                <script>
                                    window.location.href = "com.example.app://auth/callback?success=false";
                                    setTimeout(function() {
                                        window.close();
                                    }, 100);
                                </script>
                            </body>
                        </html>
                    "#.to_string());
                }
            }
        }
    }
}


async fn handle_login(saml_attr: SamlAttr, db: &State<PgPool>)->Result<Redirect,Status> {
    let mut conn = db.get().await.expect("Failed to connect to DB");
    // if any of the required attributes is missing the login fails
    if saml_attr.given_name.is_none() || saml_attr.sn.is_none() ||
       saml_attr.email.is_none() || saml_attr.idada.clone().is_none() { 
        return Err(Status::BadRequest);
    }

    //check if user exist
    let user = users::table
        .filter(users::idada.eq(saml_attr.idada.clone().unwrap()))
        .select(users::idada)
        .first::<String>(&mut  conn)
        .await
        .optional()
        .expect("Failed to fetch user's info");

    match user {
        Some(u) => {
            return get_user_on_idada(u, db).await;  //calls the getter of user info + token
        },
        None => {
            let mut id = rand::random_range(1..=i64::MAX);
            //generate a new id that is unique 
            while let Some(_) = {
                users::table.find(id)
                    .select(users::id)
                    .first::<i64>(&mut  conn)
                    .await
                    .optional()
                    .expect("Error in db query")
            } {
                id = rand::random_range(1..=i64::MAX);
            }
            //create new request to create user with saml data
            let new_user = CreateUserRequest::new(id,saml_attr.given_name.unwrap(),saml_attr.sn.unwrap(),saml_attr.email.unwrap(),saml_attr.idada.clone().unwrap());
            let res = create_new_user(Json(new_user), db).await;
            if let Ok(_s) = res {
                return get_user_on_idada(saml_attr.idada.clone().unwrap(), db).await; // get userinfo + token with the id
            } else {
                Err(Status::BadRequest)
            }
        }
    }
}

#[post("/users", format = "application/json", data = "<user>")]
pub async fn create_new_user(
    user: Json<CreateUserRequest>,
    db: &State<PgPool>
) -> Result<Status, Status> {
    let mut conn = db.get().await.expect("Failed to connect to DB");
    
    let new_user = User::new(
        user.id,
        user.name.clone(),
        user.surname.clone(),
        user.mail.clone(),
        user.idada.clone(), 
    );

    let result = diesel::insert_into(users::table)
        .values(&new_user)
        .execute(&mut conn)
        .await;

    match result {
        Ok(_) => Ok(Status::Created),
        Err(e) => {
            eprintln!("{:?}",e);
            Err(Status::Conflict)
        }
    }
}


async fn get_user_on_idada(idada: String, db: &State<PgPool>) -> Result<Redirect,Status> {
    let mut conn = db.get().await.expect("Failed to connect to DB");
    
    let user_res = users::table
        .filter(users::idada.eq(idada))
        .first::<User>(&mut conn)
        .await
        .optional()
        .expect("Query failed");

    if let Some(user) = user_res {
        let token = crate::utils::jwt_management::generate_jwt_tokens(user.id.to_string().as_str(), user.mail.as_str());
        if let Ok(token_res) = token {
            let userinfo = UserInfo::new(user.id,user.name,user.surname,user.mail,user.username,user.telegram_username,user.starred_routes,user.auto);
            let user_json_str = json::serde_json::to_string(&userinfo).expect("Failed to parse retry login");
            Ok(Redirect::to(format!("https://mp.disi.unitn.it/blablauintn/api/test/redirect?userinfo={}&token={}&success=true",user_json_str,token_res.access_token)))
            // Ok(Json((userinfo,token_res)))
        } else {
            Err(Status::InternalServerError)    
        }
    } else {
        Err(Status::NotFound)
    }
}

#[get("/users/<id>", format="application/json")]
pub async fn get_user_on_id(id:i64, db: &State<PgPool>) -> Result<Json<(UserInfo,crate::utils::jwt_management::Tokens)>,Status> {
    let mut conn = db.get().await.expect("Failed to connect to DB");
    let user_res = users::table
        .filter(users::id.eq(id))
        .first::<User>(&mut conn)
        .await
        .optional()
        .expect("Query failed");
    if let Some(user) = user_res {
        let token = crate::utils::jwt_management::generate_jwt_tokens(user.id.to_string().as_str(), user.mail.as_str());
        let userinfo = UserInfo::new(user.id,user.name,user.surname,user.mail,user.username,user.telegram_username,user.starred_routes,user.auto);
        Ok(Json((userinfo,token.ok().unwrap())))
    } else {
        Err(Status::NotFound)
    }
}

#[get("/user_info", format="application/json")]
pub async fn get_user_info(claims: Claims, db: &State<PgPool>) -> Result<Json<UserInfo>,Status> {
    let mut conn = db.get().await.expect("Failed to connect to DB");
    match claims.sub.parse::<i64>().map_err(|_| Status::Unauthorized) {
        Ok(id) =>{
            let user_res = users::table
                .filter(users::id.eq(id))
                .first::<User>(&mut conn)
                .await
                .optional()
                .expect("Query failed");
            if let Some(user) = user_res {
                let userinfo = UserInfo::new(user.id,user.name,user.surname,user.mail,user.username,user.telegram_username,user.starred_routes,user.auto);
                Ok(Json(userinfo))
            } else {
                Err(Status::NotFound)
            }
        }
        Err(e) => Err(e)
    }
}

#[get("/get_username/<id>")]
pub async fn get_username(id: i64, db: &State<PgPool>,claims: Claims)->Result<String,Status> {
    let auth = claims.sub.parse::<i64>().map_err(|_| Status::Unauthorized);

    if let Ok(_) = auth {
        let mut conn = db.get().await.expect("Failed to cpnnect to DB");
        let name = users::table
            .find(id)
            .select(users::username)
            .first::<String>(&mut conn)
            .await
            .optional()
            .expect("Failed to fetch username");
        if let Some(n) = name {
            Ok(n)
        } else {
            Err(Status::NotFound)
        }
    } else {
        Err(Status::Unauthorized)
    }
}

#[get("/get_telegram_username/<id>")]
pub async fn get_telegram_username(id:i64, db: &State<PgPool>,claims: Claims)->Result<String,Status> {
    let auth = claims.sub.parse::<i64>().map_err(|_| Status::Unauthorized);

    if let Ok(_) = auth {
        let mut conn = db.get().await.expect("Failed to cpnnect to DB");
        let name = users::table
            .find(id)
            .select(users::telegram_username)
            .first::<Option<String>>(&mut conn)
            .await
            .expect("Failed to fetch telegram username");
        if let Some(n) = name {
            Ok(n)
        } else {
            Err(Status::NotFound)
        }
    } else {
        Err(Status::Unauthorized)
    }
}

#[get("/get_user_full_name/<id>")]
pub async fn get_user_full_name(id: i64, db: &State<PgPool>,claims: Claims)->Result<String,Status> {
    let auth = claims.sub.parse::<i64>().map_err(|_| Status::Unauthorized);

    if let Ok(_) = auth {
        let mut conn = db.get().await.expect("Failed to cpnnect to DB");
        let name = users::table
            .find(id)
            .select((users::name, users::surname))
            .first::<(String, String)>(&mut conn)
            .await
            .optional()
            .expect("Failed to get user name");
        if let Some((n,s)) = name {
            Ok(format!("{} {}",n.clone(),s.clone()))
        } else {
            Err(Status::NotFound)
        }
    } else {
        Err(Status::Unauthorized)
    }
}
//To remove
#[get("/users/get_request/<id>", format = "application/json")]
pub async fn get_req_on_id(id: i64, db: &State<PgPool>, claims: Claims) -> Result<Json<Vec<Request>>,Status> {
    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(id) = auth {
        let mut conn = db.get().await.expect("Failed to connect to DB");
    
        let requests = requests::table
            .filter(requests::passenger_id.eq(id))
            .load::<Request>(&mut conn)
            .await
            .expect("Query failed");
    
        println!("Found {} requests for passenger {}", requests.len(), id);
        Ok(Json(requests))
    } else {
        Err(Status::Unauthorized)
    }

}
//to remove
#[patch("/users/<id>/request/<session_id>")]
pub async fn resign_driver(id: i64, session_id: i64, db: &State<PgPool>,claims: Claims) -> Result<Status,Status>{
    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(id) = auth {
        let mut conn = db.get().await.expect("Failed to connect to DB");
    
        let _result = diesel::update(
            requests::table
                .filter(requests::driver_id.eq(id))
                .filter(requests::session_id.eq(session_id))
        )
        .set(requests::driver_id.eq(None::<i64>))
        .execute(&mut conn)
        .await
        .expect("Patch failed");

        if _result > 0 {
            Ok(Status::Ok)
        } else {
            Err(Status::InternalServerError)
        }
    } else {
        Err(Status::Unauthorized)
    }

}

#[derive(Debug,Serialize,Deserialize,Clone)]
pub struct RouteRequest {
    name: String,
    route: Route,
}


#[patch("/users/<id>/new_starred_route", format = "application/json", data = "<data>")]
pub async fn new_starred_route(
    id: i64, 
    mut data: Json<RouteRequest>, 
    db: &State<PgPool>,
    claims: Claims
) -> Result<Status, status::Custom<String>> {
    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(id) = auth {
        let mut conn = db.get().await.map_err(|e| {
            status::Custom(Status::InternalServerError, format!("Database connection failed: {}", e))
        })?;
    
        let mut user = users::table
            .find(id)
            .first::<User>(&mut conn)
            .await
            .map_err(|e| {
                status::Custom(Status::NotFound, format!("User not found: {}", e))
            })?;
    
        if data.name.is_empty() {
            let len = user.starred_routes.0.len();
            data.name = format!("Route {}", len + 1);
        }
    
        user.starred_routes.0.insert(data.name.clone(), data.route.clone());
    
        let result = diesel::update(users::table.find(id))
            .set(users::starred_routes.eq(&user.starred_routes))
            .execute(&mut conn)
            .await;
    
        match result {
            Ok(_) => Ok(Status::Ok),
            Err(e) => Err(status::Custom(
                Status::BadRequest, 
                format!("Route: {} could not be saved, because: {}", data.name, e)
            ))
        }
    } else {
        Err(status::Custom(Status::Unauthorized, "User not auht".to_string()))
    }

}


#[patch("/users/<id>/patch_route", format = "application/json", data = "<data>")]
pub async fn patch_route(
    id: i64, 
    data: Json<RouteRequest>, 
    db: &State<PgPool>,
    claims: Claims
) -> Result<Status, status::Custom<String>> {
    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(id) = auth {
        let mut conn = db.get().await.map_err(|e| {
            status::Custom(Status::InternalServerError, e.to_string())
        })?;
    
        let mut user = users::table
            .find(id)
            .first::<User>(&mut conn)
            .await
            .map_err(|e| {
                status::Custom(Status::NotFound, format!("User not found: {}", e))
            })?;
    
        let mut starred_routes = user.starred_routes.clone();
        starred_routes.0.remove(&data.name);
        starred_routes.0.insert(data.name.clone(), data.route.clone());
    
        diesel::update(users::table.find(id))
            .set(users::starred_routes.eq(&user.starred_routes.clone()))
            .execute(&mut conn)
            .await
            .map_err(|e| {
                status::Custom(Status::BadRequest, format!("Failed to update route '{}': {}", data.name, e))
            })?;
    
        Ok(Status::Ok)
    } else {
        Err(status::Custom(Status::Unauthorized, "User not auth".to_string()))
    }
}


#[patch("/users/<id>/patch_car", format = "application/json", data = "<req>")]
pub async fn modify_user_car(id: i64, req: Json<Auto>, db: &State<PgPool>,claims: Claims) -> Result<Status, Status> {
    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(id) = auth {
        let mut conn = db.get().await.map_err(|_| Status::InternalServerError)?;
    
        let result = diesel::update(users::table.find(id))
            .set(users::auto.eq(&req.0))
            .execute(&mut conn)
            .await;
    
        match result {
            Ok(rows_affected) if rows_affected > 0 => Ok(Status::Ok),
            Ok(_) => Err(Status::NotFound),
            Err(_) => Err(Status::BadRequest)
        }
    } else {
        Err(Status::Unauthorized)
    }

}

#[patch("/patch_username/<username>", format = "application/json")]
pub async fn modify_username(username: String, db: &State<PgPool>,claims: Claims) -> Result<Status, Status> {
    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(id) = auth {
        let mut conn = db.get().await.map_err(|_| Status::InternalServerError)?;
    
        let result = diesel::update(users::table.find(id))
            .set(users::username.eq(&username.clone()))
            .execute(&mut conn)
            .await;
    
        match result {
            Ok(rows_affected) if rows_affected > 0 => Ok(Status::Ok),
            Ok(_) => Err(Status::NotFound),
            Err(_) => Err(Status::BadRequest)
        }
    } else {
        Err(Status::Unauthorized)
    }

}

#[patch("/patch_telegram_username/<username>", format = "application/json")]
pub async fn modify_telegram_username(username: String, db: &State<PgPool>,claims: Claims) -> Result<Status, Status> {
    let auth = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest);

    if let Ok(id) = auth {
        let mut conn = db.get().await.map_err(|_| Status::InternalServerError)?;
        let mut result : Result<usize, diesel::result::Error> = Ok(0);
        if username.is_empty(){
            result = diesel::update(users::table.find(id))
                .set(users::telegram_username.eq::<Option<String>>(None))
                .execute(&mut conn)
                .await;
        }else {
            result = diesel::update(users::table.find(id))
                .set(users::telegram_username.eq(&Some(username.clone())))
                .execute(&mut conn)
                .await;        
        }
        match result {
            Ok(rows_affected) if rows_affected > 0 => Ok(Status::Ok),
            Ok(_) => Err(Status::NotFound),
            Err(_) => Err(Status::BadRequest)
        }
    } else {
        Err(Status::Unauthorized)
    }

}

#[delete("/delete_users_info")]
pub async fn delete_user(db: &State<PgPool>, claims: Claims) -> Result<Status, Status> {
    let user_id = claims.sub.parse::<i64>().map_err(|_| Status::BadRequest)?;
    
    let mut conn = db.get().await.map_err(|_| Status::InternalServerError)?;
    
    conn.transaction::<_, diesel::result::Error, _>(|conn| {
        Box::pin(async move {
            diesel::delete(
                ride_history::table
                    .filter(ride_history::passengers_id.eq(vec![user_id]))
            )
            .execute(conn)
            .await?;

            diesel::delete(
                ride_history::table
                    .filter(ride_history::driver_id.eq(user_id))
            )
            .execute(conn)
            .await?;

            diesel::delete(
                requests::table
                    .filter(requests::passenger_id.eq(user_id))
            )
            .execute(conn)
            .await?;

            diesel::update(
                requests::table
                    .filter(requests::driver_id.eq(user_id))
            ).set(requests::driver_id.eq(None::<i64>))
            .execute(conn)
            .await?;

            diesel::delete(
                offers::table
                    .filter(offers::driver_id.eq(user_id))
            )
            .execute(conn)
            .await?;

            let user_deleted = diesel::delete(
                users::table.find(user_id)
            )
            .execute(conn)
            .await?;

            if user_deleted == 0 {
                return Err(diesel::result::Error::NotFound);
            }

            Ok(())
        })
    })
    .await
    .map_err(|e| {
        eprintln!("Database error during user deletion: {}", e);
        match e {
            diesel::result::Error::NotFound => Status::NotFound,
            _ => Status::InternalServerError,
        }
    })?;

    Ok(Status::Ok)
}