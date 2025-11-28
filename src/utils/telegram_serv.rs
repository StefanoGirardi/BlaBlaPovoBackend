use rocket::serde::json::Json;
use rocket::{post, get, State};
use teloxide::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelegramUser {
    pub user_id: i64,
    pub username: Option<String>,
    pub chat_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatRequest {
    pub initiator_id: i64,
    pub target_id: i64,
    pub initiator_username: Option<String>,
    pub target_username: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatResponse {
    pub success: bool,
    pub message: String,
    pub deep_link: Option<String>,
}

pub struct TelegramService {
    bot: Bot,
    user_sessions: Arc<Mutex<HashMap<i64, TelegramUser>>>,
}

impl TelegramService {
    pub fn new(bot_token: String) -> Self {
        Self {
            bot: Bot::new(bot_token),
            user_sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn start_bot(&self) {
        let bot = self.bot.clone();
        let user_sessions = self.user_sessions.clone();
        
        tokio::spawn(async move {
            teloxide::repl(bot, move |bot: Bot, msg: Message| {
                let user_sessions = user_sessions.clone();
                async move {
                    if let Some(user) = msg.from() {
                        let mut sessions = user_sessions.lock().await;
                        sessions.insert(
                            user.id.0 as i64,
                            TelegramUser {
                                user_id: user.id.0 as i64,
                                username: user.username.clone(),
                                chat_id: Some(msg.chat.id.0),
                            },
                        );
                        
                        bot.send_message(msg.chat.id, "🤖 Bot is ready! You can now receive connection requests.")
                            .await?;
                    }
                    Ok(())
                }
            }).await;
        });
    }

    pub async fn initiate_chat(&self, request: ChatRequest) -> ChatResponse {
        let sessions = self.user_sessions.lock().await;
        
        let initiator_session = sessions.get(&request.initiator_id);
        let target_session = sessions.get(&request.target_id);

        match (initiator_session, target_session) {
            (Some(initiator), Some(target)) => {
                self.send_chat_invitation(initiator, target, &request).await
            }
            _ => {
                self.create_direct_deep_link(&request).await
            }
        }
    }

    async fn send_chat_invitation(
        &self,
        _initiator: &TelegramUser,
        target: &TelegramUser,
        request: &ChatRequest,
    ) -> ChatResponse {
        if let Some(target_chat_id) = target.chat_id {
            let deep_link = self.generate_deep_link(request).await;

            let message = format!(
                "👋 Connection request from {}!\n\nClick here to start chatting:\n{}\n\nOr copy this link: {}",
                request.initiator_username.as_deref().unwrap_or("User"),
                deep_link,
                deep_link
            );

            if let Err(e) = self.bot
                .send_message(ChatId(target_chat_id), message)
                .await
            {
                return ChatResponse {
                    success: false,
                    message: format!("Failed to send invitation: {}", e),
                    deep_link: None,
                };
            }

            ChatResponse {
                success: true,
                message: "Invitation sent successfully!".to_string(),
                deep_link: Some(deep_link),
            }
        } else {
            self.create_direct_deep_link(request).await
        }
    }

    async fn generate_deep_link(&self, request: &ChatRequest) -> String {
        if let (Some(_initiator_username), Some(target_username)) = 
            (&request.initiator_username, &request.target_username) 
        {
            format!("https://t.me/{}", target_username)
        } else {
            let bot_username = self.bot.get_me().await.unwrap().user.username.unwrap();
            format!(
                "https://t.me/{}?start=chat_{}_{}",
                bot_username,
                request.initiator_id,
                request.target_id
            )
        }
    }

    async fn create_direct_deep_link(&self, request: &ChatRequest) -> ChatResponse {
        let deep_link = self.generate_deep_link(request).await;

        ChatResponse {
            success: true,
            message: "Use the provided link to start chatting on Telegram".to_string(),
            deep_link: Some(deep_link),
        }
    }
}

#[post("/telegram/initiate-chat", format = "json", data = "<request>")]
pub async fn initiate_telegram_chat(
    telegram_service: &State<TelegramService>,
    request: Json<ChatRequest>,
) -> Json<ChatResponse> {
    let response = telegram_service.initiate_chat(request.0).await;
    Json(response)
}

#[get("/telegram/status")]
async fn get_bot_status(
    telegram_service: &State<TelegramService>,
) -> Json<rocket::serde::json::Value> {
    Json(rocket::serde::json::json!({
        "status": "running",
        "service": "telegram_bot"
    }))
}
//
#[get("/telegram/sessions")]
async fn get_user_sessions(
    telegram_service: &State<TelegramService>,
) -> Json<rocket::serde::json::Value> {
    // TODO: expose sessions
    Json(rocket::serde::json::json!({
        "sessions_count": 0, //
        "message": "Sessions endpoint"
    }))
}