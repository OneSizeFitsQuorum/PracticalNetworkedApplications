use serde::{Deserialize, Serialize};

/// a request struct which supports serialization and deserialization
#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    /// for set command
    SET(String, String),
    /// for rm command
    RM(String),
    /// for get command
    GET(String),
}

/// a response struct which supports serialization and deserialization
#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    /// for successful request
    Ok(Option<String>),
    /// for failed request
    Err(String),
}
