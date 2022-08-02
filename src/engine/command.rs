use serde::{Deserialize, Serialize};

/// a struct which supports serialization and deserialization
#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    /// for set command
    SET(String, String),
    /// for rm command
    RM(String),
}
