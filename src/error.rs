use env_util;
use sqlx;
use std::{error::Error as StdError, fmt::Display};

#[derive(Debug)]
pub enum Error {
    Env(env_util::Error),
    Initialize(sqlx::Error),
    Acquire(sqlx::Error),
    Fetch(sqlx::Error),
    StreamExhausted,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Env(e) => e.fmt(f),
            Error::Initialize(e) => write!(f, "Error initializing SqliteDb: {}", e,),
            Error::Acquire(e) => write!(f, "Error acquiring SqliteDb connection: {}", e,),
            Error::Fetch(e) => write!(f, "Error fetching rows from SqliteDb: {}", e,),
            Error::StreamExhausted => {
                write!(f, "Error fetching rows from SqliteDb: Stream Exhausted",)
            }
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Error::Env(e) => Some(e),
            Error::Initialize(e) => Some(e),
            Error::Acquire(e) => Some(e),
            Error::Fetch(e) => Some(e),
            Error::StreamExhausted => None,
        }
    }
}

impl From<env_util::Error> for Error {
    fn from(e: env_util::Error) -> Self {
        Error::Env(e)
    }
}
