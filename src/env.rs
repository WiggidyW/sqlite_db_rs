use crate::error::Error;
use env_util;
use std::{fmt::Display, time::Duration};

const URL: &str = "SQLITE_URL";
const MAX_CONNECTIONS: &str = "SQLITE_MAX_CONNECTIONS";
const MIN_CONNECTIONS: &str = "SQLITE_MIN_CONNECTIONS";
const ACQUIRE_TIMEOUT: &str = "SQLITE_ACQUIRE_TIMEOUT";
const MAX_LIFETIME: &str = "SQLITE_MAX_LIFETIME";
const IDLE_TIMEOUT: &str = "SQLITE_IDLE_TIMEOUT";

pub enum Url {
    Env(String),
    Default(&'static str),
}

impl AsRef<str> for Url {
    fn as_ref(&self) -> &str {
        match self {
            Url::Env(v) => v.as_ref(),
            Url::Default(v) => v,
        }
    }
}

pub fn url(namespace: impl Display, default: &'static str) -> Result<Url, Error> {
    let key = format!("{}_{}", namespace, URL);
    Ok(match env_util::get(&key).optional_checked()? {
        Some(v) => Url::Env(v.into_inner()),
        None => Url::Default(default),
    })
}

pub fn max_connections(namespace: impl Display, default: u32) -> Result<u32, Error> {
    let key = format!("{}_{}", namespace, MAX_CONNECTIONS);
    Ok(match env_util::get(&key).optional_checked()? {
        Some(v) => v.then_try_fromstr_into()?.into_inner(),
        None => default,
    })
}

pub fn min_connections(namespace: impl Display, default: u32) -> Result<u32, Error> {
    let key = format!("{}_{}", namespace, MIN_CONNECTIONS);
    Ok(match env_util::get(&key).optional_checked()? {
        Some(v) => v.then_try_fromstr_into()?.into_inner(),
        None => default,
    })
}

pub fn acquire_timeout(namespace: impl Display, default: Duration) -> Result<Duration, Error> {
    let key = format!("{}_{}", namespace, ACQUIRE_TIMEOUT);
    Ok(match env_util::get(&key).optional_checked()? {
        Some(v) => v
            .then_try_fromstr_into()?
            .then_fn_into(|p| Duration::from_secs(p))
            .into_inner(),
        None => default,
    })
}

pub fn max_lifetime(namespace: impl Display, default: Duration) -> Result<Duration, Error> {
    let key = format!("{}_{}", namespace, MAX_LIFETIME);
    Ok(match env_util::get(&key).optional_checked()? {
        Some(v) => v
            .then_try_fromstr_into()?
            .then_fn_into(|p| Duration::from_secs(p))
            .into_inner(),
        None => default,
    })
}

pub fn idle_timeout(namespace: impl Display, default: Duration) -> Result<Duration, Error> {
    let key = format!("{}_{}", namespace, IDLE_TIMEOUT);
    Ok(match env_util::get(&key).optional_checked()? {
        Some(v) => v
            .then_try_fromstr_into()?
            .then_fn_into(|p| Duration::from_secs(p))
            .into_inner(),
        None => default,
    })
}
