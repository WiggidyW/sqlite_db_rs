use crate::{env, error::Error};
use futures::{Stream, TryStreamExt};
use sqlx::{
    self,
    pool::{PoolConnection, PoolOptions},
    query_as,
    sqlite::{Sqlite, SqliteArguments, SqliteConnectOptions, SqliteRow},
    Encode, FromRow, SqlitePool, Type,
};
use std::{fmt::Display, str::FromStr, time::Duration};

const DEFAULT_MAX_CONNECTIONS: u32 = 10;
const DEFAULT_MIN_CONNECTIONS: u32 = 0;
const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_LIFETIME: Duration = Duration::from_secs(30 * 60);
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const DEFAULT_TEST_BEFORE_ACQUIRE: bool = false;
const DEFAULT_URL: &str = "sqlite:db.sqlite";

pub struct Query<'q, O> {
    inner: sqlx::query::QueryAs<'q, Sqlite, O, SqliteArguments<'q>>,
}

impl<'q, O> Query<'q, O>
where
    O: for<'r> FromRow<'r, SqliteRow>,
{
    pub fn new(sql: &'q str) -> Self {
        Self {
            inner: query_as(sql),
        }
    }

    pub fn bind<P>(self, param: P) -> Self
    where
        P: 'q + Send + Encode<'q, Sqlite> + Type<Sqlite>,
    {
        Self {
            inner: self.inner.bind(param),
        }
    }
}

pub struct SqliteDb {
    inner: SqlitePool,
}

impl SqliteDb {
    pub fn new(namespace: impl Display) -> Result<Self, Error> {
        Ok(Self {
            inner: PoolOptions::new()
                .test_before_acquire(DEFAULT_TEST_BEFORE_ACQUIRE)
                .max_lifetime(env::max_lifetime(&namespace, DEFAULT_MAX_LIFETIME)?)
                .idle_timeout(env::idle_timeout(&namespace, DEFAULT_IDLE_TIMEOUT)?)
                .max_connections(env::max_connections(&namespace, DEFAULT_MAX_CONNECTIONS)?)
                .min_connections(env::min_connections(&namespace, DEFAULT_MIN_CONNECTIONS)?)
                .acquire_timeout(env::acquire_timeout(&namespace, DEFAULT_ACQUIRE_TIMEOUT)?)
                .connect_lazy_with(
                    SqliteConnectOptions::from_str(env::url(&namespace, DEFAULT_URL)?.as_ref())
                        .map_err(|e| Error::Initialize(e))?,
                ),
        })
    }

    pub async fn select_one<'q, O>(&self, query: Query<'q, O>) -> Result<Option<O>, Error>
    where
        O: for<'r> FromRow<'r, SqliteRow> + Send + Unpin,
    {
        query
            .inner
            .fetch_optional(&mut self.acquire().await?)
            .await
            .map_err(|e| Error::Fetch(e))
    }

    pub async fn select_all<'q, O>(&self, query: Query<'q, O>) -> Result<Vec<O>, Error>
    where
        O: for<'r> FromRow<'r, SqliteRow> + Send + Unpin,
    {
        query
            .inner
            .fetch_all(&mut self.acquire().await?)
            .await
            .map_err(|e| Error::Fetch(e))
    }

    pub async fn select_stream<'q, O>(
        &self,
        query: Query<'q, O>,
    ) -> Result<SelectStream<'q, O>, Error>
    where
        O: for<'r> FromRow<'r, SqliteRow> + Send + Unpin,
    {
        Ok(SelectStream {
            conn: self.acquire().await?,
            query: Some(query),
        })
    }

    async fn acquire(&self) -> Result<PoolConnection<Sqlite>, Error> {
        self.inner.acquire().await.map_err(|e| Error::Acquire(e))
    }
}

pub struct SelectStream<'q, O> {
    conn: PoolConnection<Sqlite>,
    query: Option<Query<'q, O>>,
}

impl<'q, O> SelectStream<'q, O>
where
    O: for<'r> FromRow<'r, SqliteRow> + Send + Unpin,
{
    pub fn stream(&mut self) -> Result<impl Stream<Item = Result<O, Error>> + Send + '_, Error> {
        Ok(self
            .query
            .take()
            .ok_or(Error::StreamExhausted)?
            .inner
            .fetch(&mut self.conn)
            .map_err(|e| Error::Fetch(e)))
    }
}
