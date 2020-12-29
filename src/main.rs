use std::sync::Arc;
use rusqlite::{Connection, NO_PARAMS, OpenFlags, ToSql, params};
use juniper::{FieldResult, RootNode, graphql_object};
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Method, Response, Server, StatusCode,
};
use anyhow::{Context, Result, bail};

struct Database {
    conn: Connection
}

unsafe impl Sync for Database {}

impl juniper::Context for Database {}

impl Database {
    fn new() -> Result<Self> {
        Ok(Self {
            conn: Self::new_connection().context("Failed to set up database")?
        })
    }

    fn new_connection() -> Result<Connection> {
        let conn = Connection::open_with_flags(
            "store.db",
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_NO_MUTEX)?;
        let mode: String = conn.prepare("pragma journal_mode=WAL")?.query(NO_PARAMS)?.next()?.unwrap().get(0)?;
        println!("mode: {}", mode);
        // let _: i32 = conn.prepare("pragma mmap_size=268435456;")?.query(NO_PARAMS)?.next()?.unwrap().get(0)?;
        conn.execute(
            "create table if not exists events (
                id              integer primary key,
                store           text not null,
                timestamp       text not null,
                data            text not null,
                blob            blob
                );
            ",
            NO_PARAMS,
        )?;
        conn.execute("create index if not exists idx_events_store on events (store)", NO_PARAMS)?;
        conn.execute("create index if not exists idx_events_timestamp on events (timestamp)", NO_PARAMS)?;
        Ok(conn)
    }

    fn _add_index_for_json_key(&self, key: &str) -> Result<()> {
        for ch in key.chars() {
            if !ch.is_ascii_alphanumeric() && ch != '_' {
                bail!("JSON key must be alphanumeric");
            }
        }
        self.conn.execute(
            format!("create index if not exists idx_events_{0} on events (json_extract(data, '$.{0}'))", key).as_str(),
            NO_PARAMS,
        ).context("Failed to create index for a JSON key")?;
        Ok(())
    }

    fn add_event(&self, store: &str, data: &str, blob: Option<Vec<u8>>) -> Result<()> {
        self.conn.execute(
            "insert into events (store, timestamp, data, blob) values (
                ?1, strftime('%Y-%m-%d %H:%M:%f', 'now'), json(?2), ?3)",
            params![store, data, blob],
        ).context("Failed to add an event")?;
        Ok(())
    }

    fn delete_store(&self, name: &str) -> Result<()> {
        self.conn.execute(
            "delete from events where store == ?1",
            params!(name),
        ).context("Failed to delete store")?;
        Ok(())
    }

    fn count_rows_in_store(&self, store: &str) -> Result<i64> {
        let count: i64 = self.conn
            .prepare("select count(*) from events where store == ?1")?
            .query(params![store])?
            .next()?.expect("Failed to count rows in store").get(0)?;
        Ok(count)
    }

    fn events(&self, store: Option<String>, before: Option<String>, after: Option<String>, limit: Option<i32>, offset: i32, reverse: bool) -> Result<Vec<Event>> {
        let limit = limit.unwrap_or(-1);
        let mut where_clauses = vec![];
        let mut params = vec![
            (":limit", &limit as &dyn ToSql),
            (":offset", &offset as &dyn ToSql),
        ];
        if store.is_some() {
            where_clauses.push("store == :store");
            params.push((":store", &store as &dyn ToSql));
        }
        if before.is_some() {
            where_clauses.push("timestamp < strftime('%Y-%m-%d %H:%M:%f', :before)");
            params.push((":before", &before as &dyn ToSql));
        }
        if after.is_some() {
            where_clauses.push("timestamp > strftime('%Y-%m-%d %H:%M:%f', :after)");
            params.push((":after", &after as &dyn ToSql));
        }
        let joined_where = if where_clauses.is_empty() {
            "".to_string()
        } else {
            format!(" where {}", where_clauses.join(" and "))
        };

        let statement_string = format!("
            select * from events
            {}
            order by id {}
            limit :limit offset :offset",
            joined_where,
            if reverse { "desc" } else { "asc" });
        let mut statement = self.conn.prepare(&statement_string)?;
        let mut results = statement.query_named(params.as_slice())?;
        eprintln!("Executing {:?}", statement_string);
        let mut res = Vec::new();
        while let Some(row) = results.next()? {
            let event = Event {
                id: row.get(0)?,
                store: Store::new(row.get(1)?),
                timestamp: row.get(2)?,
                data: row.get(3)?,
                blob: row.get(4)?,
            };
            res.push(event);
        }
        Ok(res)
    }

    fn stores(&self, limit: Option<i32>, offset: i32) -> Result<Vec<Store>> {
        let mut statement = self.conn.prepare("select distinct store from events limit ?1 offset ?2")?;
        eprintln!("Executing {:?}", statement);
        let mut results = statement.query(params![limit.unwrap_or(-1), offset])?;
        let mut res = Vec::new();
        while let Some(row) = results.next()? {
            let store_name: String = row.get(0)?;
            res.push(Store::new(store_name));
        }
        Ok(res)
    }

    fn begin_transaction(&self) -> Result<()> {
        self.conn.execute("begin transaction", NO_PARAMS)?;
        Ok(())
    }

    fn commit_transaction(&self) -> Result<()> {
        self.conn.execute("commit transaction", NO_PARAMS)?;
        Ok(())
    }

    fn vacuum(&self) -> Result<()> {
        self.conn.execute("vacuum", NO_PARAMS)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct Store {
    name: String,
}

impl Store {
    fn new(name: String) -> Self {
        Self {
            name
        }
    }
}

#[graphql_object(Context = Database)]
impl Store {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn event_count(&self, context: &Database) -> FieldResult<String> {
        Ok(context.count_rows_in_store(&self.name)?.to_string())
    }

    #[graphql(arguments(after(default = None), before(default = None), limit(default = None), offset(default = 0), reverse(default = false)))]
    fn events(&self, after: Option<String>, before: Option<String>, limit: Option<i32>, offset: i32, reverse: bool, context: &Database) -> FieldResult<Vec<Event>> {
        Ok(context.events(Some(self.name.clone()), before, after, limit, offset, reverse)?)
    }
}

#[derive(Debug)]
struct Mutations {
}

impl Mutations {
    fn new() -> Self {
        Self { }
    }
}

#[graphql_object(Context = Database)]
impl Mutations {
    fn delete_store(&self, store: String, context: &Database) -> FieldResult<String> {
        context.delete_store(&store)?;
        Ok(format!("Successfully deleted store '{}'", store))
    }

    #[graphql(arguments(blob(default = None)))]
    fn add_event(&self, store: String, data: String, blob: Option<String>, context: &Database) -> FieldResult<String> {
        let blob = match blob {
            Some(encoded) => Some(base64::decode(encoded)?),
            None => None,
        };
        context.add_event(store.as_str(), data.as_str(), blob)?;
        Ok("Successfully added an event".to_string())
    }

    fn vacuum(&self, context: &Database) -> FieldResult<String> {
        context.commit_transaction()?;
        context.vacuum()?;
        context.begin_transaction()?;
        Ok("Successfully vacuumed the database".to_string())
    }
}

#[derive(Debug)]
struct Stores {}

impl Stores {
    fn new() -> Self {
        Self { }
    }
}

#[graphql_object(Context = Database)]
impl Stores {
    #[graphql(arguments(limit(default = None), offset(default = 0)))]
    fn stores(&self, limit: Option<i32>, offset: i32, context: &Database) -> FieldResult<Vec<Store>> {
        Ok(context.stores(limit, offset)?)
    }

    fn store(&self, name: String) -> Store {
        Store::new(name)
    }

    #[graphql(arguments(store(default = None), after(default = None), before(default = None), limit(default = None), offset(default = 0), reverse(default = false)))]
    fn events(&self, store: Option<String>, after: Option<String>, before: Option<String>, limit: Option<i32>, offset: i32, reverse: bool, context: &Database) -> FieldResult<Vec<Event>> {
        Ok(context.events(store, before, after, limit, offset, reverse)?)
    }
}

#[derive(Debug)]
struct Event {
    id: i64,
    store: Store,
    timestamp: String,
    data: String,
    blob: Option<Vec<u8>>,
}

#[graphql_object(Context = Database)]
impl Event {
    fn id(&self) -> String {
        self.id.to_string()
    }

    fn store(&self) -> Store {
        self.store.clone()
    }

    fn timestamp(&self) -> &str {
        self.timestamp.as_str()
    }

    fn data(&self) -> &str {
        self.data.as_str()
    }

    fn has_blob(&self) -> bool {
        self.blob.is_some()
    }

    fn blob_as_base64(&self) -> Option<String> {
        if let Some(blob) = &self.blob {
            Some(base64::encode(blob))
        } else {
            None
        }
    }
}

fn _add_test_events(count: usize) -> Result<()> {
    let db = Database::new()?;
    db.begin_transaction()?;
    for i in 0..count {
        if i % 100000 == 0 {
            println!(".");
        }
        db.add_event("test", "{}", None)?;
    }
    db.commit_transaction()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    //_add_test_events(10_000_000)?;
    let root_node = Arc::new(RootNode::new(Stores::new(), Mutations::new(), juniper::EmptySubscription::new()));
    let _db = Arc::new(Database::new()?);

    let juniper_service = make_service_fn(move |_| {
        let root_node = root_node.clone();

        async {
            Ok::<_, hyper::Error>(service_fn(move |req| {
                let root_node = root_node.clone();

                async {
                    match (req.method(), req.uri().path()) {
                        (&Method::GET, "/") => juniper_hyper::playground("/graphql", None).await,
                        (&Method::GET, "/graphql") | (&Method::POST, "/graphql") => {
                            let ctx = Arc::new(Database::new().unwrap());
                            ctx.begin_transaction().unwrap();
                            let res = juniper_hyper::graphql(root_node, ctx.clone(), req).await;
                            ctx.commit_transaction().unwrap();
                            res
                        }
                        _ => {
                            let mut response = Response::new(Body::empty());
                            *response.status_mut() = StatusCode::NOT_FOUND;
                            Ok(response)
                        }
                    }
                }
            }))
        }
    });

    let addr = ([127, 0, 0, 1], 3000).into();
    let server = Server::bind(&addr).serve(juniper_service);

    println!("Listening on http://{}", addr);

    if let Err(e) = server.await {
        eprintln!("server error: {}", e)
    }

    Ok(())
}
