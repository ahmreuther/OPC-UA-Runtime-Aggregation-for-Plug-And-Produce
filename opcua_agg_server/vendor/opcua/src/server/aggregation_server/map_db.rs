use crate::client::prelude::ReferenceDescription;
use crate::server::aggregation_server::error_types::MappingError;
use crate::types::{BinaryEncoder, DecodingOptions, NodeId};
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef};
use rusqlite::{params, OptionalExtension, ToSql};
use std::fs;
use std::time::Duration;
use tracing::instrument;

#[derive(Debug)]
pub(crate) struct MapDatabasePool {
    pool: Pool<SqliteConnectionManager>,
}

#[derive(Debug)]
pub struct MapDatabaseConnection {
    conn: PooledConnection<SqliteConnectionManager>,
}

impl MapDatabasePool {
    #[instrument(level = "info", err, ret)]
    pub fn new() -> Result<Self, MappingError> {
        // let db_file_path = "./maps.db3";
        // if fs::metadata(db_file_path).is_ok() {
        //     // If the file exists, remove it
        //     if let Err(e) = fs::remove_file(db_file_path) {
        //         warn!("Error removing old db file: {:?}", e);
        //     };
        // }
        let manager = SqliteConnectionManager::memory();
        let pool_builder = r2d2::Pool::builder();
        let pool = pool_builder
            .max_size(2000)
            .min_idle(Some(10))
            .idle_timeout(Some(Duration::from_secs(60)))
            .build(manager)?;
        let conn = pool.get()?;
        conn.execute_batch(
            "BEGIN;
            PRAGMA foreign_keys = ON;
            CREATE TABLE lower_server (
                id                      INTEGER PRIMARY KEY NOT NULL,
                lserver_name            TINYTEXT NOT NULL,
                root_folder_node_id     VARBINARY NOT NULL
            );
            CREATE UNIQUE INDEX idx_lserver_name ON lower_server(lserver_name);
            CREATE UNIQUE INDEX idx_root_folder ON lower_server(root_folder_node_id);
            CREATE TABLE namespace_map (
                aggserver_nsid_inst     INTEGER PRIMARY KEY NOT NULL,
                aggserver_nsid_types    INTEGER,
                lserver_id              INTEGER NOT NULL,
                lserver_nsid            UNSIGNED SMALLINT NOT NULL,
                FOREIGN KEY(lserver_id) REFERENCES lower_server(id) ON DELETE CASCADE
            );
            CREATE UNIQUE INDEX idx_lserver_nsid ON namespace_map(lserver_id, lserver_nsid);
            CREATE TABLE reference_map (
                ref_id                  INTEGER PRIMARY KEY NOT NULL,
                from_node               VARBINARY NOT NULL,
                lserver_id              INTEGER NOT NULL,
                reference               VARBINARY NOT NULL,
                FOREIGN KEY(lserver_id) REFERENCES lower_server(id) ON DELETE CASCADE
            );
            CREATE INDEX idx_from_node ON reference_map(from_node);
            CREATE TABLE type_map (
                lserver_id              INTEGER NOT NULL,
                lserver_node_id         VARBINARY NOT NULL,
                aggserver_node_id       VARBINARY NOT NULL,
                PRIMARY KEY (lserver_id, lserver_node_id),
                FOREIGN KEY(lserver_id) REFERENCES lower_server(id) ON DELETE CASCADE
            );
            CREATE UNIQUE INDEX idx_aggserver_type ON type_map(lserver_id, aggserver_node_id);
            CREATE TABLE subscription_map (
                internal_sub_id         INTEGER PRIMARY KEY NOT NULL,
                lserver_id              INTEGER NOT NULL,
                lserver_sub_id          UNSIGNED MEDIUMINT NOT NULL,
                aggserver_sub_id        UNSIGNED MEDIUMINT NOT NULL,
                FOREIGN KEY(lserver_id) REFERENCES lower_server(id) ON DELETE CASCADE
            );
            CREATE UNIQUE INDEX idx_lserver_subscription ON subscription_map(lserver_id, lserver_sub_id);
            CREATE UNIQUE INDEX idx_aggserver_subscription ON subscription_map(lserver_id, aggserver_sub_id);
            CREATE TABLE monitored_item_map (
                internal_sub_id         INTEGER NOT NULL,
                lserver_mitem_id        UNSIGNED INTEGER NOT NULL,
                aggserver_mitem_id      UNSIGNED INTEGER NOT NULL,
                PRIMARY KEY (internal_sub_id, lserver_mitem_id),
                FOREIGN KEY (internal_sub_id) REFERENCES subscription_map(internal_sub_id) ON DELETE CASCADE
            );
            CREATE INDEX idx_aggserver_mitem ON monitored_item_map(internal_sub_id, aggserver_mitem_id);
            COMMIT;"
        )?;
        drop(conn);
        return Ok(Self { pool });
    }
}

impl MapDatabasePool {
    #[instrument(level = "debug", err, ret)]
    pub fn connect(&self) -> Result<MapDatabaseConnection, MappingError> {
        let conn = self.pool.get()?;
        return Ok(MapDatabaseConnection { conn });
    }
}

impl MapDatabaseConnection {
    /// returns lower server id
    #[instrument(level = "trace", err, ret)]
    pub fn insert_lserver(
        &self,
        lserver_name: &String,
        root_folder_nodeid: &NodeId,
    ) -> Result<u16, MappingError> {
        let mut insert_lserver = self.conn.prepare_cached(
            "INSERT INTO lower_server (lserver_name, root_folder_node_id) VALUES (?1, ?2);",
        )?;
        insert_lserver.execute(params![lserver_name, root_folder_nodeid])?;

        let mut get_lserver_id = self
            .conn
            .prepare_cached("SELECT id FROM lower_server WHERE lserver_name = ?1;")?;
        let id = get_lserver_id.query_row(params![lserver_name], |row| row.get(0))?;

        return Ok(id);
    }

    #[instrument(level = "trace", err, ret)]
    pub fn get_lservers(&self) -> Result<Vec<u16>, MappingError> {
        let mut get_lservers = self.conn.prepare_cached("SELECT id FROM lower_server;")?;
        let lservers: Vec<u16> = get_lservers
            .query_map([], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect();
        return Ok(lservers);
    }

    #[instrument(level = "trace", err, ret)]
    pub fn delete_lserver(&self, lserver_id: u16) -> Result<(), MappingError> {
        let mut delete_lserver = self
            .conn
            .prepare_cached("DELETE FROM lower_server WHERE id = ?1;")?;
        delete_lserver.execute([lserver_id])?;
        Ok(())
    }

    #[instrument(level = "trace", err, ret)]
    pub fn insert_namespace(
        &self,
        lserver_id: u16,
        aggserver_nsid_inst: u16,
        lserver_nsid: u16,
    ) -> Result<(), MappingError> {
        let mut insert_namespace = self.conn.prepare_cached(
            "INSERT INTO namespace_map (aggserver_nsid_inst, lserver_id, lserver_nsid) VALUES (?1, ?2, ?3);"
        )?;
        insert_namespace.execute(params![aggserver_nsid_inst, lserver_id, lserver_nsid])?;
        return Ok(());
    }

    #[instrument(level = "trace", err, ret)]
    pub fn add_aggserver_nsid_types(
        &self,
        lserver_id: u16,
        lserver_nsid: u16,
        aggserver_nsid_types: u16,
    ) -> Result<(), MappingError> {
        let mut insert_namespace = self.conn.prepare_cached(
            "UPDATE namespace_map SET aggserver_nsid_types = ?3 \
            WHERE lserver_id = ?1 AND lserver_nsid = ?2;",
        )?;
        insert_namespace.execute(params![lserver_id, lserver_nsid, aggserver_nsid_types])?;
        return Ok(());
    }

    #[instrument(level = "trace", err, ret)]
    pub fn insert_reference(
        &self,
        from_node: &NodeId,
        lserver_id: u16,
        reference: &ReferenceDescription,
    ) -> Result<(), MappingError> {
        let mut insert_reference = self.conn.prepare_cached(
            "INSERT INTO reference_map (from_node, lserver_id, reference) VALUES (?1, ?2, ?3);",
        )?;
        insert_reference.execute(params![from_node, lserver_id, reference,])?;
        return Ok(());
    }

    #[instrument(level = "trace", err, ret)]
    pub fn insert_type(
        &self,
        lserver_id: u16,
        lserver_type: &NodeId,
        aggserver_type: &NodeId,
    ) -> Result<(), MappingError> {
        let mut insert_type = self.conn.prepare_cached(
            "INSERT INTO type_map (lserver_id, lserver_node_id, aggserver_node_id) VALUES (?1, ?2, ?3);"
        )?;
        insert_type.execute(params![lserver_id, lserver_type, aggserver_type])?;
        return Ok(());
    }

    #[instrument(level = "trace", err, ret)]
    pub fn insert_subscription(
        &self,
        lserver_id: u16,
        lserver_sub_id: u32,
        aggserver_sub_id: u32,
    ) -> Result<(), MappingError> {
        let mut insert_subscription = self.conn.prepare_cached(
            "INSERT INTO subscription_map (lserver_id, lserver_sub_id, aggserver_sub_id)
            VALUES (?1, ?2, ?3);",
        )?;
        insert_subscription.execute(params![lserver_id, lserver_sub_id, aggserver_sub_id])?;
        return Ok(());
    }

    #[instrument(level = "trace", err, ret)]
    pub fn delete_subscription(&self, internal_sub_id: i64) -> Result<(), MappingError> {
        let mut delete_subscriptions = self
            .conn
            .prepare_cached("DELETE FROM subscription_map WHERE internal_sub_id = ?1;")?;
        delete_subscriptions.execute([internal_sub_id])?;
        return Ok(());
    }

    #[instrument(level = "trace", err, ret)]
    pub fn insert_monitored_item(
        &self,
        internal_sub_id: i64,
        lserver_mitem_id: u32,
        aggserver_mitem_id: u32,
    ) -> Result<(), MappingError> {
        let mut insert_monitored_item = self.conn.prepare_cached(
            "INSERT INTO monitored_item_map (internal_sub_id, lserver_mitem_id, aggserver_mitem_id) \
            VALUES (?1, ?2, ?3);"
        )?;
        insert_monitored_item.execute(params![
            internal_sub_id,
            lserver_mitem_id,
            aggserver_mitem_id
        ])?;
        return Ok(());
    }

    #[instrument(level = "trace", err, ret)]
    pub fn delete_monitored_item(
        &self,
        internal_sub_id: i64,
        lserver_mitem_id: u32,
    ) -> Result<(), MappingError> {
        let mut insert_monitored_item = self.conn.prepare_cached(
            "DELETE FROM monitored_item_map WHERE internal_sub_id = ?1 AND lserver_mitem_id = ?2;",
        )?;
        insert_monitored_item.execute(params![internal_sub_id, lserver_mitem_id])?;
        return Ok(());
    }

    #[instrument(level = "trace", err, ret)]
    pub fn delete_monitored_item_aggserver(
        &self,
        aggserver_sub_id: u32,
        aggserver_mitem_id: u32,
    ) -> Result<(), MappingError> {
        let mut delete_monitored_item_aggserver = self.conn.prepare_cached(
            "DELETE FROM monitored_item_map \
            WHERE ROWID IN ( \
                SELECT m.ROWID FROM monitored_item_map m \
                INNER JOIN subscription_map s \
                 ON (m.internal_sub_id = s.internal_sub_id) \
                WHERE aggserver_sub_id = ?1 AND aggserver_mitem_id = ?2 \
            );
            ",
        )?;
        delete_monitored_item_aggserver.execute(params![aggserver_sub_id, aggserver_mitem_id])?;
        return Ok(());
    }

    #[instrument(level = "trace", err, ret)]
    pub fn get_lserver_monitored_item(
        &self,
        aggserver_sub_id: u32,
        aggserver_mitem_id: u32,
    ) -> Result<Option<u32>, MappingError> {
        let mut get_lserver_monitored_item = self.conn.prepare_cached(
            "SELECT lserver_mitem_id FROM monitored_item_map \
             INNER JOIN subscription_map \
             ON monitored_item_map.internal_sub_id = subscription_map.internal_sub_id \
             WHERE aggserver_sub_id = ?1 AND aggserver_mitem_id = ?2;",
        )?;
        let lserver_item_id = get_lserver_monitored_item
            .query_row(params![aggserver_sub_id, aggserver_mitem_id], |row| {
                row.get(0)
            })
            .optional()?;
        return Ok(lserver_item_id);
    }

    #[instrument(level = "trace", err, ret)]
    pub fn get_aggserver_monitored_item(
        &self,
        aggserver_sub_id: u32,
        lserver_mitem_id: u32,
    ) -> Result<Option<u32>, MappingError> {
        let mut get_aggserver_monitored_item = self.conn.prepare_cached(
            "SELECT aggserver_mitem_id FROM subscription_map \
             INNER JOIN monitored_item_map \
             ON subscription_map.internal_sub_id = monitored_item_map.internal_sub_id \
             WHERE aggserver_sub_id = ?1 AND lserver_mitem_id = ?2;",
        )?;
        let aggserver_item_id = get_aggserver_monitored_item
            .query_row(params![aggserver_sub_id, lserver_mitem_id], |row| {
                row.get(0)
            })
            .optional()?;
        return Ok(aggserver_item_id);
    }

    #[instrument(level = "trace", err, ret)]
    pub fn get_lserver_by_monitored_item(
        &self,
        aggserver_sub_id: u32,
        aggserver_mitem_id: u32,
    ) -> Result<Option<u16>, MappingError> {
        let mut get_lserver_by_monitored_item = self.conn.prepare_cached(
            "SELECT lserver_id FROM subscription_map \
             INNER JOIN monitored_item_map \
             ON subscription_map.internal_sub_id = monitored_item_map.internal_sub_id \
             WHERE aggserver_sub_id = ?1 AND aggserver_mitem_id = ?2;",
        )?;
        let lserver_id = get_lserver_by_monitored_item
            .query_row(params![aggserver_sub_id, aggserver_mitem_id], |row| {
                row.get(0)
            })
            .optional()?;
        return Ok(lserver_id);
    }

    #[instrument(level = "trace", err, ret)]
    pub fn get_aggserver_type(
        &self,
        lserver_id: u16,
        lserver_type: &NodeId,
    ) -> Result<Option<NodeId>, MappingError> {
        let mut get_aggserver_type = self.conn.prepare_cached(
            "SELECT aggserver_node_id FROM type_map WHERE lserver_id = ?1 AND lserver_node_id = ?2;"
        )?;
        let aggserver_type = get_aggserver_type
            .query_row(params![lserver_id, lserver_type], |row| row.get(0))
            .optional()?;
        return Ok(aggserver_type);
    }

    #[instrument(level = "trace", err, ret)]
    pub fn get_lserver_type(
        &self,
        lserver_id: u16,
        aggserver_type: &NodeId,
    ) -> Result<NodeId, MappingError> {
        let mut get_aggserver_type = self.conn.prepare_cached(
            "SELECT lserver_node_id FROM type_map WHERE lserver_id = ?1 AND aggserver_node_id = ?2;"
        )?;
        let lserver_type =
            get_aggserver_type.query_row(params![lserver_id, aggserver_type], |row| row.get(0))?;
        return Ok(lserver_type);
    }

    #[instrument(level = "trace", err, ret)]
    pub fn get_aggserver_nsid_inst(
        &self,
        lserver_id: u16,
        lserver_nsid: u16,
    ) -> Result<u16, MappingError> {
        let mut get_aggserver_nsid_inst = self.conn.prepare_cached(
            "SELECT aggserver_nsid_inst FROM namespace_map \
            WHERE lserver_id = ?1 AND lserver_nsid = ?2;",
        )?;
        let aggserver_nsid_inst = get_aggserver_nsid_inst
            .query_row(params![lserver_id, lserver_nsid], |row| row.get(0))?;
        return Ok(aggserver_nsid_inst);
    }

    #[instrument(level = "trace", err, ret)]
    pub fn get_aggserver_nsid_types(
        &self,
        lserver_id: u16,
        lserver_nsid: u16,
    ) -> Result<u16, MappingError> {
        let mut get_aggserver_nsid_types = self.conn.prepare_cached(
            "SELECT aggserver_nsid_types FROM namespace_map \
            WHERE lserver_id = ?1 AND lserver_nsid = ?2;",
        )?;
        let aggserver_nsid_types = get_aggserver_nsid_types
            .query_row(params![lserver_id, lserver_nsid], |row| row.get(0))?;
        return Ok(aggserver_nsid_types);
    }

    #[instrument(level = "trace", err, ret)]
    pub fn get_lserver_nsid(
        &self,
        aggserver_nsid_inst: u16,
    ) -> Result<LowerServerNamespace, MappingError> {
        let mut get_lserver_nsid = self.conn.prepare_cached(
            "SELECT lserver_id, lserver_nsid FROM namespace_map \
            WHERE aggserver_nsid_inst = ?1;",
        )?;
        let (lserver_id, lserver_nsid) = get_lserver_nsid
            .query_row([aggserver_nsid_inst], |row| Ok((row.get(0)?, row.get(1)?)))?;
        return Ok(LowerServerNamespace {
            id: lserver_id,
            namespace: lserver_nsid,
        });
    }

    #[instrument(level = "trace", err, ret)]
    pub fn get_references(
        &self,
        from_node: &NodeId,
    ) -> Result<Vec<ReferenceDescription>, MappingError> {
        let mut get_references = self.conn.prepare_cached(
            "SELECT reference FROM reference_map \
            WHERE from_node = ?1",
        )?;
        let references_map = get_references
            .query_map(params![from_node], |row| row.get(0))
            .optional()?;

        return match references_map {
            Some(mr) => {
                let vr: Vec<Result<ReferenceDescription, rusqlite::Error>> = mr.collect();
                let rv: Result<Vec<ReferenceDescription>, rusqlite::Error> =
                    vr.into_iter().collect();
                Ok(rv?)
            }
            None => Ok(Vec::new()),
        };
    }

    #[instrument(level = "trace", err, ret)]
    pub fn get_lserver_by_root_folder(
        &self,
        root_folder_node_id: &NodeId,
    ) -> Result<Option<u16>, MappingError> {
        let mut get_lserver_by_root_folder = self
            .conn
            .prepare_cached("SELECT id FROM lower_server WHERE root_folder_node_id = ?1;")?;
        let lserver_id = get_lserver_by_root_folder
            .query_row([root_folder_node_id], |row| row.get(0))
            .optional()?;
        return Ok(lserver_id);
    }

    #[instrument(level = "trace", err, ret)]
    pub fn get_lserver_by_namespace(
        &self,
        aggserver_nsid_inst: &u16,
    ) -> Result<Option<u16>, MappingError> {
        let mut get_lserver_by_namespace = self.conn.prepare_cached(
            "SELECT lserver_id FROM namespace_map WHERE aggserver_nsid_inst = ?1;",
        )?;
        let lserver_id = get_lserver_by_namespace
            .query_row([aggserver_nsid_inst], |row| row.get(0))
            .optional()?;
        return Ok(lserver_id);
    }

    #[instrument(level = "trace", err, ret)]
    pub fn contains_root_folder(&self, folder: &NodeId) -> Result<bool, MappingError> {
        let mut get_lserver_by_root_folder = self
            .conn
            .prepare_cached("SELECT id FROM lower_server WHERE root_folder_node_id = ?1;")?;
        let lserver_id: Option<i64> = get_lserver_by_root_folder
            .query_row([folder], |row| row.get(0))
            .optional()?;
        return match lserver_id {
            Some(_id) => Ok(true),
            None => Ok(false),
        };
    }

    #[instrument(level = "trace", err, ret)]
    pub fn get_lserver_sub_id(
        &self,
        lserver_id: u16,
        aggserver_sub_id: u32,
    ) -> Result<Subscription, MappingError> {
        let mut get_internal_sub_id_by_aggserver = self.conn.prepare_cached(
            "SELECT internal_sub_id, lserver_sub_id FROM subscription_map WHERE lserver_id = ?1 AND aggserver_sub_id = ?2;"
        )?;
        let (internal_sub_id, lserver_sub_id) = get_internal_sub_id_by_aggserver
            .query_row(params![lserver_id, aggserver_sub_id], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?;
        return Ok(Subscription {
            internal_sub_id,
            lserver_sub_id,
            aggserver_sub_id,
        });
    }

    #[instrument(level = "trace", err, ret)]
    pub fn get_aggserver_mitem_id(
        &self,
        internal_sub_id: i64,
        lserver_mitem_id: u32,
    ) -> Result<u32, MappingError> {
        let mut get_aggserver_mitem_id = self.conn.prepare_cached(
            "SELECT aggserver_mitem_id FROM monitored_item_map \
            WHERE internal_sub_id = ?1 AND lserver_mitem_id = ?2;",
        )?;
        let aggserver_mitem_id = get_aggserver_mitem_id
            .query_row(params![internal_sub_id, lserver_mitem_id], |row| row.get(0))?;
        return Ok(aggserver_mitem_id);
    }
}

impl ToSql for ReferenceDescription {
    #[instrument(level = "trace", err, ret)]
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let binary = self.encode_to_vec();
        return Ok(ToSqlOutput::from(binary));
    }
}

impl FromSql for ReferenceDescription {
    #[instrument(level = "trace", err, ret)]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let res = ReferenceDescription::decode(&mut value.as_blob()?, &DecodingOptions::minimal());
        return match res {
            Ok(reference_description) => Ok(reference_description),
            Err(_e) => Err(FromSqlError::InvalidType),
        };
    }
}

impl ToSql for NodeId {
    #[instrument(level = "trace", err, ret)]
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let binary = self.encode_to_vec();
        return Ok(ToSqlOutput::from(binary));
    }
}

impl FromSql for NodeId {
    #[instrument(level = "trace", err, ret)]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let res = NodeId::decode(&mut value.as_blob()?, &DecodingOptions::minimal());
        return match res {
            Ok(node_id) => Ok(node_id),
            Err(_e) => Err(FromSqlError::InvalidType),
        };
    }
}

#[derive(Debug)]
pub struct LowerServerNamespace {
    pub id: u16,
    pub namespace: u16,
}

#[derive(Debug)]
pub struct Subscription {
    pub internal_sub_id: i64,
    pub lserver_sub_id: u32,
    pub aggserver_sub_id: u32,
}
