use crate::client::prelude::ReferenceDescription;
use crate::server::aggregation_server::error_types::MappingError;
use crate::types::{BinaryEncoder, DecodingOptions, MonitoredItemCreateResult, NodeId};
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
        let manager = SqliteConnectionManager::memory()
            .with_init(|conn| conn.execute_batch("PRAGMA foreign_keys = ON;"));
        let pool_builder = r2d2::Pool::builder();
        let pool = pool_builder
            .max_size(2000)
            .min_idle(Some(10))
            .idle_timeout(Some(Duration::from_secs(60)))
            .build(manager)?;
        let conn = pool.get()?;
        conn.execute_batch(
            "BEGIN;
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
    pub(crate) fn active_lower_server_names(
        &self,
    ) -> Result<std::collections::HashMap<u16, String>, MappingError> {
        let mut statement = self
            .conn
            .prepare("SELECT id, lserver_name FROM lower_server")?;
        let entries = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        Ok(entries.collect::<Result<_, _>>()?)
    }

    pub(crate) fn lower_server_id_by_name(&self, name: &str) -> Result<Option<u16>, MappingError> {
        Ok(self
            .conn
            .query_row(
                "SELECT id FROM lower_server WHERE lserver_name = ?1",
                [name],
                |row| row.get(0),
            )
            .optional()?)
    }

    pub(crate) fn lower_server_debug_counts(
        &self,
        name: &str,
    ) -> Result<super::aggregation_server::LowerServerDebugCounts, MappingError> {
        let mut counts = super::aggregation_server::LowerServerDebugCounts::default();
        let Some(id) = self.lower_server_id_by_name(name)? else {
            return Ok(counts);
        };
        counts.lower_servers = 1;
        // Table names are constants; the source identifier is always bound.
        let count = |table: &str| -> Result<usize, MappingError> {
            Ok(self.conn.query_row(
                &format!("SELECT COUNT(*) FROM {table} WHERE lserver_id = ?1"),
                [id],
                |row| row.get(0),
            )?)
        };
        counts.namespace_mappings = count("namespace_map")?;
        counts.type_mappings = count("type_map")?;
        counts.references = count("reference_map")?;
        counts.subscriptions = count("subscription_map")?;
        counts.monitored_items = self.conn.query_row(
            "SELECT COUNT(*) FROM monitored_item_map WHERE internal_sub_id IN \
             (SELECT internal_sub_id FROM subscription_map WHERE lserver_id = ?1)",
            [id],
            |row| row.get(0),
        )?;
        Ok(counts)
    }

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

    /// Keep upper item routing available while rejecting old source IDs. All
    /// subscriptions being recreated are invalidated before any ID is reused.
    pub fn begin_subscription_recreation(
        &self,
        lserver_id: u16,
        aggserver_sub_id: u32,
    ) -> Result<(), MappingError> {
        let changed = self.conn.execute(
            "UPDATE subscription_map SET lserver_sub_id = -internal_sub_id              WHERE lserver_id = ?1 AND aggserver_sub_id = ?2;",
            params![lserver_id, aggserver_sub_id],
        )?;
        if changed == 0 {
            return Err(rusqlite::Error::QueryReturnedNoRows.into());
        }
        Ok(())
    }

    /// Replace source IDs atomically, retaining the upper IDs returned to clients.
    /// Failed source item recreations lose their stale mapping. A transaction
    /// allows IDs to swap without colliding with another old monitored item.
    pub fn remap_recreated_subscription(
        &self,
        lserver_id: u16,
        aggserver_sub_id: u32,
        new_lserver_sub_id: u32,
        items: &[(u32, MonitoredItemCreateResult)],
    ) -> Result<(), MappingError> {
        let transaction = self.conn.unchecked_transaction()?;
        let internal_sub_id: i64 = transaction.query_row(
            "SELECT internal_sub_id FROM subscription_map              WHERE lserver_id = ?1 AND aggserver_sub_id = ?2;",
            params![lserver_id, aggserver_sub_id], |row| row.get(0),
        )?;
        let old_items: std::collections::HashMap<u32, u32> = {
            let mut statement = transaction.prepare(
                "SELECT lserver_mitem_id, aggserver_mitem_id FROM monitored_item_map                  WHERE internal_sub_id = ?1;",
            )?;
            let rows =
                statement.query_map([internal_sub_id], |row| Ok((row.get(0)?, row.get(1)?)))?;
            rows.collect::<rusqlite::Result<_>>()?
        };
        transaction.execute(
            "DELETE FROM monitored_item_map WHERE internal_sub_id = ?1;",
            [internal_sub_id],
        )?;
        for (old_id, result) in items {
            if !result.status_code.is_good() {
                continue;
            }
            let Some(upper_id) = old_items.get(old_id) else {
                continue;
            };
            transaction.execute(
                "INSERT INTO monitored_item_map (internal_sub_id, lserver_mitem_id, aggserver_mitem_id)                  VALUES (?1, ?2, ?3);",
                params![internal_sub_id, result.monitored_item_id, upper_id],
            )?;
        }
        transaction.execute(
            "UPDATE subscription_map SET lserver_sub_id = ?1 WHERE internal_sub_id = ?2;",
            params![new_lserver_sub_id, internal_sub_id],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Delete by stable owner keys, including an invalidated source ID during reconnect.
    pub fn delete_subscription_context(
        &self,
        lserver_id: u16,
        aggserver_sub_id: u32,
    ) -> Result<(), MappingError> {
        self.conn.execute(
            "DELETE FROM subscription_map WHERE lserver_id = ?1 AND aggserver_sub_id = ?2;",
            params![lserver_id, aggserver_sub_id],
        )?;
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn foreign_keys_are_enabled_on_every_pool_connection() {
        let pool = MapDatabasePool::new().unwrap();
        // Hold more than min_idle connections to also check connections created
        // after the schema has been initialized.
        let connections = (0..11).map(|_| pool.connect().unwrap()).collect::<Vec<_>>();
        for connection in connections {
            let enabled: i64 = connection
                .conn
                .query_row("PRAGMA foreign_keys;", [], |row| row.get(0))
                .unwrap();
            assert_eq!(enabled, 1);
        }
    }

    #[test]
    fn deleting_subscriptions_and_sources_cascades_to_monitored_item_maps() {
        let pool = MapDatabasePool::new().unwrap();
        let writer = pool.connect().unwrap();
        let deleter = pool.connect().unwrap();
        let lserver_id = writer
            .insert_lserver(&"source".to_string(), &NodeId::new(1, 100))
            .unwrap();
        writer.insert_namespace(lserver_id, 2, 1).unwrap();
        writer.insert_subscription(lserver_id, 10, 20).unwrap();
        let subscription = writer.get_lserver_sub_id(lserver_id, 20).unwrap();
        writer
            .insert_monitored_item(subscription.internal_sub_id, 30, 40)
            .unwrap();

        deleter
            .delete_subscription(subscription.internal_sub_id)
            .unwrap();
        assert!(writer.get_lserver_monitored_item(20, 40).unwrap().is_none());
        let item_count: i64 = writer
            .conn
            .query_row("SELECT COUNT(*) FROM monitored_item_map;", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(item_count, 0);

        writer.insert_subscription(lserver_id, 11, 21).unwrap();
        let subscription = writer.get_lserver_sub_id(lserver_id, 21).unwrap();
        writer
            .insert_monitored_item(subscription.internal_sub_id, 31, 41)
            .unwrap();
        deleter.delete_lserver(lserver_id).unwrap();
        for table in [
            "lower_server",
            "namespace_map",
            "subscription_map",
            "monitored_item_map",
        ] {
            let count: i64 = writer
                .conn
                .query_row(&format!("SELECT COUNT(*) FROM {table};"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 0, "stale entries in {table}");
        }
    }
}

#[cfg(test)]
mod reconnect_tests {
    use super::*;
    use crate::types::{ExtensionObject, StatusCode};

    fn created(id: u32) -> MonitoredItemCreateResult {
        MonitoredItemCreateResult {
            status_code: StatusCode::Good,
            monitored_item_id: id,
            revised_sampling_interval: 100.0,
            revised_queue_size: 1,
            filter_result: ExtensionObject::null(),
        }
    }

    #[test]
    fn reconnect_swaps_source_ids_and_keeps_upper_handles_and_routing() {
        let pool = MapDatabasePool::new().unwrap();
        let db = pool.connect().unwrap();
        let source = db
            .insert_lserver(&"source".to_string(), &NodeId::new(1, 1))
            .unwrap();
        db.insert_subscription(source, 10, 100).unwrap();
        db.insert_subscription(source, 20, 200).unwrap();
        let first = db.get_lserver_sub_id(source, 100).unwrap();
        let second = db.get_lserver_sub_id(source, 200).unwrap();
        db.insert_monitored_item(first.internal_sub_id, 1, 101)
            .unwrap();
        db.insert_monitored_item(first.internal_sub_id, 2, 102)
            .unwrap();
        db.insert_monitored_item(first.internal_sub_id, 3, 103)
            .unwrap();
        db.insert_monitored_item(second.internal_sub_id, 5, 201)
            .unwrap();

        db.begin_subscription_recreation(source, 100).unwrap();
        db.begin_subscription_recreation(source, 200).unwrap();
        assert!(db.get_lserver_sub_id(source, 100).is_err());
        assert_eq!(
            db.get_lserver_by_monitored_item(100, 101).unwrap(),
            Some(source)
        );
        let mut failure = created(0);
        failure.status_code = StatusCode::BadNodeIdUnknown;
        db.remap_recreated_subscription(
            source,
            100,
            20,
            &[(1, created(2)), (2, created(1)), (3, failure)],
        )
        .unwrap();
        db.remap_recreated_subscription(source, 200, 10, &[(5, created(9))])
            .unwrap();

        assert_eq!(
            db.get_lserver_sub_id(source, 100).unwrap().lserver_sub_id,
            20
        );
        assert_eq!(
            db.get_lserver_sub_id(source, 200).unwrap().lserver_sub_id,
            10
        );
        assert_eq!(db.get_lserver_monitored_item(100, 101).unwrap(), Some(2));
        assert_eq!(db.get_lserver_monitored_item(100, 102).unwrap(), Some(1));
        assert_eq!(db.get_lserver_monitored_item(100, 103).unwrap(), None);
        assert_eq!(db.get_lserver_monitored_item(200, 201).unwrap(), Some(9));
    }

    #[test]
    fn failed_reconnect_remap_rolls_back_for_retry() {
        let pool = MapDatabasePool::new().unwrap();
        let db = pool.connect().unwrap();
        let source = db
            .insert_lserver(&"source".to_string(), &NodeId::new(1, 1))
            .unwrap();
        db.insert_subscription(source, 10, 100).unwrap();
        let sub = db.get_lserver_sub_id(source, 100).unwrap();
        db.insert_monitored_item(sub.internal_sub_id, 1, 101)
            .unwrap();
        db.insert_monitored_item(sub.internal_sub_id, 2, 102)
            .unwrap();
        db.begin_subscription_recreation(source, 100).unwrap();
        assert!(db
            .remap_recreated_subscription(source, 100, 11, &[(1, created(7)), (2, created(7))])
            .is_err());
        assert!(db.get_lserver_sub_id(source, 100).is_err());
        assert_eq!(db.get_lserver_monitored_item(100, 101).unwrap(), Some(1));
        assert_eq!(db.get_lserver_monitored_item(100, 102).unwrap(), Some(2));
        db.remap_recreated_subscription(source, 100, 12, &[(1, created(8)), (2, created(9))])
            .unwrap();
        assert_eq!(db.get_lserver_monitored_item(100, 101).unwrap(), Some(8));
        assert_eq!(
            db.get_lserver_sub_id(source, 100).unwrap().lserver_sub_id,
            12
        );
    }

    #[test]
    fn deleting_reconnecting_owner_preserves_reused_source_id() {
        let pool = MapDatabasePool::new().unwrap();
        let db = pool.connect().unwrap();
        let source = db
            .insert_lserver(&"source".to_string(), &NodeId::new(1, 1))
            .unwrap();
        db.insert_subscription(source, 10, 100).unwrap();
        let cancelled = db.get_lserver_sub_id(source, 100).unwrap();
        db.insert_monitored_item(cancelled.internal_sub_id, 1, 101)
            .unwrap();
        db.begin_subscription_recreation(source, 100).unwrap();
        db.insert_subscription(source, 10, 200).unwrap();
        let retained = db.get_lserver_sub_id(source, 200).unwrap();
        db.insert_monitored_item(retained.internal_sub_id, 2, 201)
            .unwrap();

        db.delete_subscription_context(source, 100).unwrap();

        assert!(db.get_lserver_sub_id(source, 100).is_err());
        assert!(db
            .get_aggserver_mitem_id(cancelled.internal_sub_id, 1)
            .is_err());
        assert_eq!(
            db.get_lserver_sub_id(source, 200).unwrap().lserver_sub_id,
            10
        );
        assert_eq!(db.get_lserver_monitored_item(200, 201).unwrap(), Some(2));
    }
}

#[cfg(test)]
mod onboarding_cleanup_tests {
    use super::*;

    #[test]
    fn failed_source_cleanup_preserves_all_healthy_source_mapping_tables() {
        let pool = MapDatabasePool::new().unwrap();
        let db = pool.connect().unwrap();
        let mut ids = Vec::new();
        for (offset, name) in [(0u16, "healthy"), (1u16, "failed")] {
            let id = db
                .insert_lserver(&name.to_string(), &NodeId::new(1, 100u32 + offset as u32))
                .unwrap();
            db.insert_namespace(id, 2 + offset, 1).unwrap();
            db.insert_type(id, &NodeId::new(1, 200), &NodeId::new(2 + offset, 200))
                .unwrap();
            db.insert_reference(
                &NodeId::new(1, 300),
                id,
                &ReferenceDescription {
                    reference_type_id: NodeId::new(0, 35),
                    is_forward: true,
                    node_id: NodeId::new(2 + offset, 400).into(),
                    browse_name: crate::types::QualifiedName::new(2 + offset, "object"),
                    display_name: "object".into(),
                    node_class: crate::types::NodeClass::Object,
                    type_definition: NodeId::new(0, 58).into(),
                },
            )
            .unwrap();
            db.insert_subscription(id, 10, 20 + offset as u32).unwrap();
            let subscription = db.get_lserver_sub_id(id, 20 + offset as u32).unwrap();
            db.insert_monitored_item(subscription.internal_sub_id, 30, 40)
                .unwrap();
            ids.push(id);
        }
        db.delete_lserver(ids[1]).unwrap();
        for table in [
            "lower_server",
            "namespace_map",
            "type_map",
            "reference_map",
            "subscription_map",
            "monitored_item_map",
        ] {
            let count: i64 = db
                .conn
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(
                count, 1,
                "healthy source damaged or failed source retained in {table}"
            );
        }
        let healthy = db.lower_server_debug_counts("healthy").unwrap();
        assert_eq!(healthy.lower_servers, 1);
        assert_eq!(healthy.namespace_mappings, 1);
        assert_eq!(healthy.type_mappings, 1);
        assert_eq!(healthy.references, 1);
        assert_eq!(healthy.subscriptions, 1);
        assert_eq!(healthy.monitored_items, 1);
        let failed = db.lower_server_debug_counts("failed").unwrap();
        assert_eq!(
            failed.lower_servers
                + failed.namespace_mappings
                + failed.type_mappings
                + failed.references
                + failed.subscriptions
                + failed.monitored_items,
            0
        );
    }
}
