// SPDX-FileCopyrightText: 2026 Adrian Reuther
// SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

//! Single-writer, incremental persistence of the ordered rich rule collection.
//!
//! Normal additions serialize only new records and replace the small closing tail
//! of each JSON array. A transaction keeps only old tails and collection lengths.
//! Actual global shadow removal uses a complete snapshot/rebuild as an exceptional
//! path. Files remain ordinary JSON arrays with the legacy merge/projection order.
//!
//! `rules.transaction.json` marks an in-progress transaction. It is synced before
//! file mutation and retained after an interrupted process or failed restoration.
//! Loading then fails closed. An operator must preserve the files and explicitly
//! rebuild/reset the pair before deleting the marker. This is not automatic crash
//! recovery or a power-loss durability guarantee. Call the pending-marker check
//! before any startup reset. No other writer may modify these files while loaded.

use super::rule_generator::{
    executor_projection, executor_rule, merge_rules, AggregationRule, ExecutorRule,
    ShadowedRulePaths,
};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ExecutorUpdate {
    Append(Vec<ExecutorRule>),
    Replace(Vec<ExecutorRule>),
    Truncate(usize),
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct OperationCounts {
    pub loaded_rich_records: usize,
    pub projected_records: usize,
    pub appended_rich_records: usize,
    pub appended_executor_records: usize,
    pub full_rebuilds: usize,
}

#[derive(Debug)]
struct RuleState {
    rich: Vec<AggregationRule>,
    rich_index: HashMap<u64, Vec<usize>>,
    // Deliberately keyed by legacy paths, NOT source endpoint or qualified path.
    // This exactly preserves the old global is_shadowed_rule semantics.
    shadow_index: HashMap<Vec<String>, HashMap<Vec<String>, usize>>,
    executor: Vec<ExecutorRule>,
    executor_index: HashMap<u64, Vec<usize>>,
}

impl RuleState {
    fn new(rich: Vec<AggregationRule>) -> Self {
        let executor = executor_projection(&rich);
        let mut state = Self {
            rich_index: HashMap::new(),
            shadow_index: HashMap::new(),
            executor_index: HashMap::new(),
            rich,
            executor,
        };
        for (index, rule) in state.rich.iter().enumerate() {
            index_insert(&mut state.rich_index, rule, index);
            *state
                .shadow_index
                .entry(rule.source_node.clone())
                .or_default()
                .entry(rule.target_node.clone())
                .or_default() += 1;
        }
        for (index, rule) in state.executor.iter().enumerate() {
            index_insert(&mut state.executor_index, rule, index);
        }
        state
    }

    fn contains_rich(&self, rule: &AggregationRule) -> bool {
        self.rich_index
            .get(&record_hash(rule))
            .is_some_and(|bucket| bucket.iter().any(|&index| self.rich[index] == *rule))
    }

    fn contains_executor(&self, rule: &ExecutorRule) -> bool {
        self.executor_index
            .get(&record_hash(rule))
            .is_some_and(|bucket| bucket.iter().any(|&index| self.executor[index] == *rule))
    }

    fn has_shadowed_records(&self, shadowed: &ShadowedRulePaths) -> bool {
        shadowed.iter().any(|(source, targets)| {
            self.shadow_index
                .get(source)
                .is_some_and(|known| targets.iter().any(|target| known.contains_key(target)))
        })
    }

    fn append(&mut self, rich: Vec<AggregationRule>, executor: Vec<ExecutorRule>) {
        for rule in rich {
            debug_assert!(!self.contains_rich(&rule));
            index_insert(&mut self.rich_index, &rule, self.rich.len());
            *self
                .shadow_index
                .entry(rule.source_node.clone())
                .or_default()
                .entry(rule.target_node.clone())
                .or_default() += 1;
            self.rich.push(rule);
        }
        for rule in executor {
            index_insert(&mut self.executor_index, &rule, self.executor.len());
            self.executor.push(rule);
        }
    }

    fn truncate(&mut self, rich_len: usize, executor_len: usize) {
        while self.rich.len() > rich_len {
            let rule = self.rich.pop().unwrap();
            index_remove(&mut self.rich_index, &rule, self.rich.len());
            let targets = self.shadow_index.get_mut(&rule.source_node).unwrap();
            let count = targets.get_mut(&rule.target_node).unwrap();
            *count -= 1;
            if *count == 0 {
                targets.remove(&rule.target_node);
            }
            if targets.is_empty() {
                self.shadow_index.remove(&rule.source_node);
            }
        }
        while self.executor.len() > executor_len {
            let rule = self.executor.pop().unwrap();
            index_remove(&mut self.executor_index, &rule, self.executor.len());
        }
    }
}

// Hash buckets store only stable positions. Equality is always checked against
// the canonical record, so collisions cannot conflate mappings. This avoids an
// additional persistent clone of every large rich/executable mapping in a set.
fn record_hash<T: Hash>(value: &T) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn index_insert<T: Hash>(index: &mut HashMap<u64, Vec<usize>>, value: &T, position: usize) {
    index.entry(record_hash(value)).or_default().push(position);
}

fn index_remove<T: Hash>(index: &mut HashMap<u64, Vec<usize>>, value: &T, position: usize) {
    let hash = record_hash(value);
    let bucket = index.get_mut(&hash).expect("indexed record must exist");
    bucket.retain(|candidate| *candidate != position);
    if bucket.is_empty() {
        index.remove(&hash);
    }
}

#[derive(Debug, Clone)]
struct Layout {
    len: u64,
    tail_offset: u64,
    tail: Vec<u8>,
}

impl Layout {
    // Called after complete JSON validation on load, or on our own serialized JSON.
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let close = bytes
            .iter()
            .rposition(|byte| !byte.is_ascii_whitespace())
            .ok_or("empty rule file")?;
        if bytes[close] != b']' {
            return Err("rule file must end with an array".into());
        }
        let mut tail_offset = close;
        while tail_offset > 0 && bytes[tail_offset - 1].is_ascii_whitespace() {
            tail_offset -= 1;
        }
        Ok(Self {
            len: bytes.len() as u64,
            tail_offset: tail_offset as u64,
            tail: bytes[tail_offset..].to_vec(),
        })
    }
}

#[derive(Debug)]
struct JsonFile {
    path: PathBuf,
    layout: Layout,
}

#[derive(Debug)]
struct FileUndo {
    layout: Layout,
    full_bytes: Option<Vec<u8>>,
}

struct PreparedWrite {
    offset: u64,
    bytes: Vec<u8>,
    layout: Layout,
}

impl JsonFile {
    fn verify_tail(&self, file: &mut File) -> Result<()> {
        if file.metadata()?.len() != self.layout.len {
            return Err(format!(
                "rule file changed outside RuleStore: {}",
                self.path.display()
            )
            .into());
        }
        file.seek(SeekFrom::Start(self.layout.tail_offset))?;
        let mut tail = vec![0; self.layout.tail.len()];
        file.read_exact(&mut tail)?;
        if tail != self.layout.tail {
            return Err(format!(
                "rule file tail changed outside RuleStore: {}",
                self.path.display()
            )
            .into());
        }
        Ok(())
    }

    fn checkpoint(&self) -> Result<FileUndo> {
        let mut file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        self.verify_tail(&mut file)?;
        Ok(FileUndo {
            layout: self.layout.clone(),
            full_bytes: None,
        })
    }

    fn append_plan<T: Serialize>(
        &self,
        values: &[T],
        existing_count: usize,
    ) -> Result<Option<PreparedWrite>> {
        if values.is_empty() {
            return Ok(None);
        }
        let mut bytes = Vec::new();
        for (index, value) in values.iter().enumerate() {
            if existing_count + index > 0 {
                bytes.push(b',');
            }
            bytes.push(b'\n');
            let serialized = serde_json::to_vec_pretty(value)?;
            for (line_index, line) in serialized.split(|byte| *byte == b'\n').enumerate() {
                if line_index > 0 {
                    bytes.push(b'\n');
                }
                bytes.extend_from_slice(b"  ");
                bytes.extend_from_slice(line);
            }
        }
        let tail_offset = self.layout.tail_offset + bytes.len() as u64;
        bytes.extend_from_slice(b"\n]");
        Ok(Some(PreparedWrite {
            offset: self.layout.tail_offset,
            layout: Layout {
                len: self.layout.tail_offset + bytes.len() as u64,
                tail_offset,
                tail: b"\n]".to_vec(),
            },
            bytes,
        }))
    }

    fn replace_plan<T: Serialize>(values: &[T]) -> Result<PreparedWrite> {
        let bytes = serde_json::to_vec_pretty(values)?;
        Ok(PreparedWrite {
            offset: 0,
            layout: Layout::from_bytes(&bytes)?,
            bytes,
        })
    }

    fn write(&mut self, plan: &PreparedWrite) -> Result<()> {
        let mut file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        self.verify_tail(&mut file)?;
        file.seek(SeekFrom::Start(plan.offset))?;
        file.write_all(&plan.bytes)?;
        file.set_len(plan.layout.len)?;
        file.flush()?;
        self.layout = plan.layout.clone();
        Ok(())
    }

    fn restore(&mut self, undo: &FileUndo) -> Result<()> {
        let mut file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        if let Some(bytes) = &undo.full_bytes {
            file.seek(SeekFrom::Start(0))?;
            file.write_all(bytes)?;
        } else {
            file.seek(SeekFrom::Start(undo.layout.tail_offset))?;
            file.write_all(&undo.layout.tail)?;
        }
        file.set_len(undo.layout.len)?;
        file.sync_all()?;
        self.layout = undo.layout.clone();
        Ok(())
    }
}

#[derive(Debug)]
struct Transaction {
    rich_len: usize,
    executor_len: usize,
    rich_undo: FileUndo,
    executor_undo: FileUndo,
    // Only populated on a real shadow deletion. No complete normal-path copies.
    replaced_state: Option<RuleState>,
    applied: bool,
}

#[derive(Debug)]
pub(crate) struct RuleStore {
    state: RuleState,
    rich_file: JsonFile,
    executor_file: JsonFile,
    journal: PathBuf,
    transaction: Option<Transaction>,
    poisoned: bool,
    counts: OperationCounts,
    #[cfg(test)]
    fail_executor_write: bool,
    #[cfg(test)]
    fail_restore: bool,
}

impl RuleStore {
    pub(crate) fn journal_path(path: impl AsRef<Path>) -> PathBuf {
        path.as_ref().with_extension("transaction.json")
    }

    /// Must also precede a caller's startup reset, not merely loading the store.
    pub(crate) fn ensure_no_pending_transaction(path: impl AsRef<Path>) -> Result<()> {
        let journal = Self::journal_path(path);
        if journal.try_exists()? {
            return Err(format!("unresolved rule transaction at {}; preserve both rule files and explicitly repair or rebuild before removing the marker", journal.display()).into());
        }
        Ok(())
    }

    pub(crate) fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        Self::ensure_no_pending_transaction(path)?;
        let executor_path = path.with_file_name("rules_executor.json");
        let rich_exists = path.try_exists()?;
        let executor_exists = executor_path.try_exists()?;
        if rich_exists != executor_exists {
            return Err("rule file pair is incomplete; explicit repair required".into());
        }
        if !rich_exists {
            // Never truncate an existing or concurrently created file.
            for target in [path, executor_path.as_path()] {
                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(target)?;
                file.write_all(b"[]")?;
                file.sync_all()?;
            }
        }
        let rich_bytes = fs::read(path)?;
        let executor_bytes = fs::read(&executor_path)?;
        let rich: Vec<AggregationRule> = serde_json::from_slice(&rich_bytes)?;
        let recorded_executor: Vec<ExecutorRule> = serde_json::from_slice(&executor_bytes)?;
        let loaded_rich_records = rich.len();
        let state = RuleState::new(rich);
        if recorded_executor != state.executor {
            return Err(
                "executor file differs from ordered rich-rule projection; explicit repair required"
                    .into(),
            );
        }
        Ok(Self {
            state,
            rich_file: JsonFile {
                path: path.to_path_buf(),
                layout: Layout::from_bytes(&rich_bytes)?,
            },
            executor_file: JsonFile {
                path: executor_path,
                layout: Layout::from_bytes(&executor_bytes)?,
            },
            journal: Self::journal_path(path),
            transaction: None,
            poisoned: false,
            counts: OperationCounts {
                loaded_rich_records,
                projected_records: loaded_rich_records,
                ..OperationCounts::default()
            },
            #[cfg(test)]
            fail_executor_write: false,
            #[cfg(test)]
            fail_restore: false,
        })
    }

    fn healthy(&self) -> Result<()> {
        if self.poisoned {
            return Err(
                "rule store is poisoned; stop onboarding and preserve the transaction marker"
                    .into(),
            );
        }
        Ok(())
    }

    pub(crate) fn executor_rules(&self) -> &[ExecutorRule] {
        &self.state.executor
    }
    pub(crate) fn rich_rules(&self) -> &[AggregationRule] {
        &self.state.rich
    }
    pub(crate) fn operation_counts(&self) -> &OperationCounts {
        &self.counts
    }

    /// One apply is allowed per transaction. Roll back on any downstream failure.
    pub(crate) fn begin_transaction(&mut self) -> Result<()> {
        self.healthy()?;
        if self.transaction.is_some() {
            return Err("rule transaction already active".into());
        }
        let rich_undo = self.rich_file.checkpoint()?;
        let executor_undo = self.executor_file.checkpoint()?;
        let mut marker = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&self.journal)?;
        let content = serde_json::json!({
            "schema": "ojies.rule-transaction/v1", "recovery": "explicit-repair-required",
            "rich_file": self.rich_file.path, "executor_file": self.executor_file.path,
            "rich_length": rich_undo.layout.len, "executor_length": executor_undo.layout.len,
        });
        if let Err(error) = (|| -> Result<()> {
            serde_json::to_writer(&mut marker, &content)?;
            marker.sync_all()?;
            Ok(())
        })() {
            self.poisoned = true;
            return Err(error);
        }
        self.transaction = Some(Transaction {
            rich_len: self.state.rich.len(),
            executor_len: self.state.executor.len(),
            rich_undo,
            executor_undo,
            replaced_state: None,
            applied: false,
        });
        Ok(())
    }

    pub(crate) fn apply(
        &mut self,
        new_rules: &[AggregationRule],
        shadowed: &ShadowedRulePaths,
    ) -> Result<ExecutorUpdate> {
        self.healthy()?;
        let transaction = self
            .transaction
            .as_ref()
            .ok_or("no active rule transaction")?;
        if transaction.applied {
            return Err("only one apply is allowed per rule transaction".into());
        }
        if self.state.has_shadowed_records(shadowed) {
            return self.apply_replacement(new_rules, shadowed);
        }
        let mut seen = HashSet::new();
        let additions: Vec<_> = new_rules
            .iter()
            .filter(|rule| !self.state.contains_rich(rule) && seen.insert(*rule))
            .cloned()
            .collect();
        let mut projection_seen = HashSet::new();
        let projected: Vec<_> = additions
            .iter()
            .map(executor_rule)
            .filter(|rule| {
                !self.state.contains_executor(rule) && projection_seen.insert(rule.clone())
            })
            .collect();
        self.counts.projected_records += additions.len();
        let rich_plan = self
            .rich_file
            .append_plan(&additions, self.state.rich.len())?;
        let executor_plan = self
            .executor_file
            .append_plan(&projected, self.state.executor.len())?;
        self.write_pair(rich_plan, executor_plan)?;
        self.counts.appended_rich_records += additions.len();
        self.counts.appended_executor_records += projected.len();
        let update = ExecutorUpdate::Append(projected.clone());
        self.state.append(additions, projected);
        self.transaction.as_mut().unwrap().applied = true;
        Ok(update)
    }

    fn apply_replacement(
        &mut self,
        new_rules: &[AggregationRule],
        shadowed: &ShadowedRulePaths,
    ) -> Result<ExecutorUpdate> {
        let (merged, _, removed) = merge_rules(self.state.rich.clone(), new_rules, shadowed);
        debug_assert!(removed > 0);
        self.counts.projected_records += merged.len();
        self.replace_state(RuleState::new(merged))
    }

    /// Explicit full-reset path after the caller has stopped all lower workers.
    /// Uses the same active-transaction contract as apply, including rollback.
    pub(crate) fn clear(&mut self) -> Result<ExecutorUpdate> {
        self.healthy()?;
        let transaction = self
            .transaction
            .as_ref()
            .ok_or("no active rule transaction")?;
        if transaction.applied {
            return Err("only one apply is allowed per rule transaction".into());
        }
        self.replace_state(RuleState::new(Vec::new()))
    }

    fn replace_state(&mut self, replacement: RuleState) -> Result<ExecutorUpdate> {
        let rich_plan = JsonFile::replace_plan(&replacement.rich)?;
        let executor_plan = JsonFile::replace_plan(&replacement.executor)?;
        // This full copy is taken only when indexed shadow lookup found old records.
        let rich_bytes = fs::read(&self.rich_file.path)?;
        let executor_bytes = fs::read(&self.executor_file.path)?;
        let transaction = self.transaction.as_mut().unwrap();
        transaction.rich_undo.full_bytes = Some(rich_bytes);
        transaction.executor_undo.full_bytes = Some(executor_bytes);
        self.write_pair(Some(rich_plan), Some(executor_plan))?;
        let update = ExecutorUpdate::Replace(replacement.executor.clone());
        let previous = std::mem::replace(&mut self.state, replacement);
        let transaction = self.transaction.as_mut().unwrap();
        transaction.replaced_state = Some(previous);
        transaction.applied = true;
        self.counts.full_rebuilds += 1;
        Ok(update)
    }

    fn restore_pair(&mut self) -> Result<()> {
        #[cfg(test)]
        if self.fail_restore {
            return Err("injected restoration failure".into());
        }
        let transaction = self
            .transaction
            .as_ref()
            .ok_or("no transaction to restore")?;
        // Attempt both restorations even if the first fails.
        let rich_result = self.rich_file.restore(&transaction.rich_undo);
        let executor_result = self.executor_file.restore(&transaction.executor_undo);
        match (rich_result, executor_result) {
            (Ok(()), Ok(())) => Ok(()),
            (rich, executor) => Err(format!(
                "rule-pair restoration failed: rich={:?}, executor={:?}",
                rich.err(),
                executor.err()
            )
            .into()),
        }
    }

    fn write_pair(
        &mut self,
        rich: Option<PreparedWrite>,
        executor: Option<PreparedWrite>,
    ) -> Result<()> {
        let result = (|| -> Result<()> {
            if let Some(plan) = rich {
                self.rich_file.write(&plan)?;
            }
            if let Some(plan) = executor {
                #[cfg(test)]
                if self.fail_executor_write {
                    self.fail_executor_write = false;
                    return Err(std::io::Error::other("injected second-file write failure").into());
                }
                self.executor_file.write(&plan)?;
            }
            Ok(())
        })();
        if let Err(error) = result {
            if let Err(restore) = self.restore_pair() {
                self.poisoned = true;
                return Err(format!("{error}; {restore}; transaction marker retained").into());
            }
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn commit(&mut self) -> Result<()> {
        self.healthy()?;
        if self.transaction.is_none() {
            return Err("no active rule transaction".into());
        }
        // Files are flushed before clearing the marker. Directory entry durability
        // and all external runtime/SQLite state are outside this store's contract.
        OpenOptions::new()
            .write(true)
            .open(&self.rich_file.path)?
            .sync_all()?;
        OpenOptions::new()
            .write(true)
            .open(&self.executor_file.path)?
            .sync_all()?;
        if let Err(error) = fs::remove_file(&self.journal) {
            self.poisoned = true;
            return Err(error.into());
        }
        self.transaction = None;
        Ok(())
    }

    pub(crate) fn rollback(&mut self) -> Result<ExecutorUpdate> {
        self.healthy()?;
        if self.transaction.is_none() {
            return Err("no active rule transaction".into());
        }
        if let Err(error) = self.restore_pair() {
            self.poisoned = true;
            return Err(error);
        }
        let transaction = self.transaction.as_mut().unwrap();
        let update = if let Some(previous) = transaction.replaced_state.take() {
            self.state = previous;
            ExecutorUpdate::Replace(self.state.executor.clone())
        } else {
            self.state
                .truncate(transaction.rich_len, transaction.executor_len);
            ExecutorUpdate::Truncate(transaction.executor_len)
        };
        if let Err(error) = fs::remove_file(&self.journal) {
            self.poisoned = true;
            return Err(error.into());
        }
        self.transaction = None;
        Ok(update)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server_discovery::rule_generator::{QualifiedNodeIdentity, QualifiedPathElement};
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEMP_ID: AtomicU64 = AtomicU64::new(0);
    struct TestDirectory(PathBuf);
    impl TestDirectory {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "ojies-incremental-rules-{}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                TEMP_ID.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&root).unwrap();
            Self(root)
        }
        fn path(&self) -> PathBuf {
            self.0.join("rules.json")
        }
        fn executor(&self) -> PathBuf {
            self.0.join("rules_executor.json")
        }
    }
    impl Drop for TestDirectory {
        fn drop(&mut self) {
            // The test creates and owns this unique directory under the OS temp root.
            assert!(self.0.is_absolute() && self.0.starts_with(std::env::temp_dir()));
            assert!(self
                .0
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("ojies-incremental-rules-"));
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn path(name: &str) -> Vec<QualifiedPathElement> {
        vec![
            QualifiedPathElement {
                namespace_uri: "http://opcfoundation.org/UA/".into(),
                name: "Objects".into(),
                identifier: None,
            },
            QualifiedPathElement {
                namespace_uri: "urn:test:model".into(),
                name: name.into(),
                identifier: None,
            },
        ]
    }
    fn rule(source: usize, number: usize) -> AggregationRule {
        AggregationRule {
            target_node: vec!["Objects".into(), format!("Target{number}")],
            source_node: vec!["Objects".into(), format!("Source{number}")],
            ref_type: vec![
                "Types".into(),
                "ReferenceTypes".into(),
                "References".into(),
                "HierarchicalReferences".into(),
                "Organizes".into(),
            ],
            is_forward: true,
            target_node_qualified: path(&format!("Target{number}")),
            source_node_qualified: path(&format!("Source{number}")),
            source_id: format!("opc.tcp://test-{source}:4840"),
            source_node_id: Some(QualifiedNodeIdentity {
                namespace_uri: "urn:test:model".into(),
                identifier: format!("i={number}"),
            }),
            reference_type: Some(QualifiedNodeIdentity {
                namespace_uri: "http://opcfoundation.org/UA/".into(),
                identifier: "i=35".into(),
            }),
            source_reference_type: Some(QualifiedNodeIdentity {
                namespace_uri: "http://opcfoundation.org/UA/".into(),
                identifier: "i=47".into(),
            }),
            source_reference_is_forward: Some(true),
            reference_policy: "normalize_tree_reference_to_forward_organizes".into(),
            merge_policy: "merge_at_qualified_target".into(),
            merge_key: path(&format!("Target{number}")),
        }
    }
    fn shadow(rule: &AggregationRule) -> ShadowedRulePaths {
        HashMap::from([(
            rule.source_node.clone(),
            HashSet::from([rule.target_node.clone()]),
        )])
    }
    fn disk_pair(dir: &TestDirectory) -> (Vec<u8>, Vec<u8>) {
        (
            fs::read(dir.path()).unwrap(),
            fs::read(dir.executor()).unwrap(),
        )
    }
    fn assert_state(store: &RuleStore, expected: &[AggregationRule]) {
        assert_eq!(store.rich_rules(), expected);
        assert_eq!(store.executor_rules(), executor_projection(expected));
        let rich: Vec<AggregationRule> =
            serde_json::from_slice(&fs::read(&store.rich_file.path).unwrap()).unwrap();
        let executor: Vec<ExecutorRule> =
            serde_json::from_slice(&fs::read(&store.executor_file.path).unwrap()).unwrap();
        assert_eq!(rich, expected);
        assert_eq!(executor, executor_projection(expected));
    }
    fn commit_add(
        store: &mut RuleStore,
        incoming: &[AggregationRule],
        shadows: &ShadowedRulePaths,
    ) -> ExecutorUpdate {
        store.begin_transaction().unwrap();
        let update = store.apply(incoming, shadows).unwrap();
        store.commit().unwrap();
        update
    }

    #[test]
    fn incremental_sequence_matches_legacy_merge_and_projection() {
        let dir = TestDirectory::new();
        let mut store = RuleStore::load(dir.path()).unwrap();
        let a = rule(1, 1);
        let b = rule(2, 1); // Global legacy shadow key is equal across sources.
        let c = rule(3, 2);
        let mut projected_duplicate = c.clone();
        projected_duplicate.reference_policy = "different rich-only evidence".into();
        let sequence = vec![
            (
                vec![a.clone(), a.clone(), b.clone()],
                ShadowedRulePaths::new(),
            ),
            (
                vec![a.clone(), c.clone(), projected_duplicate],
                ShadowedRulePaths::new(),
            ),
            (vec![rule(4, 4)], shadow(&rule(99, 99))),
            (vec![rule(5, 1), rule(5, 1)], shadow(&a)),
            (vec![], shadow(&c)),
            (vec![b], ShadowedRulePaths::new()),
        ];
        let mut expected = Vec::new();
        for (incoming, shadows) in sequence {
            expected = merge_rules(expected, &incoming, &shadows).0;
            commit_add(&mut store, &incoming, &shadows);
            assert_state(&store, &expected);
            // A fresh load checks persisted projection equality too.
            let reloaded = RuleStore::load(dir.path()).unwrap();
            assert_state(&reloaded, &expected);
        }
        assert_eq!(store.operation_counts().full_rebuilds, 2);
    }

    #[test]
    fn rich_duplicates_with_same_projection_survive_until_their_own_shadow() {
        let dir = TestDirectory::new();
        let mut store = RuleStore::load(dir.path()).unwrap();
        let a = rule(1, 1);
        let mut b = a.clone();
        b.source_node = vec!["Objects".into(), "DifferentLegacyPath".into()];
        b.source_reference_is_forward = Some(false);
        assert_eq!(executor_rule(&a), executor_rule(&b));
        let c = rule(2, 2);
        commit_add(
            &mut store,
            &[a.clone(), c.clone(), b.clone()],
            &ShadowedRulePaths::new(),
        );
        assert_eq!(store.rich_rules().len(), 3);
        assert_eq!(store.executor_rules().len(), 2);
        let update = commit_add(&mut store, &[], &shadow(&a));
        assert_eq!(
            update,
            ExecutorUpdate::Replace(vec![executor_rule(&c), executor_rule(&b)])
        );
        assert_state(&store, &[c, b]);
    }

    #[test]
    fn normal_rollback_restores_exact_bytes_order_and_indexes() {
        let dir = TestDirectory::new();
        let a = rule(1, 1);
        let mut rich = serde_json::to_vec_pretty(&vec![a.clone()]).unwrap();
        rich.extend_from_slice(b" \r\n\t");
        let mut executor = serde_json::to_vec_pretty(&executor_projection(&[a.clone()])).unwrap();
        executor.extend_from_slice(b"\n\n");
        fs::write(dir.path(), &rich).unwrap();
        fs::write(dir.executor(), &executor).unwrap();
        let original = disk_pair(&dir);
        let mut store = RuleStore::load(dir.path()).unwrap();
        let b = rule(2, 2);
        store.begin_transaction().unwrap();
        assert_eq!(
            store
                .apply(&[b.clone()], &ShadowedRulePaths::new())
                .unwrap(),
            ExecutorUpdate::Append(vec![executor_rule(&b)])
        );
        assert_eq!(store.rollback().unwrap(), ExecutorUpdate::Truncate(1));
        assert_eq!(disk_pair(&dir), original);
        assert_state(&store, &[a.clone()]);
        // A rolled-back record must not remain in either dedup index.
        commit_add(&mut store, &[b.clone()], &ShadowedRulePaths::new());
        assert_state(&store, &[a, b]);
    }

    #[test]
    fn global_shadow_replacement_rolls_back_the_whole_ordered_pair() {
        let dir = TestDirectory::new();
        let mut store = RuleStore::load(dir.path()).unwrap();
        let a = rule(1, 1);
        let b = rule(2, 1);
        let c = rule(3, 2);
        commit_add(
            &mut store,
            &[a.clone(), b.clone(), c.clone()],
            &ShadowedRulePaths::new(),
        );
        let original = disk_pair(&dir);
        store.begin_transaction().unwrap();
        let update = store.apply(&[rule(4, 3)], &shadow(&a)).unwrap();
        assert!(matches!(update, ExecutorUpdate::Replace(_)));
        assert_state(&store, &[c.clone(), rule(4, 3)]);
        assert_eq!(
            store.rollback().unwrap(),
            ExecutorUpdate::Replace(executor_projection(&[a.clone(), b.clone(), c.clone()]))
        );
        assert_eq!(disk_pair(&dir), original);
        assert_state(&store, &[a, b, c]);
    }

    #[test]
    fn second_file_failure_restores_first_file_and_keeps_transaction_open() {
        let dir = TestDirectory::new();
        let mut store = RuleStore::load(dir.path()).unwrap();
        let a = rule(1, 1);
        commit_add(&mut store, &[a.clone()], &ShadowedRulePaths::new());
        let original = disk_pair(&dir);
        store.begin_transaction().unwrap();
        store.fail_executor_write = true;
        assert!(store
            .apply(&[rule(2, 2)], &ShadowedRulePaths::new())
            .is_err());
        assert_eq!(disk_pair(&dir), original);
        assert_state(&store, &[a]);
        assert!(RuleStore::ensure_no_pending_transaction(dir.path()).is_err());
        assert_eq!(store.rollback().unwrap(), ExecutorUpdate::Truncate(1));
        assert!(RuleStore::ensure_no_pending_transaction(dir.path()).is_ok());
    }

    #[test]
    fn replacement_second_file_failure_restores_exact_pair() {
        let dir = TestDirectory::new();
        let mut store = RuleStore::load(dir.path()).unwrap();
        let a = rule(1, 1);
        commit_add(&mut store, &[a.clone()], &ShadowedRulePaths::new());
        let original = disk_pair(&dir);
        store.begin_transaction().unwrap();
        store.fail_executor_write = true;
        assert!(store.apply(&[rule(2, 2)], &shadow(&a)).is_err());
        assert_eq!(disk_pair(&dir), original);
        store.rollback().unwrap();
        assert_state(&store, &[a]);
    }

    #[test]
    fn failed_restore_poisoning_and_interrupted_marker_fail_closed() {
        let dir = TestDirectory::new();
        let mut store = RuleStore::load(dir.path()).unwrap();
        store.begin_transaction().unwrap();
        store.fail_executor_write = true;
        store.fail_restore = true;
        let error = store
            .apply(&[rule(1, 1)], &ShadowedRulePaths::new())
            .unwrap_err()
            .to_string();
        assert!(error.contains("transaction marker retained"));
        assert!(store.commit().is_err());
        assert!(store.rollback().is_err());
        assert!(store.begin_transaction().is_err());
        drop(store);
        assert!(RuleStore::load(dir.path())
            .unwrap_err()
            .to_string()
            .contains("unresolved rule transaction"));
    }

    #[test]
    fn completed_append_without_commit_blocks_reload() {
        let dir = TestDirectory::new();
        let mut store = RuleStore::load(dir.path()).unwrap();
        store.begin_transaction().unwrap();
        store
            .apply(&[rule(1, 1)], &ShadowedRulePaths::new())
            .unwrap();
        drop(store); // Simulate process interruption: marker is deliberately retained.
        assert!(RuleStore::load(dir.path()).is_err());
        assert!(RuleStore::ensure_no_pending_transaction(dir.path()).is_err());
    }

    #[test]
    fn malformed_or_mismatched_existing_pair_is_never_overwritten() {
        let dir = TestDirectory::new();
        fs::write(dir.path(), b"{malformed").unwrap();
        fs::write(dir.executor(), b"[]").unwrap();
        let malformed = disk_pair(&dir);
        assert!(RuleStore::load(dir.path()).is_err());
        assert_eq!(disk_pair(&dir), malformed);
        fs::write(dir.path(), serde_json::to_vec(&vec![rule(1, 1)]).unwrap()).unwrap();
        let mismatched = disk_pair(&dir);
        assert!(RuleStore::load(dir.path()).is_err());
        assert_eq!(disk_pair(&dir), mismatched);
    }

    #[test]
    fn existing_duplicate_rich_records_are_preserved_like_legacy_merge() {
        let dir = TestDirectory::new();
        let a = rule(1, 1);
        let existing = vec![a.clone(), a.clone(), rule(2, 2)];
        fs::write(dir.path(), serde_json::to_vec_pretty(&existing).unwrap()).unwrap();
        fs::write(
            dir.executor(),
            serde_json::to_vec_pretty(&executor_projection(&existing)).unwrap(),
        )
        .unwrap();
        let mut store = RuleStore::load(dir.path()).unwrap();
        commit_add(&mut store, &[a, rule(3, 3)], &ShadowedRulePaths::new());
        assert_state(
            &store,
            &merge_rules(existing, &[rule(3, 3)], &ShadowedRulePaths::new()).0,
        );
    }

    #[test]
    fn distinct_records_in_hash_bucket_are_compared_by_full_equality() {
        let a = rule(1, 1);
        let b = rule(2, 2);
        let mut state = RuleState::new(vec![a.clone()]);
        // Simulate a hash collision: a bucket hit alone must not mean equality.
        state.rich_index.insert(record_hash(&b), vec![0]);
        state
            .executor_index
            .insert(record_hash(&executor_rule(&b)), vec![0]);
        assert!(!state.contains_rich(&b));
        assert!(!state.contains_executor(&executor_rule(&b)));
        assert!(state.contains_rich(&a));
    }

    #[test]
    fn append_export_is_deterministic_and_visits_only_new_records() {
        let first = TestDirectory::new();
        let second = TestDirectory::new();
        let mut incremental = RuleStore::load(first.path()).unwrap();
        let all: Vec<_> = (1..=100).map(|source| rule(source, source)).collect();
        for incoming in &all {
            // Nonempty shadow requests that match nothing must stay incremental.
            commit_add(
                &mut incremental,
                &[incoming.clone()],
                &shadow(&rule(999, 999)),
            );
        }
        let mut batched = RuleStore::load(second.path()).unwrap();
        commit_add(&mut batched, &all, &ShadowedRulePaths::new());
        assert_eq!(disk_pair(&first), disk_pair(&second));
        assert_state(&incremental, &all);
        assert_eq!(
            incremental.operation_counts(),
            &OperationCounts {
                loaded_rich_records: 0,
                projected_records: 100,
                appended_rich_records: 100,
                appended_executor_records: 100,
                full_rebuilds: 0,
            }
        );
        let converted = incremental.executor_rules()[0]
            .to_instance_mapping_rule()
            .unwrap();
        assert_eq!(
            converted.source_id.as_deref(),
            Some("opc.tcp://test-1:4840")
        );
        assert_eq!(converted.source_node[1].name(), "Source1");
    }

    #[test]
    fn clear_uses_explicit_transaction_and_can_be_rolled_back_or_committed() {
        let dir = TestDirectory::new();
        let mut store = RuleStore::load(dir.path()).unwrap();
        assert!(store.clear().is_err());
        let a = rule(1, 1);
        commit_add(&mut store, &[a.clone()], &ShadowedRulePaths::new());
        let original = disk_pair(&dir);
        store.begin_transaction().unwrap();
        assert_eq!(store.clear().unwrap(), ExecutorUpdate::Replace(vec![]));
        assert_state(&store, &[]);
        assert_eq!(
            store.rollback().unwrap(),
            ExecutorUpdate::Replace(executor_projection(&[a.clone()]))
        );
        assert_eq!(disk_pair(&dir), original);
        store.begin_transaction().unwrap();
        store.clear().unwrap();
        store.commit().unwrap();
        assert_state(&store, &[]);
        commit_add(&mut store, &[a.clone()], &ShadowedRulePaths::new());
        assert_state(&store, &[a]);
        assert!(store.rollback().is_err());
    }

    #[test]
    fn store_updates_match_runtime_canonical_projection_without_services() {
        use opcua::server::address_space::AddressSpace;
        use opcua::server::aggregation_server::aggregation_server::AggregationServer;
        use opcua::sync::RwLock;
        use std::sync::Arc;

        fn assert_runtime(aggregation: &AggregationServer, expected: &[AggregationRule]) {
            let actual = aggregation.instance_mapping_rules_p.read();
            let projected = executor_projection(expected);
            assert_eq!(actual.len(), projected.len());
            for (actual, expected) in actual.iter().zip(projected) {
                let expected = expected.to_instance_mapping_rule().unwrap();
                assert_eq!(actual.target_node, expected.target_node);
                assert_eq!(actual.source_node, expected.source_node);
                assert_eq!(actual.ref_type, expected.ref_type);
                assert_eq!(actual.is_forward, expected.is_forward);
                assert_eq!(actual.source_id, expected.source_id);
                assert_eq!(actual.source_node_id, expected.source_node_id);
                assert_eq!(actual.reference_type, expected.reference_type);
                assert_eq!(actual.merge_policy, expected.merge_policy);
                assert_eq!(actual.merge_key, expected.merge_key);
            }
        }

        // Construction uses only AddressSpace and an in-memory SQLite pool. No
        // endpoint, source process, socket, discovery or service loop is started.
        let aggregation =
            AggregationServer::new(&Arc::new(RwLock::new(AddressSpace::default()))).unwrap();
        let dir = TestDirectory::new();
        let mut store = RuleStore::load(dir.path()).unwrap();
        let a = rule(1, 1);
        let b = rule(2, 1);
        let mut duplicate_projection = a.clone();
        duplicate_projection.reference_policy = "additional retained evidence".into();
        let sequence = vec![
            (
                vec![a.clone(), b.clone(), a.clone(), duplicate_projection],
                ShadowedRulePaths::new(),
                true,
            ),
            (vec![a.clone(), rule(3, 2)], ShadowedRulePaths::new(), true),
            (vec![rule(4, 3)], shadow(&a), false),
            (vec![rule(5, 4)], shadow(&a), true),
            (vec![rule(6, 5)], ShadowedRulePaths::new(), false),
            (vec![rule(6, 5)], ShadowedRulePaths::new(), true),
        ];
        let mut expected = Vec::new();
        for (incoming, shadows, commit) in sequence {
            let before = expected.clone();
            store.begin_transaction().unwrap();
            let update = store.apply(&incoming, &shadows).unwrap();
            crate::apply_rule_update(&aggregation, update).unwrap();
            expected = merge_rules(expected, &incoming, &shadows).0;
            assert_state(&store, &expected);
            assert_runtime(&aggregation, &expected);
            if commit {
                store.commit().unwrap();
            } else {
                crate::apply_rule_update(&aggregation, store.rollback().unwrap()).unwrap();
                expected = before;
                assert_state(&store, &expected);
                assert_runtime(&aggregation, &expected);
            }
        }
    }
}
