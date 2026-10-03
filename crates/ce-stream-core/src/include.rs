//! Live include-list for a capture session (shared by all adapters).

use std::borrow::Borrow;
use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use crate::event::TableRef;

/// Snapshot of the include list for one transaction.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum IncludeFilter {
    /// Empty `include_tables` at start: every table.
    #[default]
    All,
    /// Only these `database.table` subjects. Empty means no row events.
    Only(HashSet<String>),
}

impl IncludeFilter {
    pub fn row_allowed(&self, subject: &str) -> bool {
        match self {
            Self::All => true,
            Self::Only(set) => set.contains(subject),
        }
    }

    fn from_tables<I, T>(tables: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Borrow<TableRef>,
    {
        let set: HashSet<String> = tables
            .into_iter()
            .map(|t| t.borrow().as_subject())
            .collect();
        if set.is_empty() {
            Self::All
        } else {
            Self::Only(set)
        }
    }
}

#[derive(Debug)]
struct Inner {
    filter: IncludeFilter,
    initialized: bool,
}

/// Cloneable handle to the capture include list (`database.table` subjects).
///
/// Adapters snapshot the list at the start of each source transaction (MySQL:
/// the GTID event), so updates never split an envelope. There is no ack: the
/// change is best-effort from the next transaction.
///
/// An empty `include_tables` at capture start means all tables (v0.2.0). After
/// that, [`Self::remove`] of the last table or [`Self::replace`] with an empty
/// list means **no** row events (empty commits still flow). MySQL DDL Query
/// events are not filtered (same as v0.2.0).
#[derive(Clone, Debug)]
pub struct IncludeList {
    inner: Arc<RwLock<Inner>>,
}

impl Default for IncludeList {
    fn default() -> Self {
        Self::new()
    }
}

impl IncludeList {
    /// Uninitialized list. Capture start copies `SourceConfig::include_tables`
    /// unless [`Self::insert`], [`Self::remove`], or [`Self::replace`] ran first.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                filter: IncludeFilter::All,
                initialized: false,
            })),
        }
    }

    /// Initialized from table refs. Empty input means all tables.
    pub fn from_tables<I, T>(tables: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Borrow<TableRef>,
    {
        let list = Self::new();
        {
            let mut inner = list.write();
            inner.filter = IncludeFilter::from_tables(tables);
            inner.initialized = true;
        }
        list
    }

    /// Initialized from `database.table` subjects. Empty input means no row events.
    pub fn from_subjects<I>(subjects: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        let list = Self::new();
        {
            let mut inner = list.write();
            inner.filter = IncludeFilter::Only(subjects.into_iter().map(Into::into).collect());
            inner.initialized = true;
        }
        list
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Inner> {
        self.inner.write().unwrap_or_else(|e| e.into_inner())
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Inner> {
        self.inner.read().unwrap_or_else(|e| e.into_inner())
    }

    /// Filter to pin for one transaction.
    pub fn snapshot_filter(&self) -> IncludeFilter {
        self.read().filter.clone()
    }

    /// True when this list currently delivers every table.
    pub fn is_all(&self) -> bool {
        matches!(self.read().filter, IncludeFilter::All)
    }

    /// Replace the whole list. Empty input means no row events (not all tables).
    pub fn replace<I, T>(&self, tables: I)
    where
        I: IntoIterator<Item = T>,
        T: Borrow<TableRef>,
    {
        let mut inner = self.write();
        inner.filter = IncludeFilter::Only(
            tables
                .into_iter()
                .map(|t| t.borrow().as_subject())
                .collect(),
        );
        inner.initialized = true;
    }

    /// Add one `database.table`. No-op when already watching all tables.
    /// Takes effect at the next transaction.
    pub fn insert(&self, table: TableRef) {
        let mut inner = self.write();
        if matches!(inner.filter, IncludeFilter::All) {
            if inner.initialized {
                return;
            }
            inner.filter = IncludeFilter::Only(HashSet::from([table.as_subject()]));
            inner.initialized = true;
            return;
        }
        inner.initialized = true;
        if let IncludeFilter::Only(set) = &mut inner.filter {
            set.insert(table.as_subject());
        }
    }

    /// Remove one `database.table`. No-op when watching all tables.
    /// Takes effect at the next transaction.
    pub fn remove(&self, table: &TableRef) {
        let mut inner = self.write();
        if !inner.initialized {
            return;
        }
        if let IncludeFilter::Only(set) = &mut inner.filter {
            set.remove(&table.as_subject());
        }
    }

    /// Sorted subjects when the list is a finite include set. Empty when all tables.
    pub fn subjects(&self) -> Vec<String> {
        match &self.read().filter {
            IncludeFilter::All => Vec::new(),
            IncludeFilter::Only(set) => {
                let mut v: Vec<String> = set.iter().cloned().collect();
                v.sort();
                v
            }
        }
    }

    /// Copy `config.include_tables` if this handle was never mutated.
    pub fn seed_if_uninitialized(&self, tables: &[TableRef]) {
        let mut inner = self.write();
        if inner.initialized {
            return;
        }
        inner.filter = IncludeFilter::from_tables(tables);
        inner.initialized = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_copies_config_once() {
        let list = IncludeList::new();
        list.seed_if_uninitialized(&[TableRef::new("demo", "orders")]);
        assert_eq!(list.subjects(), vec!["demo.orders"]);
        list.seed_if_uninitialized(&[TableRef::new("demo", "noise")]);
        assert_eq!(list.subjects(), vec!["demo.orders"]);
    }

    #[test]
    fn seed_empty_config_is_all_tables() {
        let list = IncludeList::new();
        list.seed_if_uninitialized(&[]);
        assert!(list.is_all());
        assert!(list.snapshot_filter().row_allowed("demo.anything"));
    }

    #[test]
    fn insert_before_run_skips_later_seed() {
        let list = IncludeList::new();
        list.insert(TableRef::new("demo", "noise"));
        list.seed_if_uninitialized(&[TableRef::new("demo", "orders")]);
        assert_eq!(list.subjects(), vec!["demo.noise"]);
        assert!(!list.is_all());
    }

    #[test]
    fn clone_shares_updates() {
        let a = IncludeList::from_subjects(["demo.orders"]);
        let b = a.clone();
        b.insert(TableRef::new("demo", "noise"));
        a.remove(&TableRef::new("demo", "orders"));
        assert_eq!(a.subjects(), vec!["demo.noise"]);
        assert_eq!(b.snapshot_filter(), a.snapshot_filter());
    }

    #[test]
    fn remove_last_table_delivers_no_rows() {
        let list = IncludeList::from_subjects(["demo.orders"]);
        list.remove(&TableRef::new("demo", "orders"));
        assert!(!list.is_all());
        assert!(!list.snapshot_filter().row_allowed("demo.orders"));
    }

    #[test]
    fn replace_empty_is_watch_nothing() {
        let list = IncludeList::from_subjects(["demo.orders"]);
        list.replace(Vec::<TableRef>::new());
        assert!(!list.is_all());
        assert!(!list.snapshot_filter().row_allowed("demo.orders"));
    }
}
