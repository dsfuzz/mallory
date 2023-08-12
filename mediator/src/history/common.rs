use bztree::BzTree;

pub type NodeIp = String;
// Used to keep timelines. The type must implement Ord.
pub type OrderedSet<T> = BzTree<T, ()>;
pub type OrderedMap<K, V> = BzTree<K, V>;

pub type SeqNum = usize;

pub use crate::event::{MonotonicTimestamp, NodeId, ProcessId};
