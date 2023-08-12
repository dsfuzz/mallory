pub mod afl;
pub mod eventcount;
pub mod eventhistory;
pub mod vectorclock;

use enum_dispatch::enum_dispatch;
use hashbrown::{HashMap, HashSet};
use siphasher::sip::SipHasher;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

pub use self::{
    afl::AFLBranchFeedback, eventcount::EventCount, eventhistory::EventHistory,
    vectorclock::VectorClock,
};

use super::summary::SummaryWrapper;
// Required by the code that `enum_dispatch` generates.
use crate::feedback::LamportEvent;

#[enum_dispatch(SummaryControl)]
pub enum SummaryKind {
    VectorClock(SummaryWrapper<VectorClock>),
    EventCount(SummaryWrapper<EventCount>),
    EventHistory(SummaryWrapper<EventHistory>),
    AFLBranchFeedback(SummaryWrapper<AFLBranchFeedback>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum SummaryProducerIdentifier {
    EventHistory,
    AFLBranchFeedback,
    #[default]
    Unspecified,
}

pub trait SummaryProducer {
    fn new() -> Self;

    /// Restore the summary to a pristine state.
    fn reset(&mut self);

    /// Update the summary with a new event.
    fn update(&mut self, new_event: &LamportEvent, state_similarity_threshold: f64);

    /// Merge two summaries from different processes.
    fn merge(&mut self, this_event: &LamportEvent, other: &Self, other_event: &LamportEvent);

    /// Union two summaries to obtain a cumulative summary.
    /// This is different from merging in that it does not
    /// take pairwise-max, but treats the two as disjoint and
    /// adds the components together.
    fn union(&mut self, other: &Self);
}

/// A wrapper around HashSet that that has a 'hash' field that can
/// be used to detect changes.
#[derive(Debug, Clone)]
pub struct ChangeAwareSet<T> {
    hash: u64,
    set: HashSet<T>,

    pub _num_skipped: usize,
}

impl<T: Eq + Hash + Clone> ChangeAwareSet<T> {
    pub fn new() -> ChangeAwareSet<T> {
        ChangeAwareSet {
            hash: 0,
            set: HashSet::new(),
            _num_skipped: 0,
        }
    }

    pub fn insert(&mut self, value: T) -> bool {
        let new_hash = {
            let mut hasher = SipHasher::new_with_keys(0, 0);
            value.hash(&mut hasher);
            self.hash ^ hasher.finish()
        };
        let changed = self.set.insert(value);
        if changed {
            self.hash = new_hash;
        }
        changed
    }

    pub fn contains(&self, value: &T) -> bool {
        self.set.contains(value)
    }

    pub fn len(&self) -> usize {
        self.set.len()
    }

    pub fn hash(&self) -> u64 {
        self.hash
    }

    pub fn different_from(&self, other: &ChangeAwareSet<T>) -> bool {
        self.hash != other.hash
    }

    /// First checks that the hash is different, and if so, extends.
    pub fn extend(&mut self, other: &ChangeAwareSet<T>) {
        if self.different_from(other) {
            for value in &other.set {
                self.insert(value.clone());
            }
        } else {
            self._num_skipped += other.len();
        }
    }

    pub fn inner_set(&self) -> &HashSet<T> {
        &self.set
    }

    pub fn iter(&self) -> hashbrown::hash_set::Iter<T> {
        self.set.iter()
    }

    pub fn clear(&mut self) {
        self.set.clear();
        self.hash = 0;
    }
}

impl<T: Eq + Hash + Clone> Default for ChangeAwareSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, T> IntoIterator for &'a ChangeAwareSet<T> {
    type Item = &'a T;
    type IntoIter = hashbrown::hash_set::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.set.iter()
    }
}

/// A change-aware set for pairs and triplets of values.
#[derive(Debug, Clone)]
pub struct LayeredChangeAwareSet<T> {
    _id: u32,

    single: ChangeAwareSet<T>,
    // Pairs are obtained my adding a T2 after every T1 in the single set.
    pairs: ChangeAwareSet<(T, T)>,
    hash_of_single_when_last_added: HashMap<T, u64>,
    _num_skipped_pairs: usize,

    // Triplets are obtained by adding a T3 after every (T1, T2) in the pairs set.
    triplets: ChangeAwareSet<(T, T, T)>,
    hash_of_pairs_when_last_added: HashMap<T, u64>,
    _num_skipped_triplets: usize,
}

impl<T: Eq + Hash + Clone + Debug> LayeredChangeAwareSet<T> {
    pub fn new() -> LayeredChangeAwareSet<T> {
        LayeredChangeAwareSet {
            _id: rand::random(),
            single: ChangeAwareSet::new(),
            pairs: ChangeAwareSet::new(),
            hash_of_single_when_last_added: HashMap::new(),
            _num_skipped_pairs: 0,
            triplets: ChangeAwareSet::new(),
            hash_of_pairs_when_last_added: HashMap::new(),
            _num_skipped_triplets: 0,
        }
    }

    fn insert_single(&mut self, value: T) -> bool {
        self.single.insert(value)
    }

    fn insert_pairs_with(&mut self, value: T) {
        let last_hash = self
            .hash_of_single_when_last_added
            .entry(value.clone())
            .or_default();
        // If single hasn't changed since we last added `value` to pairs,
        // then there is nothing to do.
        if *last_hash == self.single.hash() {
            self._num_skipped_pairs += self.single.len();
            return;
        }
        // let _prev_hash = *last_hash;
        // let _curr_hash = self.single.hash();
        *last_hash = self.single.hash();

        // let prev_pairs_hash = self.triplets.hash();
        // Add all pairs to the pairs set.
        let mut _num_added: usize = 0;
        for ev_a in &self.single {
            let pair = (ev_a.clone(), value.clone());
            _num_added += if self.pairs.insert(pair) { 1 } else { 0 };
        }
        // log::info!(
        //     "[LAYERS {}] Added {} pairs with {:?} (on top of hash {} | last seen {}); pairs: {} => {}",
        //     self._id,
        //     _num_added,
        //     value,
        //     _curr_hash,
        //     _prev_hash,
        //     prev_pairs_hash,
        //     self.pairs.hash()
        // );
    }

    fn insert_triplets_with(&mut self, value: T) {
        let last_hash = self
            .hash_of_pairs_when_last_added
            .entry(value.clone())
            .or_default();
        // If pairs hasn't changed since we last added `value` to triplets,
        // then there is nothing to do.
        if *last_hash == self.pairs.hash() {
            self._num_skipped_triplets += self.pairs.len();
            return;
        }
        // let _prev_hash = *last_hash;
        // let _curr_hash = self.pairs.hash();
        *last_hash = self.pairs.hash();

        // let prev_triplets_hash = self.triplets.hash();
        // Add all triplets to the triplets set.
        let mut _num_added: usize = 0;
        for (ev_a, ev_b) in &self.pairs {
            let triplet = (ev_a.clone(), ev_b.clone(), value.clone());
            _num_added += if self.triplets.insert(triplet) { 1 } else { 0 };
        }
        // log::info!(
        //     "[LAYERS {}] Added {} triplets with {:?} (on top of hash {} | last seen {}); triplets: {} => {}",
        //     self._id,
        //     _num_added,
        //     value,
        //     _curr_hash,
        //     _prev_hash,
        //     prev_triplets_hash,
        //     self.triplets.hash()
        // );
    }

    pub fn insert(&mut self, value: T) {
        // log::info!("[LAYERS {}] Inserting {:?}", self._id, value);
        // self.insert_triplets_with(value.clone());
        self.insert_pairs_with(value.clone());
        self.insert_single(value);
    }

    pub fn get_single(&self) -> &ChangeAwareSet<T> {
        &self.single
    }

    pub fn get_pairs(&self) -> &ChangeAwareSet<(T, T)> {
        &self.pairs
    }

    pub fn get_triplets(&self) -> &ChangeAwareSet<(T, T, T)> {
        &self.triplets
    }

    /// TODO: can this be improved?
    pub fn extend(&mut self, other: &LayeredChangeAwareSet<T>) {
        self.single.extend(&other.single);
        self.pairs.extend(&other.pairs);
        self.triplets.extend(&other.triplets);
        self._num_skipped_pairs += other._num_skipped_pairs;
        self._num_skipped_triplets += other._num_skipped_triplets;
    }

    pub fn get_stats(&self) -> (usize, usize, usize) {
        (
            self.single._num_skipped,
            self._num_skipped_pairs,
            self._num_skipped_triplets,
        )
    }
}

impl<T: Eq + Hash + Clone + Debug> Default for LayeredChangeAwareSet<T> {
    fn default() -> Self {
        Self::new()
    }
}
