/****************************
 * Event pairs and triplets *
 ****************************/

use core::fmt;

use probabilistic_collections::similarity::{MinHash, ShingleIterator};
use probabilistic_collections::SipHasherBuilder;
use std::hash::{Hash, Hasher};
use std::time::Instant;

use hashbrown::{hash_map::Entry, HashMap, HashSet};
extern crate siphasher;
use ordered_float::OrderedFloat;
use siphasher::sip::SipHasher;

#[cfg(feature = "selfcheck")]
use crate::nemesis::schedules::ScheduleId;
use crate::{
    event::{AdministrativeEvent, BlockId, Event, FunctionId, LamportEvent, NodeId, ProcessId},
    feedback::{
        reward::{RewardEntry, RewardFunction, SummaryTask},
        JEPSEN_NODE_ID,
    },
    nemesis::schedules::{qlearning::StateDescriptor, StateId, StepId},
};

use super::{LayeredChangeAwareSet, SummaryProducer, SummaryProducerIdentifier, VectorClock};

/// Coalesces concrete events into their "kind". This only collects "real"
/// events, i.e. of the SUT, and not "environment" events, like `ClientRequest`
/// `ClientResponse` or `Fault`.
#[derive(Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord, Hash)]
pub enum EventKind {
    BlockExecute { block_id: BlockId },
    FunctionExecute { function_id: FunctionId },
    PacketSend { data: u32, from: NodeId, to: NodeId },
    PacketReceive { data: u32, from: NodeId, to: NodeId },
    ResetSummary,
}

impl fmt::Display for EventKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::BlockExecute { block_id } => write!(f, "BB({})", block_id),
            Self::FunctionExecute { function_id } => write!(f, "F({})", function_id),
            Self::PacketSend { data, from, to } => {
                write!(f, "Send({} from {} to {})", data, from, to)
            }
            Self::PacketReceive { data, from, to } => {
                write!(f, "Receive({} from {} at {})", data, from, to)
            }
            Self::ResetSummary => write!(f, "Reset"),
        }
    }
}

fn zscores_str(zscores: &[(ProcessId, EventKind, f32)]) -> String {
    zscores
        .iter()
        .map(|(pid, ek, z)| format!("{}:{}={:.2}", pid, ek, z))
        .collect::<Vec<_>>()
        .join(" ")
}

type EventPair = (EventKind, EventKind);
type EventTriplet = (EventKind, EventKind, EventKind);

impl EventKind {
    fn from_event(event: &LamportEvent) -> Option<EventKind> {
        match event.bare_event() {
            Event::BlockExecute { block_id, .. } => Some(EventKind::BlockExecute {
                block_id: *block_id,
            }),
            Event::FunctionExecute { function_id, .. } => Some(EventKind::FunctionExecute {
                function_id: *function_id,
            }),
            Event::PacketSend { data, to, .. } => Some(EventKind::PacketSend {
                data: *data,
                from: event.proc(),
                to: *to,
            }),
            Event::PacketReceive { data, from, .. } => Some(EventKind::PacketReceive {
                data: *data,
                from: *from,
                to: event.proc(),
            }),
            Event::TimelineEvent(AdministrativeEvent::StartWindow { .. }) => {
                Some(EventKind::ResetSummary)
            }
            _ => None,
        }
    }

    fn is_packet_recv(&self) -> bool {
        matches!(self, Self::PacketReceive { .. })
    }

    fn is_execution(&self) -> bool {
        matches!(
            self,
            Self::BlockExecute { .. } | Self::FunctionExecute { .. }
        )
    }
}

#[derive(Clone)]
pub struct EventHistory {
    /// Vector clock. For deciding when expensive updates are superflous.
    vc: VectorClock,

    /// The number of `LamportEvent`s seen so far for each process.
    num_events: HashMap<ProcessId, usize>,

    /// The number of unique `Event`s for each process.
    unique_events: HashMap<ProcessId, HashMap<EventKind, usize>>,

    // The number of unique 'execution' (non-packet) events per-process.
    pub unique_exec_events: HashMap<ProcessId, HashMap<EventKind, usize>>,

    // Keep track of events, pairs, and triplets.
    pub layers: HashMap<ProcessId, LayeredChangeAwareSet<EventKind>>,

    // The min hash for each state: state signature -> state Id
    pub state_min_hashes: HashMap<Vec<u64>, StateId>,

    // Intuitively, these merge all causal chains into one. This is the
    // artificial "process" that is the causal chain of this summary.
    // pub cross_process_unique_exec_events: HashSet<EventKind>,
    // pub cross_process_unique_exec_pairs: HashSet<EventPair>,
    // pub cross_process_unique_exec_triplets: HashSet<EventTriplet>,

    // Only used by the cumulative summaries, to keep track of what
    // states have been seen. This lets us disincentivise visiting
    // states we've seen before in the reward function.
    // TODO: we should probably do it in the Q-learning agent. This
    // doesn't logically fit with the summary, per se.
    pub unique_states: HashMap<StateId, usize>,

    #[cfg(feature = "selfcheck")]
    /// For debugging purposes.
    pub last_reset: HashMap<ProcessId, (ScheduleId, StepId)>,
}

impl fmt::Display for EventHistory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut procs = self.num_events.keys().cloned().collect::<Vec<_>>();
        procs.sort_unstable();
        writeln!(f, "H[")?;
        writeln!(f, "{}", self.vc)?;
        for proc in procs {
            let num_events = self.num_events.get(&proc).unwrap_or(&0);
            let unique_events = match self.unique_events.get(&proc) {
                Some(x) => x.len(),
                _ => 0,
            };
            let unique_exec_events = match self.unique_exec_events.get(&proc) {
                Some(x) => x.len(),
                _ => 0,
            };
            let unique_exec_pairs = match self.layers.get(&proc) {
                Some(x) => x.get_pairs().len(),
                _ => 0,
            };
            let unique_exec_triplets = match self.layers.get(&proc) {
                Some(x) => x.get_triplets().len(),
                _ => 0,
            };
            let _stats = match self.layers.get(&proc) {
                Some(x) => x.get_stats(),
                _ => (0, 0, 0),
            };

            writeln!(
                f,
                "{}: {} events / {} unique / {} exec-unique / {} exec-pairs",
                proc,
                num_events,
                unique_events,
                unique_exec_events,
                unique_exec_pairs,
                // unique_exec_triplets,
                // _stats,
            )?
        }

        let unified_exec_events = self
            .unique_exec_events
            .values()
            .flat_map(|x| x.keys().cloned())
            .collect::<HashSet<_>>();

        let unified_exec_pairs = self
            .layers
            .values()
            .flat_map(|x| x.get_pairs())
            .collect::<HashSet<_>>();

        let unified_exec_triplets = self
            .layers
            .values()
            .flat_map(|x| x.get_triplets())
            .collect::<HashSet<_>>();

        writeln!(
            f,
            "Unified: {} exec-unique / {} exec-pairs / {} exec-triplets",
            unified_exec_events.len(),
            unified_exec_pairs.len(),
            unified_exec_triplets.len()
        )?;

        // writeln!(
        //     f,
        //     "Cross: {} exec-unique / {} exec-pairs / {} exec-triplets",
        //     self.cross_process_unique_exec_events.len(),
        //     self.cross_process_unique_exec_pairs.len(),
        //     self.cross_process_unique_exec_triplets.len()
        // )?;

        // FIXME: Code duplication with `notify_schedule_end` for Q-learning agent.
        let mut most_hit = self
            .unique_states
            .iter()
            .map(|(a, b)| (*a, *b))
            .collect::<Vec<_>>();
        most_hit.sort_unstable_by(|(_, ac), (_, bc)| bc.cmp(ac));
        let total_visits = most_hit.iter().map(|(_, v)| *v).sum::<usize>();
        let first_n = 10;
        let top_n = most_hit.iter().take(first_n).cloned().collect::<Vec<_>>();
        write!(
            f,
            "Most hit states: {:?} ({} states seen, {} unique)",
            top_n,
            total_visits,
            self.unique_states.len()
        )?;
        write!(f, "]")?;
        #[cfg(feature = "selfcheck")]
        write!(f, " | last_reset: {:?}", self.last_reset)?;
        Ok(())
    }
}

impl EventHistory {
    fn is_empty(&self) -> bool {
        self.num_events.is_empty()
    }

    fn num_nodes(&self) -> usize {
        self.num_events.len()
    }

    fn are_likely_dead(&self) -> Vec<ProcessId> {
        self.num_events
            .iter()
            .filter(|(_, &x)| x == 0)
            .map(|(&proc, _)| proc)
            .collect()
    }

    fn have_received_packets(&self) -> Vec<ProcessId> {
        self.unique_events
            .iter()
            .filter(|(_, x)| x.iter().any(|(ev, _)| ev.is_packet_recv()))
            .map(|(&proc, _)| proc)
            .collect()
    }

    fn num_likely_dead(&self) -> usize {
        self.are_likely_dead().len()
    }

    // Heuristics for reward / features for state
    fn majority_is_likely_dead(&self) -> bool {
        !self.is_empty() && self.num_likely_dead() > self.num_nodes() / 2
    }

    fn majority_has_not_received_packets(&self) -> bool {
        !self.is_empty() && self.have_received_packets().len() < self.num_nodes() / 2
    }

    /// Returns the mapping of packets sent/received during this window.
    fn packet_map(&self) -> Vec<(NodeId, NodeId)> {
        // Collate all the packets into a single set with counts.
        let mut all_packets: HashMap<(NodeId, NodeId), usize> = HashMap::new();
        for (_, events) in self.unique_events.iter() {
            for (ev_kind, count) in events {
                match ev_kind {
                    EventKind::PacketSend { data: _, from, to }
                    | EventKind::PacketReceive { data: _, from, to } => {
                        let key = (*from, *to);
                        let entry = all_packets.entry(key).or_insert(0);
                        *entry += count;
                    }
                    _ => {}
                }
            }
        }
        // TODO: do we want to return normalized (e.g. log2) counts here?
        // Normalize by sorting the keys
        let mut packets: Vec<(NodeId, NodeId)> = all_packets.keys().cloned().collect();
        packets.sort_unstable();
        packets
    }

    fn pairs_map(&self) -> Vec<(ProcessId, EventPair)> {
        let mut pairs: Vec<(ProcessId, EventPair)> = Vec::new();
        for proc in self.layers.keys() {
            let layer = self.layers.get(proc).unwrap();
            for pair in layer.get_pairs() {
                pairs.push((*proc, *pair));
            }
        }
        pairs.sort_unstable();
        pairs
    }

    fn single_map(&self) -> Vec<(ProcessId, EventKind)> {
        let mut single: Vec<(ProcessId, EventKind)> = Vec::new();
        for proc in self.layers.keys() {
            let layer = self.layers.get(proc).unwrap();
            for ev in layer.get_single() {
                single.push((*proc, *ev));
            }
        }
        single.sort_unstable();
        single
    }

    fn unified_exec_events(&self) -> Vec<EventKind> {
        let mut events = HashSet::new();
        for proc in self.layers.keys() {
            let layer = self.layers.get(proc).unwrap();
            for ev in layer.get_single() {
                events.insert(*ev);
            }
        }
        let mut events: Vec<EventKind> = events.into_iter().collect();
        events.sort_unstable();
        events
    }

    fn unified_exec_pairs(&self) -> Vec<EventPair> {
        let mut pairs = HashSet::new();
        for proc in self.layers.keys() {
            let layer = self.layers.get(proc).unwrap();
            for pair in layer.get_pairs() {
                pairs.insert(*pair);
            }
        }
        let mut pairs: Vec<EventPair> = pairs.into_iter().collect();
        pairs.sort_unstable();
        pairs
    }

    /// How much do the triplets of processes differ (as percentage of total,
    /// rounded to the nearest percent).
    fn exec_diff_map(&self) -> Vec<(ProcessId, ProcessId, u8)> {
        let mut procs: HashSet<ProcessId> = self.layers.keys().cloned().collect();
        // Remove the Jepsen node from the set of processes.
        procs.remove(&JEPSEN_NODE_ID);
        let mut procs = procs.into_iter().collect::<Vec<_>>();
        procs.sort();

        // We compute the pairwise symmetric difference between all
        // triplet sets of processes and compute a similarity score
        // based on how many triplets are in the symmetric difference.
        let mut diff: HashMap<(ProcessId, ProcessId), f64> = HashMap::new();
        for fst in procs.iter() {
            for snd in procs.iter() {
                if *fst == *snd {
                    continue;
                }

                // We only insert a symmetric difference, so don't need to
                // add both (a, b) and (b, a). They will be the same.
                let mut key = vec![*fst, *snd];
                key.sort();
                let key = (key[0], key[1]);
                match diff.entry(key) {
                    // Do nothing if we've already computed this.
                    Entry::Occupied(_) => {}
                    Entry::Vacant(e) => {
                        let fst_tr = self.layers.get(fst).unwrap().get_pairs().inner_set();
                        let snd_tr = self.layers.get(snd).unwrap().get_pairs().inner_set();

                        let diff = fst_tr.symmetric_difference(snd_tr);
                        let total = fst_tr.len() + snd_tr.len();
                        let diff = if total != 0 {
                            (diff.count() as f64) / total as f64
                        } else {
                            1.0
                        };
                        e.insert(diff);
                    }
                }
            }
        }
        // Normalize by sorting the keys
        let mut sorted_keys = diff.keys().cloned().collect::<Vec<_>>();
        sorted_keys.sort_unstable();
        let diff: Vec<(ProcessId, ProcessId, u8)> = sorted_keys
            .iter()
            .map(|(s, d)| {
                let x = *diff.get(&(*s, *d)).unwrap();
                let perc = (x * 100.0).round() as u8;
                (*s, *d, perc)
            })
            .collect();
        diff
    }

    /// Returns the mapping of (abstract) events to z-scores.
    fn exec_event_zscores(&self) -> Vec<(ProcessId, EventKind, f32)> {
        let mut zscores: Vec<(ProcessId, EventKind, f32)> = self
            .unique_exec_events
            .iter()
            .flat_map(|(proc, kv)| {
                let values = kv.values().cloned().collect::<Vec<_>>();
                if let Some((mean, stdev)) = mean_stdev(&values) {
                    let rare = kv
                        .iter()
                        .map(|(event, count)| {
                            let zscore = (*count as f32 - mean) / stdev;
                            (*proc, *event, zscore)
                        })
                        .collect();
                    rare
                } else {
                    vec![]
                }
            })
            .collect();
        zscores.sort_unstable_by_key(|(proc, evt, zscore)| (*proc, OrderedFloat(*zscore), *evt));
        zscores
    }

    fn rare_events(&self, zscore_threshold: f32) -> Vec<(ProcessId, EventKind, f32)> {
        let zscores = self.exec_event_zscores();
        zscores
            .into_iter()
            .filter(|(_, _, zscore)| !zscore.is_nan() && *zscore < zscore_threshold)
            .collect()
    }

    fn difference_factor(&self) -> f64 {
        let exec_diff_map = self.exec_diff_map();
        let sum = exec_diff_map.iter().map(|(_, _, x)| x).sum::<u8>();
        let avg = sum as f64 / exec_diff_map.len() as f64;
        avg
    }

    /// Generic version of both `merge` and `union`, to avoid code duplication.
    fn traverse(
        &mut self,
        this_event: Option<&LamportEvent>,
        other: &Self,
        other_event: Option<&LamportEvent>,
        op: fn(usize, usize) -> usize,
    ) {
        // As an optimisation, we only traverse on `union()`, i.e. if
        // `this_event` and `other_event` are None, OR on CollateSummaries.
        // Traversing is VERY expensive and we don't want to do it on every
        // packet, even though that is the morally correct thing to do.

        let is_union = this_event.is_none() && other_event.is_none();
        let is_collate = if let Some(this_event) = this_event {
            matches!(
                this_event.bare_event(),
                Event::TimelineEvent(AdministrativeEvent::CollateSummaries { .. })
            )
        } else {
            false
        };

        if !is_union && !is_collate {
            return;
        }

        // Merge the vector clocks to figure out what changed.
        let changed_procs = self.vc.merge_report(&other.vc);

        // FIXME: this code has a lot of duplication. Get rid of it if possible.
        // It suffices to merge only what actually changed rather than everything.
        for proc in changed_procs {
            // Counts (pairwise max)
            let count = self.num_events.entry(proc).or_insert(0);
            if let Some(other_count) = other.num_events.get(&proc) {
                *count = op(*count, *other_count);
            }

            // Unique counts (pairwise max)
            let unique = self.unique_events.entry(proc).or_default();
            if let Some(other_unique) = other.unique_events.get(&proc) {
                for (ev, other_count) in other_unique {
                    let my_count = unique.entry(*ev).or_insert(0);
                    *my_count = op(*my_count, *other_count);
                }
            }

            // Exec-unique counts (pairwise max)
            let exec_unique = self.unique_exec_events.entry(proc).or_default();
            if let Some(other_exec_unique) = other.unique_exec_events.get(&proc) {
                for (ev, other_count) in other_exec_unique {
                    let my_count = exec_unique.entry(*ev).or_insert(0);
                    *my_count = op(*my_count, *other_count);
                }
            }

            let me = self.layers.entry(proc).or_default();
            if let Some(other) = other.layers.get(&proc) {
                me.extend(other);
            }

            // Cross-process metrics: we are merging two causal chains A and B:
            //  [A] -\
            //  ---> [B]
            // If the update() is called after the merge(), then this does not form
            // any triplets/pairs now, but only when update() is called (and B is
            // processed on top of the unified/merged causal chain).
            // We therefore only need to union the cross-process metrics.
            // It might be helpful to keep counts, but it's unclear what kind of
            // count would be meaningful so we don't double-count.
            // self.cross_process_unique_exec_events
            //     .extend(other.cross_process_unique_exec_events.iter());
            // self.cross_process_unique_exec_pairs
            //     .extend(other.cross_process_unique_exec_pairs.iter());
            // self.cross_process_unique_exec_triplets
            //     .extend(other.cross_process_unique_exec_triplets.iter());

            // States (union with max-count)
            for (state, other_count) in &other.unique_states {
                let my_count = self.unique_states.entry(*state).or_insert(0);
                *my_count = std::cmp::max(*my_count, *other_count);
            }
        }
    }
}

impl Default for EventHistory {
    fn default() -> Self {
        EventHistory::new()
    }
}

impl SummaryProducer for EventHistory {
    fn new() -> Self {
        EventHistory {
            vc: VectorClock::new(),
            num_events: HashMap::new(),
            unique_events: HashMap::new(),
            unique_exec_events: HashMap::new(),
            layers: HashMap::new(),
            state_min_hashes: HashMap::new(),
            // cross_process_unique_exec_events: HashSet::new(),
            // cross_process_unique_exec_pairs: HashSet::new(),
            // cross_process_unique_exec_triplets: HashSet::new(),
            unique_states: HashMap::new(),
            #[cfg(feature = "selfcheck")]
            last_reset: HashMap::new(),
        }
    }

    fn reset(&mut self) {
        self.num_events.clear();
        self.unique_events.clear();
        self.unique_exec_events.clear();
        self.layers.clear();
        // self.cross_process_unique_exec_events.clear();
        // self.cross_process_unique_exec_pairs.clear();
        // self.cross_process_unique_exec_triplets.clear();
        log::debug!("[TRIPLETS] ResetSummary.")
    }

    fn update(&mut self, new_event: &LamportEvent, state_similarity_threshold: f64) {
        if let Some(ev_kind) = EventKind::from_event(new_event) {
            // Update the vector clock.
            self.vc.update(new_event, state_similarity_threshold);

            if ev_kind == EventKind::ResetSummary {
                self.reset();

                #[cfg(feature = "selfcheck")]
                // Record that we've reset this.
                match new_event.bare_event() {
                    Event::TimelineEvent(AdministrativeEvent::StartWindow {
                        schedule_id,
                        step_id,
                    }) => {
                        self.last_reset
                            .insert(new_event.proc(), (*schedule_id, *step_id));
                    }
                    _ => unreachable!(),
                }

                return;
            }

            // We only count "real" events, not "environment" events.
            // For timeline events, we don't reach this point.

            let proc = new_event.proc();
            // Counts
            let count = self.num_events.entry(proc).or_insert(0);
            *count += 1;

            // We proceed in "top-down"/reverse order such that the event we add
            // _now_ does not build a pair/triplet including itself.
            if ev_kind.is_execution() {
                // Cross-process metrics
                // 'Execution' triplets
                // for (ev_a, ev_b) in self.cross_process_unique_exec_pairs.iter() {
                //     let triplet = (*ev_a, *ev_b, ev_kind);
                //     self.cross_process_unique_exec_triplets.insert(triplet);
                // }

                // // 'Execution' pairs
                // for prev_ev in self.cross_process_unique_exec_events.iter() {
                //     let pair = (*prev_ev, ev_kind);
                //     self.cross_process_unique_exec_pairs.insert(pair);
                // }
                // 'Execution' counts
                // self.cross_process_unique_exec_events.insert(ev_kind);

                // Per-process metrics
                let me = self.layers.entry(proc).or_default();
                me.insert(ev_kind);

                // 'Execution' counts
                let unique_exec_events = self.unique_exec_events.entry(proc).or_default();
                let count = unique_exec_events.entry(ev_kind).or_insert(0);
                *count += 1;
            }

            // Unique counts
            let unique_events = self.unique_events.entry(proc).or_default();
            let count = unique_events.entry(ev_kind).or_insert(0);
            *count += 1;
        }

        // Compute the state of the system on CollateSummaries
        if matches!(
            new_event.bare_event(),
            Event::TimelineEvent(AdministrativeEvent::CollateSummaries { .. })
        ) {
            let state = self.get_state_id(state_similarity_threshold);
            let count = self.unique_states.entry(state).or_insert(0);
            *count += 1;
        }
    }

    fn merge(&mut self, this_event: &LamportEvent, other: &Self, other_event: &LamportEvent) {
        self.traverse(Some(this_event), other, Some(other_event), std::cmp::max);
        #[cfg(feature = "selfcheck")]
        for (proc, ls) in other.last_reset.iter() {
            let e = self.last_reset.entry(*proc).or_default();
            *e = std::cmp::max(*e, *ls);
        }
    }

    fn union(&mut self, other: &Self) {
        self.traverse(None, other, None, |a, b| a + b);
    }
}

fn unique_keys<T>(kv: &HashMap<u8, HashMap<T, usize>>) -> HashSet<&T>
where
    T: Eq + Hash,
{
    kv.iter().flat_map(|(_, kv)| kv.keys()).collect()
}

/// Events more than 1.75 stdev less than the mean are considered rare.
const ZSCORE_RARITY_THRESHOLD: f32 = -1.75;

fn mean_stdev(data: &[usize]) -> Option<(f32, f32)> {
    fn mean(data: &[usize]) -> Option<f32> {
        let sum = data.iter().sum::<usize>() as f32;
        let count = data.len();
        match count {
            positive if positive > 0 => Some(sum / count as f32),
            _ => None,
        }
    }
    let m = mean(data);
    match (m, data.len()) {
        (Some(data_mean), count) if count > 0 => {
            let variance = data
                .iter()
                .map(|value| {
                    let diff = data_mean - (*value as f32);
                    diff * diff
                })
                .sum::<f32>()
                / count as f32;
            Some((data_mean, variance.sqrt()))
        }
        _ => None,
    }
}

impl RewardFunction for EventHistory {
    /// Goals of the reward function:
    ///  1. Produce runs that are substantially different from previous runs,
    ///     i.e. exercise distinct system behaviours.
    ///  2. Exercise "rare" behaviours more often, i.e. behaviours that are
    ///     not exercised by most schedules.
    ///  3. Produce runs with "diverse" windows, i.e. the run is not monotonous
    ///     but rather has a variety of behaviours.
    fn reward_function(
        task: SummaryTask,
        overall_cumulative: &Self,
        schedule_cumulative: &Self,
        schedule_window_summaries: &HashMap<StepId, Self>,
        current: &mut Self,
        state_similarity_threshold: f64,
    ) -> RewardEntry {
        // Simple solution: if we've seen this state before, give a negative reward.
        let this_state = current.get_state_id(state_similarity_threshold);
        let reward_for_step = if overall_cumulative.unique_states.contains_key(&this_state)
            || schedule_cumulative.unique_states.contains_key(&this_state)
        {
            -1
        } else {
            0
        };

        RewardEntry::new_with_post_state(
            SummaryProducerIdentifier::EventHistory,
            task,
            this_state,
            reward_for_step,
        )
    }
}

/// Features of the event history, used to construct the state.
struct EventHistoryFeatures {
    packet_map: Vec<(NodeId, NodeId)>,
    unique_events: Vec<EventKind>,
    unique_pairs: Vec<(EventKind, EventKind)>,
    single_map: Vec<(ProcessId, EventKind)>,
    pairs_map: Vec<(ProcessId, EventPair)>,
    exec_diff_map: Vec<(ProcessId, ProcessId, u8)>,
    exec_event_zscores: Vec<(ProcessId, EventKind, f32)>,
    majority_no_events: bool,
    majority_no_received_packets: bool,
    states: Vec<usize>,
}

impl EventHistoryFeatures {
    fn from_history(history: &EventHistory) -> Self {
        // Vector features
        let packet_map = history.packet_map();
        let unique_events = history.unified_exec_events();
        let unique_pairs = history.unified_exec_pairs();
        let single_map = history.single_map();
        let pairs_map = history.pairs_map();
        let exec_diff_map = history.exec_diff_map();
        let exec_event_zscores = history.exec_event_zscores();

        // Binary features
        let majority_no_events = history.majority_is_likely_dead();
        let majority_no_received_packets = history.majority_has_not_received_packets();

        let states = history.unique_states.keys().cloned().collect::<Vec<_>>();

        Self {
            packet_map,
            unique_events,
            unique_pairs,
            single_map,
            pairs_map,
            exec_diff_map,
            exec_event_zscores,
            majority_no_events,
            majority_no_received_packets,
            states,
        }
    }
}

impl StateDescriptor for EventHistory {
    fn get_state_id(&mut self, state_similarity_threshold: f64) -> StateId {
        let features = EventHistoryFeatures::from_history(self);
        // We abstract the state of the system into a single integer, which
        // captures the set of packets (abstracted) sent and received in the
        // SUT in the given window. In other words, the "state" the agent sees
        // is the externally observable behaviour of the system, whereas the
        // reward are based on internal behaviour of the SUT as well.
        let packets = features.packet_map;
        let mut hasher_packets = SipHasher::new_with_keys(0, 0);
        packets.hash(&mut hasher_packets);
        let packets_hash = hasher_packets.finish();

        let events = features.unique_events;
        let mut hasher_global = SipHasher::new_with_keys(0, 0);
        events.hash(&mut hasher_global);
        let events = hasher_global.finish();

        let global_pairs = features.unique_pairs;
        let mut hasher_global_pairs = SipHasher::new_with_keys(0, 0);
        global_pairs.hash(&mut hasher_global_pairs);
        let global_pair_hash = hasher_global_pairs.finish();

        let single = features.single_map;
        let mut hasher_single = SipHasher::new_with_keys(0, 0);
        single.hash(&mut hasher_single);
        let single_hash = hasher_single.finish();

        let pairs = features.pairs_map;
        let mut hasher_pairs = SipHasher::new_with_keys(0, 0);
        pairs.hash(&mut hasher_pairs);
        let pairs_hash = hasher_pairs.finish();

        let hash = global_pair_hash;
        log::info!(
            "[PKT STATE] {:?} | [EVENTS STATE]: {:?} | [PAIRS STATE]: {:?}",
            packets,
            events,
            global_pairs
        );

        let states = features.states;
        // check hash whether in the states
        let current_state = hash as StateId;
        if !states.contains(&current_state) {
            if global_pairs.is_empty() {
                log::info!("No pairs in the state");
                return current_state;
            }

            let begin_time = Instant::now();

            // string to &str
            let global_pairs_string = global_pairs
                .iter()
                .map(|x| format!("{:?}", x))
                .collect::<Vec<_>>();

            let shingles = ShingleIterator::new(
                1,
                global_pairs_string
                    .iter()
                    .map(|x| x.as_str())
                    .collect::<Vec<_>>(),
            );
            let min_hash: MinHash<ShingleIterator<str>, _> = MinHash::with_hashers(
                100,
                [
                    SipHasherBuilder::from_seed(0, 0),
                    SipHasherBuilder::from_seed(1, 1),
                ],
            );

            let min_hash1 = min_hash.get_min_hashes(shingles);

            let mut similarity_cache_vec: Vec<f64> = Vec::new();

            let state_min_hashes = &mut self.state_min_hashes;
            for (min_hash2, state_id) in state_min_hashes.iter() {
                let similarity = min_hash.get_similarity_from_hashes(&min_hash1.clone(), min_hash2);

                if similarity > state_similarity_threshold {
                    log::debug!(
                        "[STATE] The current state is similar to one old state: {} -> {}; Similarity: {}",
                        current_state,
                        *state_id,
                        similarity
                    );
                    log::debug!("[STATE] The following is in details:");
                    global_pairs_string.iter().for_each(|x| {
                        log::debug!("{:?}", x);
                    });
                    return *state_id;
                } else {
                    log::debug!(
                        "[STATE] Similarity between {} and {}: {}",
                        current_state,
                        *state_id,
                        similarity
                    );
                    similarity_cache_vec.push(similarity);
                }
            }

            state_min_hashes.insert(min_hash1, current_state);
            log::debug!(
                "[STATE] The total length of state hashes: {}",
                state_min_hashes.len()
            );

            let duration = begin_time.elapsed();
            log::info!(
                "[STATE] Time to calculate the similarity between the new state and old states: {:?}",
                duration
            );

            log::debug!("[STATE] Observe one new state: {}", current_state);
            log::debug!("[STATE] The following is in details:");
            global_pairs_string.iter().for_each(|x| {
                log::debug!("{:?}", x);
            });
        }

        current_state
    }
}
