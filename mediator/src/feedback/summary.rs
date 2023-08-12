use std::collections::VecDeque;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use enum_dispatch::enum_dispatch;
use hashbrown::hash_map::Entry;
// We're using this rather than std HashMap for `get_many_mut`, which is Nightly-only for now
use hashbrown::{HashMap, HashSet};
use petgraph::stable_graph::{DefaultIx, NodeIndex, StableGraph};
use petgraph::{visit::EdgeRef, Directed};

use chrono::{TimeZone, Utc};

use crate::event::{AdministrativeEvent, Event, LamportEvent, MonotonicTimestamp, ProcessId};
use crate::history::time::ClockManager;
use crate::history::timeline::CommitManager;
use crate::nemesis::AdaptiveNemesis;

// This seems needed by the `enum_dispatch` macro
use crate::feedback::producers::{AFLBranchFeedback, EventCount, EventHistory, VectorClock};
use crate::nemesis::schedules::{ScheduleId, StepId};

use super::producers::{SummaryKind, SummaryProducer, SummaryProducerIdentifier};
use super::reward::RewardFunction;

pub type TimelineIndex = NodeIndex<DefaultIx>;

/// Helper struct for `Timeline`, to store summaries of the timeline.
/// This is parametric in the type of the summary.
pub struct SummaryWrapper<T: SummaryProducer + Clone> {
    /// What is the summary of the timeline at this event('s timestamp)?
    /// We can produce an event's E summary once the summaries of its
    /// dependencies have been produced. This data-structure is pruned
    /// as needed.
    summary_for_event: HashMap<LamportEvent, T>,

    stored_summaries: HashMap<ScheduleId, HashMap<StepId, T>>,

    per_schedule_cumulative: HashMap<ScheduleId, T>,

    /// We keep an overall test cumulative summary, which includes
    /// everything _except_ the current `per_schedule_cumulative`.
    overall_cumulative: (ScheduleId, T),

    last_summarised_event: Option<LamportEvent>,

    /// File to save ShiViz-compatible event history to.
    shiviz_log_file: Option<File>,

    /// File to save the summaries to.
    summary_log_filename: String,
    // Cleanup:
    // - we can remove the summary for an event once its dependants
    //   have been processed, i.e. once all causal links originating
    //   from that event have been traversed.

    state_similarity_threshold: f64,
}

impl<T> SummaryWrapper<T>
where
    T: SummaryProducer + RewardFunction + Clone + fmt::Display,
{
    fn new(
        summary_log_filename: &String,
        shiviz_log_filename: Option<String>,
        state_similarity_threshold: f64,
    ) -> SummaryWrapper<T> {
        let summary_log_filename = summary_log_filename.clone();
        let shiviz_log_file = shiviz_log_filename.map(|f| {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(f)
                .expect("Could not open ShiViz log file!");
            writeln!(
                f,
                "# ShiViz log parsing regex: (?<event>.*)\\n(?<host>\\S*) (?<clock>{{.*}})\\n"
            )
            .expect("Could not write log parsing regex to ShiViz log file!");
            f
        });

        SummaryWrapper {
            summary_for_event: HashMap::new(),
            stored_summaries: HashMap::new(),
            per_schedule_cumulative: HashMap::new(),
            overall_cumulative: (0, T::new()),
            last_summarised_event: None,
            shiviz_log_file,
            summary_log_filename,
            state_similarity_threshold,
        }
    }

    /// Merge the per-schedule cumulative summary into the overall one
    /// after we move to a new schedule.
    fn maintain_overall_cumulative(&mut self, current_schedule_id: ScheduleId) {
        let last_schedule_id = self.overall_cumulative.0;

        if current_schedule_id > last_schedule_id {
            log::debug!(
                "[SUMMARY] Updating overall cumulative with schedule {}!",
                last_schedule_id
            );

            if let Some(last_summary) = self.per_schedule_cumulative.get(&last_schedule_id) {
                self.overall_cumulative.0 = current_schedule_id - 1;
                self.overall_cumulative.1.union(last_summary);
            } else {
                panic!(
                    "[SUMMARY] No cumulative summary for past schedule {}! Cannot update overall cumulative!",
                    last_schedule_id
                );
            }
        }
    }

    fn cleanup_schedules_older_than(&mut self, schedule_id: ScheduleId) {
        let two_behind = usize::checked_sub(schedule_id, 2).unwrap_or(0);
        self.stored_summaries
            .drain_filter(|&sid, _| sid < two_behind);

        self.per_schedule_cumulative
            .drain_filter(|&sid, _| sid < two_behind);
    }
}

#[enum_dispatch]
/// Interface for operating on a SummaryWrapper of a given Summary instance;
/// e.g. SummaryWrapper<EventHistory> implements this.
pub trait SummaryControl {
    fn initialize(&mut self, ev: &LamportEvent);

    /// Copy `pred_ev`'s summary into `this_ev`.
    fn copy(&mut self, pred_ev: &LamportEvent, this_ev: &LamportEvent);
    /// Move `pred_ev`'s summary into `this_ev`.
    fn take_ownership(&mut self, pred_ev: &LamportEvent, this_ev: &LamportEvent);

    /// Actually carry out the update for `this_ev`.
    fn update(&mut self, this_ev: &LamportEvent, state_similarity_threshold: f64);

    fn merge_into(&mut self, pred_ev: &LamportEvent, this_ev: &LamportEvent);

    fn contains_summary_of(&self, event: &LamportEvent) -> bool;
    fn remove_summary(&mut self, event: &LamportEvent);

    fn finalise_and_report_reward(
        &mut self,
        our_event: &LamportEvent,
        nemesis: Option<&dyn AdaptiveNemesis>,
        summary_producers: &Vec<SummaryProducerIdentifier>,
    ) -> bool;

    fn summary_string(&self) -> String;
}

impl<T> SummaryControl for SummaryWrapper<T>
where
    T: SummaryProducer + RewardFunction + Clone + fmt::Display,
{
    fn initialize(&mut self, ev: &LamportEvent) {
        self.summary_for_event.insert(ev.clone(), T::new());
    }

    fn copy(&mut self, pred_ev: &LamportEvent, this_ev: &LamportEvent) {
        let this_summary = self
            .summary_for_event
            .get(pred_ev)
            .expect("Missing summary for dependency!")
            .clone();

        self.summary_for_event.insert(this_ev.clone(), this_summary);
    }

    fn take_ownership(&mut self, pred_ev: &LamportEvent, this_ev: &LamportEvent) {
        let this_summary = self
            .summary_for_event
            .remove(pred_ev)
            .expect("Missing summary for dependency!");

        self.summary_for_event.insert(this_ev.clone(), this_summary);
    }

    fn update(&mut self, this_ev: &LamportEvent, state_similarity_threshold: f64) {
        let this_summary = self
            .summary_for_event
            .get_mut(this_ev)
            .expect("Missing summary for dependency!");

        let pred_str = format!("{}", this_summary);
        this_summary.update(this_ev, state_similarity_threshold);
        let new_str = format!("{}", this_summary);

        log::debug!(
            "[SUMMARY] Update\n{}\nwith\n{}=>\n{}",
            pred_str,
            this_ev,
            new_str
        );

        self.last_summarised_event.replace(this_ev.clone());
    }

    fn merge_into(&mut self, pred_ev: &LamportEvent, this_ev: &LamportEvent) {
        assert!(
            self.summary_for_event.contains_key(pred_ev),
            "Missing summaries for pred_ev {}!",
            pred_ev,
        );
        assert!(
            self.summary_for_event.contains_key(this_ev),
            "Missing summaries for this_ev {}!",
            this_ev,
        );

        let summaries = self.summary_for_event.get_many_mut([pred_ev, this_ev]);
        assert!(
            summaries.is_some(),
            "Missing summaries for {} and/or {}!",
            pred_ev,
            this_ev
        );
        let [dep_summary, this_summary] = summaries.unwrap();

        let dep_str = format!("{}", dep_summary);
        let this_str = format!("{}", this_summary);
        this_summary.merge(this_ev, dep_summary, pred_ev);
        self.last_summarised_event.replace(this_ev.clone());

        let new_str = format!("{}", this_summary);
        log::debug!(
            "[SUMMARY] Merge\n{}\ninto\n{}\n=> {}",
            dep_str,
            this_str,
            new_str
        );
    }

    fn contains_summary_of(&self, ev: &LamportEvent) -> bool {
        self.summary_for_event.contains_key(ev)
    }

    fn remove_summary(&mut self, ev: &LamportEvent) {
        if let Some(summary) = self.summary_for_event.remove(ev) {
            log::debug!("[CLEANUP] Removed summary for {}: {}", ev, summary);
        }
    }

    /// Returns true if we stored the summary.
    fn finalise_and_report_reward(
        &mut self,
        our_event: &LamportEvent,
        nemesis: Option<&dyn AdaptiveNemesis>,
        summary_producers: &Vec<SummaryProducerIdentifier>,
    ) -> bool {
        // Produce ShiViz log
        if let Some(shiviz_log_file) = &mut self.shiviz_log_file {
            // This code assumes `shiviz_log_filename` is only set for
            // `VectorClock` SummaryKind, such that printing makes sense.
            let vc = self
                .summary_for_event
                .get(our_event)
                .expect("Marked summary as finalised, but it's not in summary_for_event!")
                .clone();

            // Append event with VC to the ShiViz log file
            let output = format!("{}\n{} {}", our_event, our_event.proc(), vc);
            log::debug!("[SHIVIZ] {}", output);
            writeln!(shiviz_log_file, "{}", output).expect("Failed to write to summary log file!");
        }
        // Store `CollateSummaries` summaries permanently.
        if let Some(task) = our_event.should_store_summary() {
            let mut summary = self
                .summary_for_event
                .get(our_event)
                .expect("Marked summary as finalised, but it's not in summary_for_event!")
                .clone();

            let schedule_id = task.schedule_id;
            let schedule_cumulative = self
                .per_schedule_cumulative
                .entry(schedule_id)
                .or_insert_with(T::new);

            let latency = ClockManager::utc_now() - Utc.timestamp_nanos(task.end_ts);
            log::info!(
                "[STORE] (Latency: {} ms) Storing summary for {} ({}): {}\nSchedule cumulative: {}\nOverall cumulative: {}",
                latency.num_milliseconds(),
                our_event,
                task,
                summary,
                schedule_cumulative,
                self.overall_cumulative.1,
            );

            // Append summary to the summary log file
            let mut summary_log_file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.summary_log_filename.clone())
                .expect("Failed to open summary log file!");

            writeln!(
                summary_log_file,
                "Current summary: {}\nSchedule cumulative: {}\nOverall cumulative: {}",
                summary, schedule_cumulative, self.overall_cumulative.1
            )
            .expect("Failed to write to summary log file!");

            // Compute and rewards to Nemesis
            let schedule_window_summaries =
                self.stored_summaries.entry(task.schedule_id).or_default();
            let state_similarity_threshold = self.state_similarity_threshold;
            if let Some(nemesis) = nemesis {
                let reward = T::reward_function(
                    task.clone(),
                    &self.overall_cumulative.1,
                    schedule_cumulative,
                    schedule_window_summaries,
                    &mut summary,
                    state_similarity_threshold,
                );
                nemesis.report_reward(&reward, summary_producers);
            }

            // Update per-schedule cumulative summary (after reward is computed)
            schedule_cumulative.union(&summary);

            // Actually store the summary
            self.stored_summaries
                .entry(task.schedule_id)
                .or_default()
                .insert(task.step_id, summary);

            // Update overall cumulative summary, if necessary
            self.maintain_overall_cumulative(schedule_id);

            // Clean-up old schedules
            self.cleanup_schedules_older_than(task.schedule_id);

            // Return true to indicate that we stored the summary.
            true
        } else {
            false
        }
    }

    fn summary_string(&self) -> String {
        format!("{} summaries", self.summary_for_event.len(),)
    }
}

/// Special node in the timeline graph, representing the "at-infinity" event.
/// This is the end-point of edge dependencies that are not yet known, i.e.
/// if event A has a dependent B that has not yet been registered, then
/// we add an edge from A to the at-infinity event. This gets removed
/// once B is registered.
const EVENT_AT_INFINITY: LamportEvent = LamportEvent {
    clock: MonotonicTimestamp::at_infinity(0),
    ev: Event::default(),
};

pub struct SummaryManager {
    summaries: Vec<SummaryKind>,
    summary_producer: Vec<SummaryProducerIdentifier>,

    /// We inform the nemesis of summary results.
    nemesis: Option<Arc<dyn AdaptiveNemesis>>,

    /// We construct a graph (DAG) representation of the timeline
    /// to drive the construction of the summaries.
    timeline: StableGraph<LamportEvent, (), Directed>,

    /// Reverse-mapping from events to indices in the graph.
    /// It's annoying that we have to store this, but whatever.
    index_for_event: HashMap<LamportEvent, TimelineIndex>,

    /// Implementation detail: when registering same_process dependencies
    /// we keep track of which event we've last registered, such that
    /// the newly registered event depends on the last one.
    last_event: HashMap<ProcessId, TimelineIndex>,

    // CommitManager shared with the timeline, to figure us
    // how much of what's added into `timeline` we can process.
    // (The `timeline` is populated dynamically, and it's only safe
    // to process the "complete"/"committed" portion of it.)
    _commit_manager: Arc<CommitManager>,

    /// Special node that marks the "end" of the process' timeline.
    end_node: HashMap<ProcessId, TimelineIndex>,

    /// Special node that marks an event at infinity with an unknown process.
    node_at_infinity: TimelineIndex,

    // For debugging and self-check.
    _graph_dump_counter: AtomicUsize,

    state_similarity_threshold: f64,

    // To ensure every event is added exactly once and traversed at least once.
    #[cfg(feature = "selfcheck")]
    _traversed: HashMap<LamportEvent, bool>,
}

impl SummaryManager {
    pub fn new(
        commit_manager: Arc<CommitManager>,
        event_log_filename: &String,
        shiviz_log_filename: &String,
        feedback_type: &String,
        state_similarity_threshold: f64,
    ) -> SummaryManager {
        // let summaries = vec![
        //     // SummaryKind::EventCount(SummaryWrapper::new(event_log_filename: &String)),
        //     SummaryKind::EventHistory(SummaryWrapper::new(event_log_filename)),
        //     // SummaryKind::AFLBranchFeedback(SummaryWrapper::new(event_log_filename: &String)),
        // ];

        let summaries = match feedback_type.as_str() {
            "event_history" => vec![SummaryKind::EventHistory(SummaryWrapper::new(
                event_log_filename,
                None,
                state_similarity_threshold,
            ))],
            "vc" => vec![SummaryKind::VectorClock(SummaryWrapper::new(
                event_log_filename,
                Some(shiviz_log_filename.to_string()),
                state_similarity_threshold,
            ))],
            "afl_branch" => vec![SummaryKind::AFLBranchFeedback(SummaryWrapper::new(
                event_log_filename,
                None,
                state_similarity_threshold,
            ))],
            "afl_branch_and_event_history" => vec![
                SummaryKind::AFLBranchFeedback(SummaryWrapper::new(event_log_filename, None, state_similarity_threshold)),
                SummaryKind::EventHistory(SummaryWrapper::new(event_log_filename, None, state_similarity_threshold)),
            ],
            _ => panic!("Unknown feedback type: {}", feedback_type),
        };

        let summary_producer = match feedback_type.as_str() {
            "event_history" => vec![SummaryProducerIdentifier::EventHistory],
            "vc" => vec![SummaryProducerIdentifier::Unspecified],
            "afl_branch" => vec![SummaryProducerIdentifier::AFLBranchFeedback],
            "afl_branch_and_event_history" => vec![
                // We want to check whether EventHistory is able to produce more AFL vectors.
                // So only use EventHistory as feedback with two summaries.
                SummaryProducerIdentifier::EventHistory,
            ],
            
            _ => panic!("Unknown feedback type: {}", feedback_type),
        };

        // Add `EVENT_AT_INFINITY` to which events whose dependencies
        // have not yet been registered point.
        let mut timeline = StableGraph::default();
        let node_at_infinity = timeline.add_node(EVENT_AT_INFINITY);
        let mut index_for_event = HashMap::new();
        index_for_event.insert(EVENT_AT_INFINITY, node_at_infinity);

        let state_similarity_threshold = state_similarity_threshold;

        SummaryManager {
            summaries,
            summary_producer,
            nemesis: None,
            timeline,
            index_for_event,
            last_event: HashMap::new(),
            _commit_manager: commit_manager,
            end_node: HashMap::new(),
            node_at_infinity,
            _graph_dump_counter: AtomicUsize::new(0),
            state_similarity_threshold,
            #[cfg(feature = "selfcheck")]
            _traversed: HashMap::new(),
        }
    }

    pub fn register_nemesis(&mut self, nemesis: Arc<dyn AdaptiveNemesis>) {
        self.nemesis = Some(nemesis);
    }

    pub fn print_summary(&self) {
        let edges_to_infinity = self
            .timeline
            .edges_directed(self.node_at_infinity, petgraph::Incoming);

        let _undep_edges: Vec<_> = edges_to_infinity
            .clone()
            .take(5)
            .map(|e| {
                let src = e.source();
                let src_ev = self.timeline.node_weight(src).unwrap();
                (src.index(), src_ev)
            })
            .collect();

        let summ = self.summaries.first().unwrap();
        log::debug!(
            "[STATS][SUMMARY] Timeline has {} nodes ({}) and {} edges ({} to infinity).",
            self.timeline.node_count(),
            summ.summary_string(),
            self.timeline.edge_count(),
            edges_to_infinity.count(),
        );
    }

    pub fn register_process(&mut self, proc: ProcessId) {
        match self.end_node.entry(proc) {
            // Already registered. Nothing to do.
            Entry::Occupied(_) => {}
            Entry::Vacant(entry) => {
                let end_ev = LamportEvent {
                    clock: MonotonicTimestamp::at_infinity(proc),
                    ev: Event::default(),
                };
                let end_node = self.timeline.add_node(end_ev.clone());
                self.index_for_event.insert(end_ev, end_node);

                // Add a link from this node to the node at infinity, to maintain the invariant
                // that every process' timeline ends at the node at infinity.
                // FIXME: we would want to call `add_edge_if_not_exists` here, but the borrow
                // checker will not let us.
                if self
                    .timeline
                    .find_edge(end_node, self.node_at_infinity)
                    .is_none()
                {
                    self.timeline.add_edge(end_node, self.node_at_infinity, ());
                }
                log::debug!(
                    "[GRAPH] Registering node {} as end-node for process {}.",
                    end_node.index(),
                    proc
                );
                entry.insert(end_node);
            }
        }
    }

    fn is_at_infinity(&self, node: TimelineIndex) -> bool {
        node == self.node_at_infinity
            || self
                .end_node
                .iter()
                .any(|(_, idx)| idx.index() == node.index())
    }

    /// Add an edge from `from` to `to`, ensuring no parallel edges are added.
    pub fn add_edge_if_not_exists(&mut self, from: TimelineIndex, to: TimelineIndex) {
        // Defense against adding parallel edges
        if self.timeline.find_edge(from, to).is_none() {
            self.timeline.add_edge(from, to, ());
        }
    }

    /// Remove all edges between `from` and `to` (there may be multiple).
    fn remove_edges(&mut self, from: TimelineIndex, to: TimelineIndex) {
        // the `while` is defensive, to handle the case where we have parallel edges
        while let Some(edge) = self.timeline.find_edge(from, to) {
            self.timeline.remove_edge(edge);
        }
    }

    /// Ensure the given event has a node in the timeline graph.
    fn ensure_node_for_event(&mut self, event: &LamportEvent) -> TimelineIndex {
        if let Some(node_id) = self.index_for_event.get(event) {
            let node_id = *node_id;
            assert!(self.timeline.node_weight(node_id).is_some());
            assert_eq!(self.timeline.node_weight(node_id).unwrap(), event);
            node_id
        } else {
            let node_id = self.timeline.add_node(event.clone());
            if event.is_administrative_event() {
                log::debug!(
                    "[GRAPH] Adding node {} with event {}",
                    node_id.index(),
                    event
                );
            }
            self.index_for_event.insert(event.clone(), node_id);
            self._selfcheck_register_unique_addition(event);
            node_id
        }
    }

    fn remove_dependency(&mut self, from: TimelineIndex, to: TimelineIndex) {
        // Remove the edge(s)
        self.remove_edges(from, to);

        // If the event has no remaining dependencies, remove it and its summary
        let outgoing: Vec<_> = self
            .timeline
            .neighbors_directed(from, petgraph::Outgoing)
            .map(|succ_id| succ_id.index())
            .collect();
        let num_outgoing = outgoing.len();

        let from_event = self.timeline.node_weight(from).unwrap();
        let to_event = self.timeline.node_weight(to).unwrap();

        if from_event.is_administrative_event() || to_event.is_administrative_event() {
            // if true {
            log::debug!(
                "[REMOVE] Deleted edges from node {} to node {}; node {} now has successors: {:?}",
                from.index(),
                to.index(),
                from.index(),
                outgoing
            );
        }

        if num_outgoing == 0 {
            if from_event.is_administrative_event() {
                // if true {
                log::debug!(
                "[REMOVE] Removing node {} (event {}) from timeline because it has no dependents.",
                from.index(),
                from_event,
            );
            }
            for summ in self.summaries.iter_mut() {
                summ.remove_summary(from_event);
            }
            self.index_for_event.remove(from_event);
            self.timeline.remove_node(from);
        }
    }

    pub fn tick(&mut self) {
        self._selfcheck_pre_invariants();
        self.process_worklist();
        self._selfcheck_post_invariants();
        // self.reset_if_no_progress();
    }

    fn is_summarised(&self, node_id: TimelineIndex) -> bool {
        let event = self.timeline.node_weight(node_id).unwrap();
        self.summaries
            .iter()
            .all(|summary| summary.contains_summary_of(event))
    }

    /// Is this event (a) part of a completely added batch AND (b) has all of
    /// its dependencies satisfied/already summarised?
    fn can_be_summarised(&self, node_id: TimelineIndex) -> bool {
        // The locking discipline guarantees we only see fully added batches.
        let dependencies_summarised = self
            .timeline
            .neighbors_directed(node_id, petgraph::Incoming)
            .all(|dep_id| self.is_summarised(dep_id));
        dependencies_summarised
    }

    /// Identify, for every process, the first unsummarised event.
    fn first_unsummarised_events(&self) -> HashMap<ProcessId, LamportEvent> {
        let mut first_unsummarised: HashMap<ProcessId, LamportEvent> = HashMap::new();
        for node_id in self.timeline.node_indices() {
            let ev = self.timeline.node_weight(node_id).unwrap();
            if self.is_at_infinity(node_id) {
                continue;
            }
            if !self.is_summarised(node_id) {
                let first = first_unsummarised.entry(ev.proc()).or_insert(ev.clone());
                if ev < first {
                    *first = ev.clone();
                }
            }
        }
        first_unsummarised
    }

    fn _remove_all_interprocess_edges(
        &mut self,
        node_id: TimelineIndex,
        ev_filter: impl Fn(&LamportEvent) -> bool,
    ) {
        let ev = self.timeline.node_weight(node_id).unwrap();
        if !ev_filter(ev) {
            return;
        }

        let ip_outgoing = self
            .timeline
            .neighbors_directed(node_id, petgraph::Outgoing)
            .filter(|succ_id| {
                let succ_event = self.timeline.node_weight(*succ_id).unwrap();
                !self.is_at_infinity(*succ_id)
                    && succ_event.proc() != ev.proc()
                    && ev_filter(succ_event)
            })
            .collect::<Vec<_>>();

        let ip_incoming = self
            .timeline
            .neighbors_directed(node_id, petgraph::Incoming)
            .filter(|pred_id| {
                let pred_event = self.timeline.node_weight(*pred_id).unwrap();
                !self.is_at_infinity(*pred_id)
                    && pred_event.proc() != ev.proc()
                    && ev_filter(pred_event)
            })
            .collect::<Vec<_>>();

        for succ_id in ip_outgoing {
            self.remove_edges(node_id, succ_id);
        }
        for pred_id in ip_incoming {
            self.remove_edges(pred_id, node_id);
        }
    }

    /// Remove all inter-process edges that do not involve administrative events.
    /// This is a heavy-handed approach to removing cycles.
    fn _remove_all_interprocess_nonadministrative_edges(&mut self) {
        let nodes = self.timeline.node_indices().collect::<Vec<_>>();
        for node_id in nodes {
            self._remove_all_interprocess_edges(node_id, |ev| !ev.is_administrative_event());
        }
        if let Err(cycle) = petgraph::algo::toposort(&self.timeline, None) {
            panic!("Cycle still present after removing all inter-process non-administrative edges: {:?}", cycle);
        }
    }

    /// In rare circumstances, a cycle can form in the timeline graph and summarisation cannot proceed.
    /// Identify if we are in such a situation and remove the cause if yes.
    fn detect_and_cleanup_cycles(&mut self) {
        let mut iterations: usize = 0;
        while let Err(cycle) = petgraph::algo::toposort(&self.timeline, None) {
            log::warn!("[CYCLE] Detected cycle in timeline graph (involving node {}). Removing all inter-process dependencies for that node.", cycle.node_id().index());
            // I am worried that a node that is part of the cycle, but
            // doesn't have a link to another process' timeline is returned,
            // in which case this doesn't do anything.
            self._remove_all_interprocess_edges(cycle.node_id(), |ev| true);
            iterations += 1;

            if iterations > 10 {
                log::warn!(
                    "[CYCLE] Detected cycle in timeline graph, but could not remove it gracefully. Trying to be heavy-handed."
                );
                self._remove_all_interprocess_nonadministrative_edges();
            }
        }
    }

    // TODO: don't construct worklist on every loop, but rather only add the first events on each process
    fn construct_worklist(&mut self) -> VecDeque<(NodeIndex, LamportEvent)> {
        self.detect_and_cleanup_cycles();

        let worklist: VecDeque<(NodeIndex, LamportEvent)> = self
            .timeline
            .node_indices()
            .filter_map(|node_id| {
                let ev = self.timeline.node_weight(node_id).unwrap();
                let can_summarise = !self.is_at_infinity(node_id)
                    && !self.is_summarised(node_id)
                    && self.can_be_summarised(node_id);
                if can_summarise {
                    Some((node_id, ev.clone()))
                } else {
                    None
                }
            })
            .collect();

        worklist
    }

    fn unsummarised_dependents(&self, ev: &LamportEvent, level: usize, depth: i32) -> String {
        let indent = " ".repeat(level * 2);
        let node_id = self
            .index_for_event
            .get(ev)
            .expect("event is not in index_for_event");
        let summarised = self.is_summarised(*node_id);
        let summarised = if summarised {
            " (summarised)"
        } else {
            " (not summarised)"
        };
        let prev_str = if depth < 0 {
            "[depth limit reached]".to_string()
        } else {
            let prev: Vec<(LamportEvent, bool)> = self
                .timeline
                .neighbors_directed(*node_id, petgraph::Incoming)
                .map(|p| {
                    let pev = self.timeline.node_weight(p).unwrap();
                    let summarised = self.is_summarised(p);
                    (pev.clone(), summarised)
                })
                .collect();

            let prev = prev.iter().map(|(ev, summarised)| {
                if *summarised {
                    format!("{}{} (summarised)", indent, ev)
                } else {
                    format!(
                        "{}{}",
                        indent,
                        self.unsummarised_dependents(ev, level + 1, depth - 1)
                    )
                }
            });
            prev.collect::<Vec<_>>().join("\n")
        };

        format!("{} {}\n{}prev:\n{}", ev, summarised, indent, prev_str)
    }

    fn _debug_unsummarised(&self) {
        // Print the unsummarised events.
        #[cfg(feature = "selfcheck")]
        {
            let first_unsummarised = self
                .first_unsummarised_events()
                .iter()
                .map(|(p, e)| format!("{}: {}", p, self.unsummarised_dependents(e, 0, 4)))
                .collect::<Vec<_>>()
                .join("\n");
            log::info!("First unsummarised events:\n{}", first_unsummarised);
        }
    }

    /// NOTE: Assumes _dependents_ (descendants) have already been registered.
    fn process_worklist(&mut self) {
        let mut num_processed: HashMap<ProcessId, usize> = HashMap::new();
        let start = Instant::now();
        let mut first_loop = true;

        // Invariant: worklist only has nodes that:
        // - are not at infinity
        // - are not summarised
        // - can be summarised
        let mut worklist = self.construct_worklist();
        // Loop as long as there are entries that can be processed.
        while let Some((node_id, our_event)) = worklist.pop_front() {
            if first_loop {
                let worklist_str = worklist
                    .iter()
                    .map(|(node_id, ev)| format!("{} ({})", node_id.index(), ev))
                    .collect::<Vec<_>>()
                    .join(", ");

                log::debug!(
                    "[SUMMARY] Worklist has {} elements: {}",
                    worklist.len(),
                    worklist_str
                );
                self._debug_unsummarised();
                self.print_summary();
                self._selfcheck_collate_summaries(true);
                first_loop = false;
            }

            // Process this element
            assert!(
                our_event != EVENT_AT_INFINITY,
                "Worklist trying to process event-at-infinity!",
            );
            assert!(
                !self.is_at_infinity(node_id),
                "Worklist trying to process end-of-process node!",
            );

            if our_event.is_administrative_event() {
                // if true {
                log::debug!(
                    "[SUMMARY] Traversing node {} ({})",
                    node_id.index(),
                    our_event
                );
            }

            self._selfcheck_register_traversed(&our_event);

            let dependencies: Vec<(NodeIndex, LamportEvent)> = self
                .timeline
                .neighbors_directed(node_id, petgraph::Incoming)
                .map(|dep_id| (dep_id, self.timeline.node_weight(dep_id).unwrap().clone()))
                .collect();

            for (dep_id, dep_ev) in dependencies.iter() {
                assert!(
                    self.is_summarised(*dep_id),
                    "Dependency node {} ({}) of {} not summarised!",
                    dep_id.index(),
                    dep_ev,
                    our_event
                );
            }

            let same_proc_dependencies = dependencies
                .iter()
                .filter(|(_, ev)| our_event.proc() == ev.proc())
                .collect::<Vec<_>>();

            if same_proc_dependencies.is_empty() {
                // This should be the beginning of a process' timeline.
                log::info!(
                    "[SUMMARY] Initializing summaries for event {} with no dependencies",
                    our_event
                );
                for summ in self.summaries.iter_mut() {
                    summ.initialize(&our_event);
                }
                log::debug!(
                    "[SUMMARY] Done initializing summaries for event {}",
                    our_event
                );
            } else {
                assert!(
                    same_proc_dependencies.len() == 1,
                    "Node {:?} has {} same-process dependencies! It should have exactly 1!",
                    node_id,
                    same_proc_dependencies.len()
                );

                // (1a) Take the predecessor summary as a base
                let (pred_id, pred_ev) = same_proc_dependencies[0];
                let pred_succ: Vec<_> = self
                    .timeline
                    .neighbors_directed(*pred_id, petgraph::Outgoing)
                    .map(|succ_id| succ_id.index())
                    .collect();
                let done_with_pred = pred_succ.len() == 1;

                log::debug!(
                        "[SUMMARY] Copyng summaries for node {} ({}) from predecessor node {} ({}) (which has successors {:?})",
                        node_id.index(),
                        our_event,
                        pred_id.index(),
                        pred_ev,
                        pred_succ
                    );

                for summ in self.summaries.iter_mut() {
                    if done_with_pred {
                        summ.take_ownership(pred_ev, &our_event);
                    } else {
                        summ.copy(pred_ev, &our_event);
                    }
                    // summ.copy(pred_ev, &our_event);
                }
                // (1b) Remove the same-process dependency
                self.remove_dependency(*pred_id, node_id);
            }

            let inter_proc_dependencies = dependencies
                .iter()
                .filter(|(_, ev)| our_event.proc() != ev.proc())
                .collect::<Vec<_>>();

            // (2a) MERGE it with the inter-process dependencies
            for (dep_id, dep_event) in inter_proc_dependencies {
                let pred_succ: Vec<_> = self
                    .timeline
                    .neighbors_directed(*dep_id, petgraph::Outgoing)
                    .map(|succ_id| succ_id.index())
                    .collect();
                log::debug!(
                        "[SUMMARY] Merging summaries for node {} ({}) with predecessor node {} ({}) (which has successors {:?})",
                        node_id.index(),
                        our_event,
                        dep_id.index(),
                        dep_event,
                        pred_succ
                    );
                for summ in self.summaries.iter_mut() {
                    summ.merge_into(dep_event, &our_event);
                }

                // (2b) Remove the inter-process dependency
                self.remove_dependency(*dep_id, node_id);

                let our_succ: Vec<_> = self
                    .timeline
                    .neighbors_directed(node_id, petgraph::Outgoing)
                    .map(|succ_id| succ_id.index())
                    .collect();
                log::debug!(
                    "[SUMMARY] After processing, node {} ({}) has successors: {:?}",
                    node_id.index(),
                    our_event,
                    our_succ
                );
            }

            // (2b) UPDATE with the current event
            // Note: some summaries rely on UPDATE happening after MERGE
            // (and this makes sense intuitively, as the current event
            // now "has access to" what happened in other causal branches)
            let state_similarity_threshold = self.state_similarity_threshold;
            for summ in self.summaries.iter_mut() {
                summ.update(&our_event, state_similarity_threshold);
            }

            // (3) Mark this node's summaries as finalised
            let nemesis = self.nemesis.as_ref().map(|t| t.as_ref());
            for summ in self.summaries.iter_mut() {
                summ.finalise_and_report_reward(&our_event, nemesis, &self.summary_producer);
            }

            // (4) Add all (real) dependents to the worklist
            for dep_id in self
                .timeline
                .neighbors_directed(node_id, petgraph::Outgoing)
            {
                let dep_ev = self.timeline.node_weight(dep_id).unwrap().clone();
                if !self.is_at_infinity(dep_id)
                    && !self.is_summarised(dep_id)
                    && self.can_be_summarised(dep_id)
                {
                    worklist.push_back((dep_id, dep_ev));
                }
            }

            let e = num_processed.entry(our_event.proc()).or_insert(0);
            *e += 1;
        }

        let total_processed = num_processed.values().sum::<usize>();
        if total_processed > 0 {
            log::debug!(
                "[PERF] Summarising {} events ({:?}) took {}ms.",
                total_processed,
                num_processed,
                start.elapsed().as_millis()
            );
            self.print_summary();
        }
    }

    pub fn register_same_process_event(
        &mut self,
        event: &LamportEvent,
        first_per_process_for_batch: bool,
    ) {
        // Create a node in the timeline graph.
        let node_id = self.ensure_node_for_event(event);
        let proc = event.proc();

        // Is this the first registered event?
        if let Some(last_idx) = self.last_event.get(&proc) {
            // No, so register a dependency.
            let last_node_id = *last_idx;
            let last_event = self.timeline.node_weight(last_node_id).unwrap().clone();

            assert!(
                last_event.proc() == proc,
                "Last event for proc {} was {}, but we are registering {}!",
                proc,
                last_event,
                event
            );

            assert!(
                last_event.ts() <= event.ts(),
                "Last event for proc {} was {}, but we are registering {}, which is before!",
                proc,
                last_event,
                event
            );

            if last_event.is_administrative_event() || event.is_administrative_event() {
                // if true {
                log::debug!(
                "[GRAPH] Registering same-process dependency from node {} to node {} ({} -> {})",
                last_node_id.index(),
                node_id.index(),
                last_event,
                event
            );
            }
            self.add_edge_if_not_exists(last_node_id, node_id);

            // If this is the first event in the batch, we are now the last node's
            // dependent, so we mark it as NOT having an unfilled dependency.
            if first_per_process_for_batch {
                log::debug!(
                    "[GRAPH] Linking last node {} (event {}) to first event in batch node {} (event {})",
                    last_node_id.index(),
                    last_event,
                    node_id.index(),
                    event,
                );

                let end_node = *self.end_node.get(&proc).unwrap();
                self.remove_dependency(last_node_id, end_node);
            }
        }

        // In any case, update the last event.
        self.last_event.insert(event.proc(), node_id);
    }

    pub fn register_unfilled_interprocess_dependent(&mut self, event: &LamportEvent) {
        let node_id = self.ensure_node_for_event(event);
        if event.is_administrative_event() {
            // if true {
            log::debug!(
                "[GRAPH] Registering unfilled inter-process dependent for node {} ({}) to infinity node {}",
                node_id.index(),
                event,
                self.node_at_infinity.index(),
            );
        }
        self.add_edge_if_not_exists(node_id, self.node_at_infinity);
    }

    pub fn deregister_unfilled_interprocess_dependent(&mut self, event: &LamportEvent) {
        let node_id = self.ensure_node_for_event(event);
        if event.is_administrative_event() {
            // if true {
            log::debug!(
                "[GRAPH] Deregistering unfilled inter-process dependent for node {} ({}) to infinity node {}",
                node_id.index(),
                event,
                self.node_at_infinity.index(),
            );
        }
        self.remove_edges(node_id, self.node_at_infinity);
    }

    pub fn register_unfilled_same_process_dependent(&mut self, event: &LamportEvent) {
        let node_id = self.ensure_node_for_event(event);
        let proc = event.proc();
        let end_node = *self
            .end_node
            .get(&proc)
            .expect("End node for process does not exist! Has the process been registered?");
        if event.is_administrative_event() {
            // if true {
            log::debug!(
                "[GRAPH] Registering unfilled dependent for node {} ({}) to end-of-process node {}",
                node_id.index(),
                event,
                end_node.index(),
            );
        }
        self.add_edge_if_not_exists(node_id, end_node);
    }

    pub fn register_interprocess_dependency(
        &mut self,
        event: &LamportEvent,
        depends_on: &LamportEvent,
    ) {
        assert!(
            event.proc() != depends_on.proc(),
            "{} and {} defined as inter-process dependency, but are from the same process!",
            event,
            depends_on
        );

        let node_id = self.ensure_node_for_event(event);
        let depends_node_id = self.ensure_node_for_event(depends_on);

        if event.is_administrative_event() || depends_on.is_administrative_event() {
            // if true {
            log::debug!(
                "[GRAPH] Registering inter-process dependency from node {} to node {} ({} -> {})",
                depends_node_id.index(),
                node_id.index(),
                depends_on,
                event
            );
        }

        // This has an actual dependency, so does not go to infinity.
        // NOTE that we can't call `remove_dependency` here, because we don't have the
        // same-process dependency registered yet and we don't want to remove the `depends_node`.
        self.remove_edges(depends_node_id, self.node_at_infinity);
        self.add_edge_if_not_exists(depends_node_id, node_id);
    }

    // Debugging and self-check.
    fn _dump_graph_dot(&self) -> bool {
        let start = Instant::now();
        let g = petgraph::dot::Dot::with_attr_getters(
            &self.timeline,
            &[],
            &|_g, e| format!("label={}", e.id().index()),
            &|_g, (_, ev)| format!("label=\"{}\"", ev),
        );
        let filename = format!(
            "graph-{}.dot",
            self._graph_dump_counter.fetch_add(1, Ordering::Relaxed)
        );
        let path = std::path::Path::new("/tmp/").join(filename);
        let mut f = std::fs::File::create(path).unwrap();
        writeln!(f, "{:?}", g).expect("Could not write DOT file");
        log::info!(
            "[GRAPH] Dumped graph to {:?} (took {} ms)",
            f,
            start.elapsed().as_millis(),
        );
        // We use this in assertions to dump whenever the rest of the condition is false,
        // e.g. `assert!(some_conditon || self._dump_graph_dot())`.
        false
    }

    fn _selfcheck_pre_invariants(&self) {
        self._selfcheck_process_timelines_connected();
        self._selfcheck_infinity_as_expected();
        self._selfcheck_all_deps_sat_on_worklist_or_summarised();
        self._selfcheck_collate_summaries(false);
        self._selfcheck_no_cycles();
    }

    fn _selfcheck_post_invariants(&self) {
        self._selfcheck_pre_invariants();
        self._selfcheck_all_registered_traversed();
    }

    fn _selfcheck_register_unique_addition(&mut self, ev: &LamportEvent) {
        #[cfg(feature = "selfcheck")]
        {
            if self._traversed.contains_key(ev) {
                panic!("[SELFCHECK] Event {} added twice!", ev);
            }
            self._traversed.insert(ev.clone(), false);
        }
    }

    fn _selfcheck_register_traversed(&mut self, ev: &LamportEvent) {
        #[cfg(feature = "selfcheck")]
        {
            if !self._traversed.contains_key(ev) {
                panic!("[SELFCHECK] Event {} traversed before being added!", ev);
            }
            assert!(
                !self._traversed[ev],
                "[SELFCHECK] Event {} traversed twice!",
                ev
            );
            self._traversed.insert(ev.clone(), true);
        }
    }

    fn _selfcheck_all_registered_traversed(&self) {
        #[cfg(feature = "selfcheck")]
        {
            for (ev, traversed) in self._traversed.iter() {
                if !traversed {
                    panic!("[SELFCHECK] Event {} added but not traversed!", ev);
                }
            }
        }
    }

    /// All events that lead to infinity are either the end_nodes,
    /// `PacketSend`s or `EndWindow`s.
    fn _selfcheck_infinity_as_expected(&self) {
        #[cfg(feature = "selfcheck")]
        {
            let edges_to_infinity = self
                .timeline
                .edges_directed(self.node_at_infinity, petgraph::Incoming);
            let mut nodes_to_infinity = edges_to_infinity.map(|e| e.source());
            nodes_to_infinity.all(|n| {
                let ev = self.timeline.node_weight(n).unwrap();
                let ok = self.end_node.values().any(|&end_node| end_node == n)
                    || ev.is_packet_send()
                    || ev.is_end_window();
                assert!(
                    ok,
                    "[SELFCHECK] Node {} ({}) leads to infinity when it should not!",
                    n.index(),
                    ev
                );
                ok
            });
        }
    }

    fn _selfcheck_all_deps_sat_on_worklist_or_summarised(&self) {
        #[cfg(feature = "selfcheck")]
        {
            let worklist = self.construct_worklist();
            self.timeline.node_indices().all(|n| {
                if !self.is_at_infinity(n) && self.can_be_summarised(n) {
                    assert!(
                        worklist.contains(&n) || self.is_summarised(n),
                        "[SELFCHECK] Node {} ({}) can be summarised, but is neither on the worklist nor summarised!",
                        n.index(),
                        self.timeline.node_weight(n).unwrap()
                    );
                }
                true
            });
        }
    }

    fn _selfcheck_no_cycles(&self) {
        #[cfg(feature = "selfcheck")]
        {
            let topo = petgraph::algo::toposort(&self.timeline, None);
            match topo {
                Ok(_) => {}
                Err(cycle) => {
                    panic!("[SELFCHECK] Cycle detected: {:?}", cycle);
                }
            }
        }
    }

    /// Check that every process' timeline is connected (while following
    /// only edges on that process).
    fn _selfcheck_process_timelines_connected(&self) {
        #[cfg(feature = "selfcheck")]
        {
            let start = Instant::now();

            // Collect all nodes and group them by process.
            let mut nodes_by_proc: HashMap<ProcessId, HashSet<NodeIndex>> = HashMap::new();
            for node_id in self.timeline.node_indices() {
                if node_id != self.node_at_infinity {
                    let ev = self.timeline.node_weight(node_id).unwrap();
                    nodes_by_proc
                        .entry(ev.proc())
                        .or_insert_with(HashSet::new)
                        .insert(node_id);
                }
            }

            // For every process, start from the end node and traverse
            // edges backwards. We should thus visit all nodes of that process.
            // Any nodes we don't visit are not connected to the end node.
            for (proc, nodes) in nodes_by_proc.iter_mut() {
                let end_node = *self.end_node.get(proc).unwrap();
                let mut next_node = Some(end_node);

                while let Some(current_node) = next_node {
                    nodes.remove(&current_node);
                    let same_proc_prev = self
                        .timeline
                        .edges_directed(current_node, petgraph::Incoming)
                        .filter_map(|e| {
                            let src = e.source();
                            let src_ev = self.timeline.node_weight(src).unwrap();
                            if src_ev.proc() == *proc {
                                Some(src)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>();
                    assert!(
                        same_proc_prev.len() <= 1 || self._dump_graph_dot(),
                        "[SELFCHECK] Node {} ({}) has {} incoming same-process edges: {:?}",
                        current_node.index(),
                        same_proc_prev.len(),
                        self.timeline.node_weight(current_node).unwrap(),
                        same_proc_prev
                    );
                    next_node = same_proc_prev.first().map(|n| {
                        assert!(
                            *n != current_node || self._dump_graph_dot(),
                            "[SELFCHECK] Node {} ({}) has an incoming edge to itself!",
                            current_node.index(),
                            self.timeline.node_weight(current_node).unwrap()
                        );
                        *n
                    });
                }
            }

            // There may be remaining (disconnected nodes), but they all
            // should be PacketSends without a corresponding PacketReceive.
            // These nodes should be summarised.
            for (proc, nodes) in nodes_by_proc.iter() {
                for node in nodes.iter() {
                    let ev = self.timeline.node_weight(*node).unwrap();
                    assert!(
                        ev.is_packet_send(),
                        "[SELFCHECK] Node {} ({}) is not connected to the end node of process {}!",
                        node.index(),
                        ev,
                        proc
                    );
                    // Intuitively, we would want to assert that the node is connected to
                    // the infinity node, but in the pre-state (before `process_worklist`),
                    // the PacketSend might have just been matched with a new PacketReceive.
                    assert!(
                        self.is_summarised(*node) || self._dump_graph_dot(),
                        "[SELFCHECK] PacketSend node {} ({}) is not summarised, despite being disconnected from the timeline!",
                        node.index(),
                        ev
                    );
                }
            }
        }
    }

    /// Check whether CollateSummaries exists, and if it does, if it is properly connected
    /// to the rest of the graph, and especially the `EndWindow` nodes.
    fn _selfcheck_collate_summaries(&self, print_stats: bool) {
        #[cfg(feature = "selfcheck")]
        {
            // Collect all `EndWindow` and `CollateSummaries` nodes.
            let mut end_windows = HashMap::new();
            let mut collate_summaries = HashMap::new();
            for n in self.timeline.node_indices() {
                let ev = self.timeline.node_weight(n).unwrap().clone();
                if ev.is_end_window() {
                    end_windows.insert(ev, n);
                } else if ev.is_collate_summaries() {
                    collate_summaries.insert(ev, n);
                }
            }

            if print_stats && (!collate_summaries.is_empty() || !end_windows.is_empty()) {
                log::info!(
                    "[SELFCHECK] Found {} CollateSummaries nodes ({:?}) and {} EndWindow nodes ({:?}).",
                    collate_summaries.len(),
                    collate_summaries.keys().collect::<Vec<_>>(),
                    end_windows.len(),
                    end_windows.keys().collect::<Vec<_>>(),
                );
            }

            // Check that all `CollateSummaries` nodes are connected to the
            // correct number of corresponding `EndWindow` nodes, i.e.
            // there is a `EndWindow` on each actual (not Jepsen or Mediator) process
            // that is connected to the `CollateSummaries`.
            let num_processes = usize::saturating_sub(self.end_node.len(), 2);
            for (ev, n) in collate_summaries.iter() {
                let incoming_windows = self
                    .timeline
                    .edges_directed(*n, petgraph::Incoming)
                    .filter_map(|e| {
                        let inc = e.source();
                        let inc_ev = self.timeline.node_weight(inc).unwrap();
                        if inc_ev.is_end_window() {
                            Some(inc_ev)
                        } else {
                            None
                        }
                    })
                    .collect::<HashSet<_>>();

                assert!(
                        incoming_windows.len() == num_processes,
                        "[SELFCHECK] CollateSummaries node {} ({}) has connections from {} EndWindow nodes ({:?}), but there are {} processes!",
                        n.index(),
                        ev,
                        incoming_windows.len(),
                        incoming_windows,
                        num_processes
                    );
            }
        }
    }
}
