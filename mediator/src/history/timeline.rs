use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{self, Excluded, Included};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use antidote::{Mutex, MutexGuard, RwLock};
use chrono::Duration;
use dashmap::DashMap;
#[cfg(feature = "selfcheck")]
use dashmap::DashSet;
use hashbrown::hash_map::Entry;
use hashbrown::{HashMap, HashSet};

use crate::event::{
    AdministrativeEvent, ArrowPosition, CausalLink, Event, EventMatchInstruction, EventMatchKey,
    LamportEvent, MatchingBehaviour, MonotonicTimestamp, ProcessId,
};

use crate::feedback::summary::SummaryManager;
use crate::feedback::JEPSEN_NODE_ID;
use crate::nemesis::schedules::{ScheduleId, StepId};

use super::time::ClockManager;

type CausalLinkIdentifier = usize;

/// Timeline of events across multiple processes.
pub struct DynamicTimeline {
    /// Each process (node + thread) maintains a sorted (by real timestamp)
    /// list of events. There is a special process for Jepsen itself
    /// (for the clients that generate requests + faults).
    events: DashMap<ProcessId, Mutex<BTreeSet<LamportEvent>>>,
    // We keep this for statistics, so we don't need to acquire the lock.
    num_events: DashMap<ProcessId, AtomicUsize>,
    num_cleaned_events: AtomicUsize,

    /// It would be nice to have the `CausalLinkIdentifier`s correspond to
    /// linearization order (for some linearization), but this is a bit tricky
    /// to do without having to do topological sort after the fact.
    causal_links: Mutex<BTreeMap<CausalLinkIdentifier, CausalLink>>,
    // We keep this for statistics, so we don't need to acquire the lock.
    num_causal_links: AtomicUsize,
    num_cleaned_causal_links: AtomicUsize,

    /// To aid in submitting links to the `SummaryManager` and
    /// cleaning up the timeline, we keep track, for each event,
    /// of the causal links that depend on it.
    causal_link_dependencies: DashMap<LamportEvent, HashSet<CausalLinkIdentifier>>,

    /// Produces summaries of the timeline, including vector clocks.
    summaries: Arc<Mutex<SummaryManager>>,

    /// This is a DynamicTimeline, so we keep track of what is committed and
    /// what is still in flux. This is shared with the `SummaryManager`.
    commit_manager: Arc<CommitManager>,

    /// Implementation detail: data structure to construct causal links.
    /// When we encounter the Source event in a causal link, we add an entry
    /// here, so we can later attach the Target event and complete the link.
    matching_table: DashMap<EventMatchKey, CausalLinkIdentifier>,

    // For debugging and self-checking.
    #[cfg(feature = "selfcheck")]
    _traversed_in_phase_two: DashSet<LamportEvent>,

    /// To ensure every event is submitted exactly once to the summary manager.
    #[cfg(feature = "selfcheck")]
    _submitted_to_summary_manager: DashSet<LamportEvent>,
}

impl DynamicTimeline {
    pub fn new(summaries: Arc<Mutex<SummaryManager>>, commit_manager: Arc<CommitManager>) -> Self {
        Self {
            events: DashMap::new(),
            num_events: DashMap::new(),
            num_cleaned_events: AtomicUsize::new(0),
            causal_links: Mutex::new(BTreeMap::new()),
            num_causal_links: AtomicUsize::new(0),
            num_cleaned_causal_links: AtomicUsize::new(0),
            causal_link_dependencies: DashMap::new(),
            summaries,
            commit_manager,
            matching_table: DashMap::new(),
            #[cfg(feature = "selfcheck")]
            _traversed_in_phase_two: DashSet::new(),
            #[cfg(feature = "selfcheck")]
            _submitted_to_summary_manager: DashSet::new(),
        }
    }

    /// Add an event to the timeline.
    pub fn add_event(&self, ev: LamportEvent) {
        let proc: ProcessId = ev.proc();
        // Add the event to the process's event list
        let events = self
            .events
            .entry(proc)
            .or_insert(Mutex::new(BTreeSet::new()));
        let mut events = events.lock();
        events.insert(ev);

        // Keep track of the number of events per process.
        self.num_events
            .entry(proc)
            .and_modify(|e| {
                e.fetch_add(1, Ordering::Relaxed);
            })
            .or_insert(AtomicUsize::new(1));
    }

    /// Maintain auxiliary data-structure submitting and cleaning causal links.
    fn register_causal_link_dependency(&self, link: &CausalLink, id: CausalLinkIdentifier) {
        self.causal_link_dependencies
            .entry(link.source.clone())
            .or_default()
            .insert(id);

        if let Some(ref target) = link.target {
            self.causal_link_dependencies
                .entry(target.clone())
                .or_default()
                .insert(id);
        }
    }

    fn remove_causal_links_dependent_on(
        &self,
        causal_links: &mut MutexGuard<BTreeMap<CausalLinkIdentifier, CausalLink>>,
        ev: &LamportEvent,
    ) -> usize {
        let mut removed = 0;
        match self.causal_link_dependencies.entry(ev.clone()) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                for id in entry.get().iter() {
                    if causal_links.remove(id).is_some() {
                        removed += 1;
                    }
                }
                entry.remove();
            }
            dashmap::mapref::entry::Entry::Vacant(_) => {}
        };
        removed
    }

    /// Track, but do not submit to the summariser, the causal link.
    fn track_causal_link(&self, key: String, link: &CausalLink) -> CausalLinkIdentifier {
        let mut causal_links = self.causal_links.lock();

        // First, try to identify if we have an existing link for the given key.
        if let Some(existing) = self.matching_table.get(&key) {
            // If we do, then we need to update the existing link rather than
            // create a new one.
            let index = *existing;
            if link.source.is_administrative_event() {
                log::debug!(
                    "[CAUSAL] Updating causal link {} (index {}) FROM {} TO {}",
                    key,
                    index,
                    causal_links[&index],
                    link,
                );
            }
            causal_links.insert(index, link.clone());
            index
        } else {
            // Otherwise, we create a new link.
            let index = self.num_causal_links.fetch_add(1, Ordering::Relaxed);
            causal_links.insert(index, link.clone());
            if link.source.is_administrative_event() {
                log::debug!(
                    "[CAUSAL] Adding new causal link {} (index {}): {}",
                    key,
                    index,
                    link,
                );
            }
            self.matching_table.insert(key, index);
            self.register_causal_link_dependency(link, index);
            index
        }
    }

    /// Add causal links to the timeline and submit the batch of events to the `SummaryManager`.
    /// A batch that is submitted is considered "committed" and will not altered.
    fn construct_causal_links_and_commit_batch(&self) {
        // Note: we do NOT implement a general algorithm to find a linearization
        // of the causal links we construct. In our case, arrows only exist
        // between packet sends and receives, and we have a total order on
        // packets imposed by the mediator. Each packet Send and its respective
        // Receive events have a unique (and matching) `mediator_id`.

        // We proceed in three phases:
        // 1. Keep track of all potential `Source`s of causal links for this batch of events.
        // 2. Complete all causal links by matching the `Source`s with `Target`s and identifying
        //   a dependency-complete portion of the timeline that can be sent to the summariser.
        // 3. Actually submit this batch/portion of the timeline to the summariser.

        // The range of events we should process is given to us by the `CommitManager`.
        // For every process, this is the range (last_committed_event, last_seen_event].
        // Because the clocks of the nodes are not synchronized, we can have
        // a causal dependency from "the future" of node A to "the past" of node B.
        // So when we add the Sources of causal links, we peer into the future up to `clocks_uncertainty_ms`.
        // The `CommitManager` incorporates this logic by giving us two ranges, a `Source` range and
        // a prospective `Target` range (that may be too narrow and need to be extended in phase 2).

        let (source_proc_ranges, target_proc_ranges) = self.commit_manager.ranges_for_processing();

        // We only need to process events when at least one timestamp has changed in the ranges,
        // i.e. we have events after `last_committed` for at least one process.
        let something_changed = target_proc_ranges
            .iter()
            .any(|(_, (_, last_committed, have_all_before))| last_committed < have_all_before);
        if !something_changed {
            return;
        }

        // Print ranges for convenience.
        target_proc_ranges
            .iter()
            .for_each(|(proc, (_, t_start, t_end))| {
                let (_, _, s_end) = source_proc_ranges
                    .get(proc)
                    .expect("RangeMaps don't have matching keys! This should not happen.");
                log::debug!(
                    "[WINDOW] Range for process {}: from {} to {}, extending to {}.",
                    proc,
                    t_start,
                    t_end,
                    s_end,
                );
            });

        let start_phase_1 = Instant::now();
        // TODO: can we do this as we add events, rather than on every loop?
        // First pass: traverse events to identify potential `Source`s of causal links.
        // We keep track of which `Source`-links have been just added, as opposed
        // to added in previous batches, and only matched with a `Target` in this batch.
        // (In the latter case, the link will be in `causal_links` but NOT `source_links_this_run`.)
        let mut source_links_this_run: HashSet<CausalLinkIdentifier> = HashSet::new();
        for (proc, (ts_range, _, _)) in source_proc_ranges.iter() {
            let timeline = self.events.get(proc).unwrap();
            let timeline = timeline.lock();
            for ev in timeline.range(ts_range.clone()) {
                log::debug!("[TIMELINE] First pass. Traversed event {}", ev);
                // Precaution: panic if we see events with match instructions we don't yet handle.
                self.panic_on_unsupported_event_kind(ev);
                if let Some(li) = self.track_link_sources(ev) {
                    source_links_this_run.insert(li);
                }
            }
        }

        let start_phase_2 = Instant::now();

        // Second pass: traverse events until all dependencies are satisfied
        // and we have constructed (but not submitted to the summariser) all causal links.
        // This works as a fixpoint computation: we loop, traversing events, until we
        // (a) have covered all the events in `target_proc_ranges` AND
        // (b) the set of traversed events has all inter-process dependencies satisfied,
        //    i.e., e.g., all packet receives have a matching packet send.

        // Last event known to _need_ to be in the batch (per process), either because of
        // condition (a) above or because it is an inter-process `Source` of a causal
        // link that ends in a `Target` that itself is needed.
        // We are done with submitting the batch when for every process,
        // `last_traversed_ev[proc] = last_known_ev[proc]`.
        let mut last_needed_ev: HashMap<ProcessId, LamportEvent> = target_proc_ranges
            .iter()
            .filter_map(|(proc, (target_range, _, _))| {
                // Get the last event before `last_ts`. We need at least that one.
                let timeline = self.events.get(proc).unwrap();
                let timeline = timeline.lock();
                timeline
                    .range(target_range.clone())
                    .last()
                    .map(|last_ev| (*proc, last_ev.clone()))
            })
            .collect();

        // We are done (with a single process) when...
        fn done_with_proc(
            proc: ProcessId,
            resume_with: &HashMap<ProcessId, LamportEvent>,
            last_needed_ev: &HashMap<ProcessId, LamportEvent>,
        ) -> bool {
            match (resume_with.get(&proc), last_needed_ev.get(&proc)) {
                (Some(resume_with), Some(last_needed_ev)) => {
                    // there are no dependency gaps
                    resume_with == last_needed_ev
                }
                // If there's no needed event, we're done.
                (_, None) => true,
                (None, Some(_)) => panic!("Process {} not in resume_with!", proc),
            }
        }

        fn done_with_all(
            resume_with: &HashMap<ProcessId, LamportEvent>,
            last_needed_ev: &HashMap<ProcessId, LamportEvent>,
        ) -> bool {
            last_needed_ev
                .iter()
                .all(|(proc, _)| done_with_proc(*proc, resume_with, last_needed_ev))
        }

        // On every iteration of the fixpoint-loop, we resume processing from the following
        // event for each process. We should never traverse the same event twice.
        // On the first iteration, this is an artificial event.
        let mut resume_with: HashMap<ProcessId, LamportEvent> = target_proc_ranges
            .iter()
            .map(|(proc, (_, start_ts, _))| (*proc, LamportEvent::last_ev_at(*start_ts)))
            .collect();

        let processes = source_proc_ranges.keys().cloned().collect::<Vec<_>>();
        loop {
            if done_with_all(&resume_with, &last_needed_ev) {
                break;
            }

            log::debug!("Starting new iteration of outer fixpoint loop.");

            for proc in processes.iter() {
                let timeline = self.events.get(proc).unwrap();
                let timeline = timeline.lock();

                // Traverse events on this process' timeline (to discover dependencies) until we're done with it.
                while !done_with_proc(*proc, &resume_with, &last_needed_ev) {
                    log::debug!(
                        "Starting new iteration of inner fixpoint loop. Resume from: {} | Last needed: {}",
                        resume_with.get(proc).unwrap(),
                        last_needed_ev.get(proc).unwrap()
                    );

                    let range_start = resume_with.get(proc).unwrap();
                    let range_end = last_needed_ev
                        .get(proc)
                        .expect("We should have a last needed event if we're not done!");
                    assert!(
                        range_start <= range_end,
                        "Attempting to traverse backwards from {} to {}!",
                        range_start,
                        range_end
                    );
                    let ev_range = timeline.range((Excluded(range_start), Included(range_end)));

                    for ev in ev_range {
                        self.track_link_targets_and_dependencies(
                            &mut last_needed_ev,
                            &resume_with,
                            ev,
                        );
                        log::debug!(
                            "[TIMELINE] Second pass. Traversed event {} (last needed: {})",
                            ev,
                            last_needed_ev.get(proc).unwrap()
                        );
                        self._selfcheck_traverse_phase_two_unique(ev);
                        resume_with.insert(*proc, ev.clone());
                    }
                }

                if let Some(last_needed) = last_needed_ev.get(proc) {
                    log::debug!(
                        "[TIMELINE] Done with process {} (last needed: {})",
                        proc,
                        last_needed,
                    );
                }
            }
        }

        let start_phase_3 = Instant::now();

        // Third pass: go over all events in the batch and send them
        // (and their causal links) to the summariser.
        // This is the actual range that is going to be submitted.
        let batch_range = target_proc_ranges
            .iter()
            .filter_map(|(proc, (_, start_ts, _))| {
                last_needed_ev.get(proc).map(|end_ev| {
                    let end_ts = end_ev.ts();
                    (
                        *proc,
                        (
                            Excluded(LamportEvent::last_ev_at(*start_ts)),
                            Included(LamportEvent::last_ev_at(end_ts)),
                        ),
                    )
                })
            })
            .collect::<HashMap<_, _>>();

        let (num_submitted_events, num_submitted_causal_links) = self.submit_to_summariser(
            processes,
            batch_range,
            source_links_this_run,
            &last_needed_ev,
        );

        // Mark as committed all events in the range.
        let committed_up_to: Vec<(ProcessId, MonotonicTimestamp)> = target_proc_ranges
            .iter()
            .map(|(proc, (_, _, end_ts))| {
                if let Some(last_ev) = last_needed_ev.get(proc) {
                    (*proc, last_ev.ts())
                } else {
                    (*proc, *end_ts)
                }
            })
            .collect();

        if self.commit_manager.commit(committed_up_to) {
            log::info!(
                "[PERF] Submitting {} events to SummaryManager (with {} causal links) took {} ms (phase 1: {} ms | phase 2: {} ms | phase 3: {} ms).",
                num_submitted_events,
                num_submitted_causal_links,
                start_phase_1.elapsed().as_millis(),
                (start_phase_2 - start_phase_1).as_millis(),
                (start_phase_3 - start_phase_2).as_millis(),
                start_phase_3.elapsed().as_millis(),
            );
        }
    }

    /// Submit a batch of events to the summariser.
    fn submit_to_summariser(
        &self,
        processes: Vec<u8>,
        batch_range: HashMap<u8, (Bound<LamportEvent>, Bound<LamportEvent>)>,
        source_links_this_run: HashSet<usize>,
        last_needed_ev: &HashMap<u8, LamportEvent>,
    ) -> (usize, usize) {
        let start = Instant::now();
        // We need to acquire the summary lock to submit the batch.
        let mut summaries = self.summaries.lock();
        log::info!(
            "[PERF] Waited {} ms for SummaryManager lock.",
            start.elapsed().as_millis()
        );
        let causal_links = self.causal_links.lock();

        // We register the first event in a batch in a special manner, so we keep track.
        let mut first_per_process_for_batch: HashMap<ProcessId, bool> =
            processes.iter().map(|p| (*p, true)).collect();
        // The last event in the sent-to-summariser timeline for a process.
        // We need to mark the actual last event in the batch (for each process)
        // by calling `register_unfilled_same_process_dependent`.
        let mut last_submitted_ev: HashMap<ProcessId, LamportEvent> = HashMap::new();
        let mut num_submitted_events: usize = 0;
        let mut num_submitted_causal_links: usize = 0;

        // Register the processes with the summary provider.
        for proc in processes.iter() {
            summaries.register_process(*proc);
        }

        for (proc, range) in batch_range.iter() {
            let timeline = self.events.get(proc).unwrap();
            let timeline = timeline.lock();
            let ev_range = timeline.range(range.clone());

            let first_per_process_for_batch =
                first_per_process_for_batch.entry(*proc).or_insert(true);

            for ev in ev_range {
                if ev.is_administrative_event() {
                    log::debug!("[TIMELINE] Sending event {} to summariser.", ev);
                }

                num_submitted_causal_links += self.submit_links_for_event(
                    &causal_links,
                    &mut summaries,
                    &source_links_this_run,
                    ev,
                );

                summaries.register_same_process_event(ev, *first_per_process_for_batch);
                *first_per_process_for_batch = false;
                self._selfcheck_register_unique_addition(ev);

                // Record we've submitted this event
                last_submitted_ev.insert(*proc, ev.clone());
                num_submitted_events += 1;
            }
        }

        // Register the last event in the batch as having an unfilled dependent
        for (_proc, last_ev) in last_submitted_ev.iter() {
            assert_eq!(
                last_ev,
                last_needed_ev
                    .get(_proc)
                    .expect("last_needed_ev does not have this process"),
                "last_submitted_ev and last_needed_ev are inconsistent"
            );
            summaries.register_unfilled_same_process_dependent(last_ev);

            #[cfg(feature = "selfcheck")]
            {
                let timeline = self.events.get(_proc).unwrap();
                let timeline = timeline.lock();
                self._selfcheck_submitted_all_to_summariser_up_to(&timeline, Some(last_ev));
            }
        }

        (num_submitted_events, num_submitted_causal_links)
    }

    fn panic_on_unsupported_event_kind(&self, ev: &LamportEvent) {
        match ev.match_instruction() {
            Some(EventMatchInstruction(_, MatchingBehaviour::Intrinsic, ArrowPosition::Any)) => {
                todo!(
                    "[TIMELINE] Support for match instruction {} of event {} is not yet implemented.",
                    ev.match_instruction().unwrap(),
                    ev
                );
            }

            Some(EventMatchInstruction(_, MatchingBehaviour::Extrinsic, _)) => {
                todo!(
                    "[TIMELINE] Support for match instruction {} of event {} is not yet implemented.",
                    ev.match_instruction().unwrap(),
                    ev
                );
            }

            _ => {}
        }
    }

    /// Returns the causal link identifier if we added a new causal link.
    fn track_link_sources(&self, ev: &LamportEvent) -> Option<CausalLinkIdentifier> {
        match ev.bare_event() {
            Event::PacketSend { .. }
            | Event::TimelineEvent(AdministrativeEvent::EndWindow { .. }) => {
                // Add a causal link with only the source.
                let link = CausalLink::new_dangling(ev.clone());
                let EventMatchInstruction(key, mb, pos) = ev
                    .match_instruction()
                    .expect("Event must have a match instruction");
                assert!(mb == MatchingBehaviour::Intrinsic);
                assert!(pos == ArrowPosition::Source);
                let index = self.track_causal_link(key, &link);
                Some(index)
            }
            _ => None,
        }
    }

    /// Returns added: Vec<CausalLink>.
    fn attach_link_targets(&self, ev: &LamportEvent) -> Vec<CausalLink> {
        let mut added = vec![];
        let match_keys = match ev.bare_event() {
            Event::PacketReceive { .. } => {
                let EventMatchInstruction(key, mb, pos) = ev
                    .match_instruction()
                    .expect("PacketReceive must have a match instruction");
                assert!(mb == MatchingBehaviour::Intrinsic);
                assert!(pos == ArrowPosition::Target);
                vec![key]
            }
            Event::TimelineEvent(AdministrativeEvent::CollateSummaries { .. }) => {
                let mut keys = vec![];
                // Link with all known nodes (except Jepsen)
                let nodes = self.events.iter().map(|pt| *pt.key());
                for node_id in nodes {
                    // NOTE: if you decide to include Jepsen, you will need to modify
                    // `request_reward_for_window` in `feedback/mod.rs`.
                    if node_id == JEPSEN_NODE_ID {
                        continue;
                    }

                    let EventMatchInstruction(key, mb, pos) = ev
                        .match_instruction_for_node(node_id)
                        .expect("CollateSummaries must have a node-match instruction");
                    assert!(mb == MatchingBehaviour::Intrinsic);
                    assert!(pos == ArrowPosition::Target);
                    keys.push(key);
                }
                keys
            }
            _ => return added,
        };

        let mut causal_links = self.causal_links.lock();
        for match_key in match_keys {
            if let Some(kv) = self.matching_table.get(&match_key) {
                let index = *kv;
                // There is a Source for which we are the Target. Update that link and submit it.
                if let Some(old_link) = causal_links.get(&index) {
                    // Here, we accept same-process causal links, but later (in `submit_link`)
                    // we ensure these are never sent to the summariser.
                    let new_link = old_link.with_target(ev);
                    self.register_causal_link_dependency(&new_link, index);
                    causal_links.insert(index, new_link.clone());
                    added.push(new_link);
                };
            } else {
                // We are the Target of a Source that we don't (yet) know about,
                // but the assumption is that this function gets called with the
                // causal dependencies of all events fulfilled (i.e., only with
                // committed events.)

                // I think this happens because of [batch_before_packet].
                if ev.is_packet_receive() {
                    log::warn!(
                    "[TIMELINE] Target event {} was matched without its dependent (with key {}) being matched.",
                    ev, match_key
                );
                } else {
                    panic!(
                    "[TIMELINE] Target event {} was matched without its dependent (with key {}) being matched.",
                    ev, match_key
                );
                }
            }
        }
        added
    }

    fn track_link_targets_and_dependencies(
        &self,
        last_needed_ev: &mut HashMap<ProcessId, LamportEvent>,
        resume_with: &HashMap<ProcessId, LamportEvent>,
        ev: &LamportEvent,
    ) {
        // Track link targets...
        let matches = self.attach_link_targets(ev);
        // ...and determine if we have a dependency gap we
        // must process up to (i.e. update `last_known_ev`)
        for cl in matches {
            // This event happened-before `ev`, so we need to (recursively)
            // pull-in all the history before `src_ev` into the timeline.
            let src_ev = cl.source;
            let src_proc = src_ev.proc();
            // Update `last_known_ev`
            // The tricky case is when there's no `last_needed_ev` for the source process.
            // Then, we essentially do a no-op -- the last needed is whatever was just
            // traversed (by default), and we take the max of *that* and the Source.
            let last_known_ev = last_needed_ev.entry(src_proc).or_insert_with(|| {
                resume_with
                    .get(&src_proc)
                    .expect("Process must have a resume-with event")
                    .clone()
            });
            *last_known_ev = std::cmp::max(last_known_ev.clone(), src_ev);
        }
    }

    fn submit_link(
        &self,
        summaries: &mut MutexGuard<SummaryManager>,
        link: &CausalLink,
        link_index: CausalLinkIdentifier,
        completed_past_link: bool,
    ) {
        if link.source.is_administrative_event() {
            log::debug!(
                "[CAUSAL] Submitting {} link ID {} {}",
                if completed_past_link {
                    "(completed)"
                } else {
                    "(new)"
                },
                link_index,
                link,
            );
        }
        if let Some(ref target) = link.target {
            // If this is a link that we've just completed, i.e. it was previously
            // registered as having an unfilled dependent in the `SummaryManager`,
            // we first need to remove that link to infinity.
            if completed_past_link {
                summaries.deregister_unfilled_interprocess_dependent(&link.source);
            }

            // We still need to deregister the unfilled dependent if the target
            // is on the same process, as when we created the `Source` link
            // we didn't know, so this code is after the above `if` block.
            let same_process = link.source.proc() == target.proc();
            if same_process {
                // We don't send same-process causal links to the summariser.
                return;
            }

            summaries.register_interprocess_dependency(target, &link.source);
        } else {
            // Don't register unfilled dependents for packets that were dropped
            // (i.e. we know they will never be received)
            if !link.source.is_dropped_packet_send() {
                summaries.register_unfilled_interprocess_dependent(&link.source);
            }
        }
    }

    fn submit_links_for_event(
        &self,
        causal_links: &MutexGuard<BTreeMap<usize, CausalLink>>,
        summaries: &mut MutexGuard<SummaryManager>,
        source_links_this_run: &HashSet<CausalLinkIdentifier>,
        ev: &LamportEvent,
    ) -> usize {
        let mut num_links_submitted = 0;
        if let Some(cls) = self.causal_link_dependencies.get(ev) {
            let cls = cls.value();
            for cl_id in cls {
                let link = causal_links.get(cl_id);

                match link {
                    Some(link) => {
                        // Is this a link that has been dangling for a while, and we've just
                        // matched it? Then we need to handle it specially.
                        let completed_past_link = !source_links_this_run.contains(cl_id);
                        self.submit_link(summaries, link, *cl_id, completed_past_link);

                        num_links_submitted += 1;
                    }
                    None => {
                        log::error!(
                            "[CAUSAL] Causal link {} has no entry in causal_links.",
                            cl_id
                        );
                    }
                }
            }
        }
        num_links_submitted
    }

    /// Clean up all events before the given markers, as long as they're committed.
    pub fn clean_up_all_committed_before(
        &self,
        mut cleanup_markers: HashMap<ProcessId, MonotonicTimestamp>,
    ) {
        // We can store the schedule timeline on disk at this point, if we want to.

        let start = Instant::now();
        let mut num_events_removed: usize = 0;
        let mut num_links_removed: usize = 0;

        // VERY IMPORTANT: never clean-up uncomitted events!
        let committed_upto = self.commit_manager.can_cleanup_upto();
        for (proc, cleanup_ts) in cleanup_markers.iter_mut() {
            if let Some(committed_ts) = committed_upto.get(proc) {
                *cleanup_ts = std::cmp::min(*cleanup_ts, *committed_ts);
            } else {
                // If a process hasn't committed anything, we can't clean up anything.
                return;
            }
        }

        for (proc, cleanup_ts) in cleanup_markers.iter() {
            if let Some(timeline) = self.events.get(proc) {
                let mut timeline = timeline.lock();
                let mut causal_links = self.causal_links.lock();
                while let Some(ev) = timeline.pop_first() {
                    if ev.ts() >= *cleanup_ts {
                        timeline.insert(ev);
                        break;
                    }
                    self._selfcheck_only_cleanup_submitted(&ev);
                    num_events_removed += 1;
                    // FIXME: are we remove causal links when we shouldn't be?
                    num_links_removed +=
                        self.remove_causal_links_dependent_on(&mut causal_links, &ev);
                }
            }
        }

        self.num_cleaned_events
            .fetch_add(num_events_removed, Ordering::Relaxed);
        self.num_cleaned_causal_links
            .fetch_add(num_links_removed, Ordering::Relaxed);

        log::info!(
            "[PERF][CLEANUP] Cleaning up {} events (including {} causal links) took {}ms. Cleaned up before: {:?}",
            num_events_removed,
            num_links_removed,
            start.elapsed().as_millis(),
            cleanup_markers,
        );
    }

    /// Declare that we will not add any more "normal" events to `proc`'s timeline before `ts`.
    pub fn have_events_before(&self, have: Vec<(ProcessId, MonotonicTimestamp)>) {
        self.commit_manager.have_events_before(have);
    }

    /// Declare that we have added the windows up to `ts`.
    pub fn declare_window(&self, window: WindowDescriptor) {
        self.commit_manager.declare_window(window);
    }

    pub fn tick(&self) {
        self.construct_causal_links_and_commit_batch();
    }

    /// Print a summary of this history.
    pub fn print_summary(&self) {
        let mut event_counter = 0;
        for r in self.num_events.iter() {
            event_counter += r.value().load(Ordering::Relaxed);
        }

        let links_counter = self.num_causal_links.load(Ordering::Relaxed);
        let process_counter = self.events.len();

        let cleaned_events = self.num_cleaned_events.load(Ordering::Relaxed);
        let cleaned_links = self.num_cleaned_causal_links.load(Ordering::Relaxed);

        let stored_events = event_counter - cleaned_events;
        let stored_links = links_counter - cleaned_links;

        log::info!(
            "[STATS][HISTORY] {} stored events ({} total) / {} stored causal links ({} total) (across {} processes)\n{:?}",
            stored_events,
            event_counter,
            stored_links,
            links_counter,
            process_counter,
            self.num_events,
        );
        if let Ok(summaries) = self.summaries.try_lock() {
            summaries.print_summary();
        }
    }

    // Debugging and self-check.
    /// To check that all events are traversed (in phase two) exactly once.
    fn _selfcheck_traverse_phase_two_unique(&self, ev: &LamportEvent) {
        #[cfg(feature = "selfcheck")]
        {
            if self._traversed_in_phase_two.contains(ev) {
                panic!("[SELFCHECK] Event {} already traversed in phase two!", ev);
            }
            self._traversed_in_phase_two.insert(ev.clone());
        }
    }

    /// To check that we don't clean up events that we haven't submitted to the summariser.
    fn _selfcheck_only_cleanup_submitted(&self, ev: &LamportEvent) {
        #[cfg(feature = "selfcheck")]
        {
            if !self._submitted_to_summary_manager.contains(ev) {
                panic!(
                    "[SELFCHECK] Event {} not submitted to summary manager, but we're cleaning it up!",
                    ev
                );
            }
        }
    }

    /// To check that all events are submitted exactly once to the summary manager.
    fn _selfcheck_register_unique_addition(&self, ev: &LamportEvent) {
        #[cfg(feature = "selfcheck")]
        {
            if self._submitted_to_summary_manager.contains(ev) {
                panic!(
                    "[SELFCHECK] Event {} already submitted to summary manager!",
                    ev
                );
            }
            self._submitted_to_summary_manager.insert(ev.clone());
        }
    }

    /// Check that all events in the timeline up to the given one were submitted to the summariser.
    fn _selfcheck_submitted_all_to_summariser_up_to(
        &self,
        timeline: &MutexGuard<BTreeSet<LamportEvent>>,
        last_ev: Option<&LamportEvent>,
    ) {
        #[cfg(feature = "selfcheck")]
        {
            if let Some(last_ev) = last_ev {
                let proc = last_ev.proc();
                let _whole =
                    LamportEvent::first_ev_at(MonotonicTimestamp::zero(proc))..=last_ev.clone();
                for ev in timeline.range(_whole) {
                    if !self._submitted_to_summary_manager.contains(ev) {
                        panic!("[SELFCHECK] Event {} not submitted to summary manager!", ev);
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowDescriptor {
    schedule_id: ScheduleId,
    step_id: StepId,

    markers: HashMap<ProcessId, (MonotonicTimestamp, MonotonicTimestamp)>,
}

impl WindowDescriptor {
    pub fn new(schedule_id: ScheduleId, step_id: StepId) -> Self {
        Self {
            schedule_id,
            step_id,
            markers: HashMap::new(),
        }
    }

    pub fn add_marker(
        &mut self,
        proc: ProcessId,
        start: MonotonicTimestamp,
        end: MonotonicTimestamp,
    ) {
        self.markers.insert(proc, (start, end));
    }
}

/// Helper struct for `Timeline`, to keep track of which parts of the
/// timeline are settled/committed and which are still being populated.
pub struct CommitManager {
    /// We will not receive any new events for `ProcessId` before this timestamp.
    /// This is a per-process measure of completeness of the timeline.
    have_all_events_before: Mutex<HashMap<ProcessId, MonotonicTimestamp>>,

    /// The scheduler divides the timeline into windows. We only mark
    /// events as available for causal link processing after
    /// the scheduler adds the window boundary events. This tells us
    /// the last window boundary event we have received.
    have_windows_before: Mutex<HashMap<ProcessId, MonotonicTimestamp>>,

    /// A process is "committed" up to a timestamp `ts` if the timeline has received
    /// all events of that processed up to `ts` AND all the events that happened-before
    /// that timestamp, including events from other processes.
    /// This is a global measure of completeness of the timeline.
    /// In effect, it is the timeline's vector clock.
    ///
    /// Only the DynamicTimeline can advance this, since only it knows about the causal
    /// dependencies between events.
    committed_up_to: Mutex<HashMap<ProcessId, MonotonicTimestamp>>,

    clocks: Arc<RwLock<ClockManager>>,
}

type RangeMap = HashMap<
    ProcessId,
    (
        (Bound<LamportEvent>, Bound<LamportEvent>),
        MonotonicTimestamp,
        MonotonicTimestamp,
    ),
>;

impl CommitManager {
    pub fn new(clocks: Arc<RwLock<ClockManager>>) -> Self {
        Self {
            have_all_events_before: Mutex::new(HashMap::new()),
            committed_up_to: Mutex::new(HashMap::new()),
            have_windows_before: Mutex::new(HashMap::new()),
            clocks,
        }
    }

    pub fn can_cleanup_upto(&self) -> HashMap<ProcessId, MonotonicTimestamp> {
        let committed_up_to = self.committed_up_to.lock();
        committed_up_to.clone()
    }

    /// Declare that we will not add any more "normal" events to the respective
    /// processes' timelines before the given timestamps.
    fn have_events_before(&self, have: Vec<(ProcessId, MonotonicTimestamp)>) {
        let mut have_all_events_before = self.have_all_events_before.lock();
        for (proc, ts) in have {
            let mut entry = have_all_events_before.entry(proc).or_insert(ts);
            if ts >= *entry {
                *entry = ts;
            } else {
                log::error!(
                    "[TIMELINE] Trying to watermark up to timestamp {:?} below previously-watermarked: {:?}",
                    ts,
                    *entry
                );
            }
        }
    }

    fn declare_window(&self, window: WindowDescriptor) {
        let mut have_windows_before = self.have_windows_before.lock();
        // Set `have_windows_before` as appropriate.
        for (proc, (_start, end)) in &window.markers {
            let entry = have_windows_before.entry(*proc).or_insert(*end);
            if *end >= *entry {
                *entry = *end;
            } else {
                log::error!(
                    "[TIMELINE] Trying to window-watermark up to timestamp {:?} below previously-watermarked: {:?}",
                    end,
                    *entry
                );
            }
        }
    }

    /// Returns `true` if anything changed in the committed state.
    fn commit(&self, committed: Vec<(ProcessId, MonotonicTimestamp)>) -> bool {
        let mut committed_up_to = self.committed_up_to.lock();
        // Keep track of what values we've updated from, so we can display the commit nicely.
        let mut updated_from = HashMap::new();

        for (proc, ts) in committed {
            let entry = committed_up_to.entry(proc);
            match entry {
                Entry::Vacant(entry) => {
                    entry.insert(ts);
                    updated_from.insert(proc, MonotonicTimestamp::zero(proc));
                }
                Entry::Occupied(mut entry) => {
                    let old_ts = entry.get();
                    match ts.cmp(old_ts) {
                        std::cmp::Ordering::Greater => {
                            updated_from.insert(proc, *old_ts);
                            entry.insert(ts);
                        }
                        std::cmp::Ordering::Less => {
                            panic!(
                            "[TIMELINE] Trying to commit up to {} below previously-committed: {}",
                            ts, old_ts
                        );
                        }
                        _ => {}
                    }
                }
            }
        }
        let any_updated = !updated_from.is_empty();
        if any_updated {
            let mut all_procs = committed_up_to.keys().cloned().collect::<Vec<_>>();
            all_procs.sort_unstable();
            let str = all_procs
                .iter()
                .map(|proc| {
                    let now_val = committed_up_to.get(proc).unwrap();
                    let diff = if let Some(old_val) = updated_from.get(proc) {
                        format!("{} -> {} (updated)", old_val, now_val)
                    } else {
                        format!("{} (unchanged)", now_val)
                    };
                    format!("{}: {}", proc, diff)
                })
                .collect::<Vec<_>>()
                .join("\n");
            log::info!("[COMMIT] Commit:\n{}", str);
        }

        any_updated
    }

    /// Based on `have_all_events_before`, determine which ranges the
    /// DynamicTimeline should inspect to determine the commit markers.
    fn ranges_for_processing(&self) -> (RangeMap, RangeMap) {
        // The idea is that `have_all_events_before`, combined with
        // `clocks_uncertainty_ms, gievs us a bound on the causal
        // structure that the DynamicTimeline attempts to reconstruct.
        // More precisely, if the difference between two nodes'
        // timestamps is less than `clocks_uncertainty_ms`, then
        // if all processes say they have all events before (absolute)
        // `min_global_ts`, the following are true:
        //   (a) All events that "actually" happened before
        //      `X = min_global_ts - clocks_uncertainty_ms` are in the timeline,
        //      and moreover, are in the timeline before `T = min_global_ts`;
        //   (b) In particular, all causal dependencies of events before
        //      X are in the timeline before T. Therefore, we can inspect
        //      the timeline up to T for Source of causal links and match
        //      them to Targets before X. If an event matches (i.e. is a Source),
        //      then commit _it_ as well as its dependencies.
        //   (c) Ilya: this cycle of recursively finding dependencies should
        //      break quickly -- essentially, it can only happen at most once
        //      per process (otherwise, you would have cycles in the causal graph).

        let have_all_events_before = self.have_all_events_before.lock();
        let have_windows_before = self.have_windows_before.lock();
        let committed_up_to = self.committed_up_to.lock();

        // Step 1: compute the minimum (absolute) timestamp across all processes.
        // We wait for window markers to be added to the timeline
        // before we start processing the timeline, so we do not consider we
        // `have_all_events_before` for a process until we have received window
        // markers for that process.
        let clocks = self.clocks.read();
        let abs_timestamps = have_all_events_before
            .iter()
            .map(|(node, rel_ts)| {
                let window_before = match have_windows_before.get(node) {
                    Some(ts) => *ts,
                    None => MonotonicTimestamp::zero(*node),
                };
                let min_ts = std::cmp::min(*rel_ts, window_before);
                clocks.get_abs_if_registered(*node, min_ts)
            })
            .collect::<Vec<_>>();
        let all_registered =
            !abs_timestamps.is_empty() && abs_timestamps.iter().all(|x| x.is_some());
        let min_global_ts_ns = if all_registered {
            Some(
                abs_timestamps
                    .iter()
                    .map(|x| x.unwrap())
                    .min()
                    .expect("Empty vector of timestamps"),
            )
        } else {
            None
        };

        // Step 2: convert `min_global_ts_ns` to relative timestamps for each process.
        let mut source_ranges = HashMap::new();
        let mut target_ranges = HashMap::new();

        if let Some(min_global_ts_ns) = min_global_ts_ns {
            for (proc, _) in have_all_events_before.iter() {
                let last_committed = match committed_up_to.get(proc) {
                    Some(ts) => *ts,
                    None => MonotonicTimestamp::zero(*proc),
                };

                let local_watermark = clocks
                    .get_rel_if_registered(*proc, min_global_ts_ns)
                    .expect("`all_registered` is true, so this should not fail.");

                let source_end = local_watermark;
                let target_end = (Duration::nanoseconds(min_global_ts_ns)
                    - Duration::milliseconds(clocks.synchronized_to_within_ms as i64))
                .num_nanoseconds()
                .unwrap_or(0);
                assert!(target_end >= 0, "target_end should be non-negative");
                let target_end = clocks
                    .get_rel_if_registered(*proc, target_end)
                    .expect("`all_registered` is true, so this should not fail.");

                assert!(
                    source_end >= target_end,
                    "source_end ({}) should be >= target_end ({}). In our experience, this \
                     occurs when the coverage-server is not running.",
                    source_end,
                    target_end
                );

                assert!(
                    source_end >= last_committed,
                    "source_end ({}) should be >= last_committed ({})",
                    source_end,
                    last_committed
                );

                // Look for Sources from `last_committed` to `min_global_ts_ns`.
                let source_range = (
                    Excluded(LamportEvent::last_ev_at(last_committed)),
                    Included(LamportEvent::last_ev_at(source_end)),
                );
                source_ranges.insert(*proc, (source_range, last_committed, source_end));

                // Targets are from `last_committed` to `min_global_ts_ns - clocks_uncertainty_ms`.

                // We need to ensure that the target range is well-formed, i.e.
                // `target_end >= last_committed`. When we commit after the
                // window, `target_end` will fall behind `last_committed`.
                // In this case, we set `target_end` to `last_committed`.
                let target_end = std::cmp::max(target_end, last_committed);
                let target_range = (
                    Excluded(LamportEvent::last_ev_at(last_committed)),
                    Included(LamportEvent::last_ev_at(target_end)),
                );
                target_ranges.insert(*proc, (target_range, last_committed, target_end));
            }
        }

        (source_ranges, target_ranges)
    }
}
