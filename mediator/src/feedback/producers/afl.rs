/********************************
 * AFL branch coverage feedback *
 ********************************/

use std::{fmt, io::Cursor};

use hashbrown::{HashMap, HashSet};
use murmur3::murmur3_32;

use crate::{
    event::{AdministrativeEvent, BranchHitCount, BranchId, Event, LamportEvent, ProcessId},
    feedback::{
        producers::SummaryProducerIdentifier,
        reward::{RewardEntry, RewardFunction, SummaryTask},
    },
    nemesis::schedules::StepId,
};

use super::SummaryProducer;

pub const AFL_SHM_MAP_SIZE: usize = 1 << 16;
type SummaryEntry = u32;

#[derive(Clone, PartialEq, Eq, Debug, PartialOrd, Ord, Hash)]
pub enum AFLEventKind {
    AFLBranchEvent { branch_events: Vec<BranchHitCount> },
    ResetSummary,
}

impl AFLEventKind {
    fn from_event(event: &LamportEvent) -> Option<AFLEventKind> {
        match event.bare_event() {
            Event::AFLHitCount { branch_events } => Some(AFLEventKind::AFLBranchEvent {
                branch_events: branch_events.clone(),
            }),
            Event::TimelineEvent(AdministrativeEvent::StartWindow { .. }) => {
                Some(AFLEventKind::ResetSummary)
            }
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct AFLBranchFeedback {
    pub summary_queue: HashSet<SummaryEntry>,
    // TODO: have a single HashMap<ProcessId, HashMap<BranchId, BranchHitCount>>
    // instead of two HashMaps?
    pub node_covered_branches: HashMap<ProcessId, HashSet<BranchId>>,
    pub node_branch_coverage: HashMap<ProcessId, Vec<BranchHitCount>>,
}

impl fmt::Display for AFLBranchFeedback {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "AFL[")?;

        let covered_branches = self.node_covered_branches.values().fold(
            HashSet::new(),
            |mut acc: HashSet<BranchId>, branches| {
                acc.extend(branches.iter().cloned());
                acc
            },
        );

        writeln!(
            f,
            "AFLSummary: {} branches / {} unique-vectors",
            covered_branches.len(),
            self.summary_queue.len()
        )?;
        write!(f, "]")?;
        Ok(())
    }
}

impl SummaryProducer for AFLBranchFeedback {
    fn new() -> Self {
        AFLBranchFeedback {
            summary_queue: HashSet::new(),
            node_covered_branches: HashMap::new(),
            node_branch_coverage: HashMap::new(),
        }
    }

    fn reset(&mut self) {
        self.node_covered_branches.clear();
        self.node_branch_coverage.clear();
    }

    fn update(&mut self, new_event: &LamportEvent, _unused_var: f64) {
        if let Some(bare_ev) = AFLEventKind::from_event(new_event) {
            if bare_ev == AFLEventKind::ResetSummary {
                self.reset();
                return;
            }

            if let AFLEventKind::AFLBranchEvent { branch_events } = bare_ev {
                let proc = new_event.proc();

                let node_covered_branches = self.node_covered_branches.entry(proc).or_default();

                for (branch_id, hitcount) in branch_events.iter().enumerate() {
                    if *hitcount > 0 {
                        let node_branch_coverage = self
                            .node_branch_coverage
                            .entry(proc)
                            .or_insert(vec![0; AFL_SHM_MAP_SIZE]);
                        node_branch_coverage[branch_id] += *hitcount;

                        if !node_covered_branches.contains(&(branch_id as BranchId)) {
                            node_covered_branches.insert(branch_id as BranchId);
                        }
                    }
                }
            }
        }
    }

    fn merge(&mut self, this_event: &LamportEvent, other: &Self, other_event: &LamportEvent) {
        if matches!(
            this_event.bare_event(),
            Event::TimelineEvent(AdministrativeEvent::CollateSummaries { .. })
        ) {
            // Merge seed queue
            self.summary_queue
                .extend(other.summary_queue.iter().cloned());

            // Merge covered branches
            for (proc, covered_branches) in other.node_covered_branches.iter() {
                let our_covered_branches = self.node_covered_branches.entry(*proc).or_default();
                our_covered_branches.extend(covered_branches.iter().cloned());
            }

            // Merge branch coverage
            for (proc, branch_coverage) in other.node_branch_coverage.iter() {
                let our_branch_coverage = self
                    .node_branch_coverage
                    .entry(*proc)
                    .or_insert(vec![0; AFL_SHM_MAP_SIZE]);

                for (branch_id, hitcount) in branch_coverage.iter().enumerate() {
                    our_branch_coverage[branch_id] =
                        std::cmp::max(our_branch_coverage[branch_id], *hitcount);
                }
            }
        }
    }

    fn union(&mut self, other: &Self) {
        // Union seed queue by including the current queue entry
        let mut other_global_branch: Vec<BranchHitCount> = vec![0; AFL_SHM_MAP_SIZE];
        for (_proc, branch_coverage) in other.node_branch_coverage.iter() {
            other_global_branch = other_global_branch
                .iter()
                .zip(branch_coverage.iter())
                .map(|(a, b)| a + b)
                .collect();
        }
        let mut other_global_branch_cursor = Cursor::new(&other_global_branch);
        let checksum = murmur3_32(&mut other_global_branch_cursor, 0).unwrap();
        self.summary_queue.insert(checksum);
        self.summary_queue.extend(other.summary_queue.iter().cloned());

        // Virgin branches
        for (proc, node_covered_branches) in other.node_covered_branches.iter() {
            let our_node_covered_branches = self.node_covered_branches.entry(*proc).or_default();
            our_node_covered_branches.extend(node_covered_branches.iter().cloned());
        }

        // Global branch coverage
        // The accumeulated branch coverage is not useful until now.
        // So we skip to union it.
    }
}

impl RewardFunction for AFLBranchFeedback {
    fn reward_function(
        task: SummaryTask,
        overall_cumulative: &Self,
        _schedule_cumulative: &Self,
        _schedule_window_summaries: &HashMap<StepId, Self>,
        current: &mut Self,
        _state_similarity_threshold: f64,
    ) -> RewardEntry {
        // Increasing branch hit count is one feedback metric of AFL.
        // So we also involve it here.
        let mut current_global_branch: Vec<BranchHitCount> = vec![0; AFL_SHM_MAP_SIZE];
        for (_proc, branch_coverage) in current.node_branch_coverage.iter() {
            current_global_branch = current_global_branch
                .iter()
                .zip(branch_coverage.iter())
                .map(|(a, b)| a + b)
                .collect();
        }

        let mut current_global_branch_cursor = Cursor::new(&current_global_branch);
        let checksum = murmur3_32(&mut current_global_branch_cursor, 0).unwrap();
        let if_increased_hitted_count = overall_cumulative.summary_queue.contains(&checksum);

        // Merge all hashsets of each node into one
        let current_hitted_branch: HashSet<u16> = current.node_covered_branches.iter().fold(
            HashSet::new(),
            |mut acc, (_, branch_coverage)| {
                acc.extend(branch_coverage.iter());
                acc
            },
        );

        let accum_hitted_branch: HashSet<u16> = overall_cumulative
            .node_covered_branches
            .iter()
            .fold(HashSet::new(), |mut acc, (_, branch_coverage)| {
                acc.extend(branch_coverage.iter());
                acc
            });

        let newly_hitted_branch = current_hitted_branch
            .difference(&accum_hitted_branch)
            .count();

        let reward_value = {
            if if_increased_hitted_count {
                -1
            } else {
                // Write back the new SummaryEntry into summary_queue
                match newly_hitted_branch {
                    0 => 0,
                    1..=8 => 1,
                    9..=32 => 2,
                    _ => 3,
                }
            }
        };

        let reward = RewardEntry::new(
            SummaryProducerIdentifier::AFLBranchFeedback,
            task,
            reward_value,
        );
        log::info!(
            "[REWARD][AFLSummary] Reward: {:?} ({} newly branch hitted, {}/{} hit count increased)",
            reward,
            newly_hitted_branch,
            if_increased_hitted_count,
            overall_cumulative.summary_queue.len()
        );
        reward
    }
}
