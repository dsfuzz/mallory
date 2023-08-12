/****************
 * VECTOR CLOCK *
 ****************/

use std::fmt;

use hashbrown::HashMap;

use crate::{
    event::{LamportEvent, MonotonicTimestamp, ProcessId},
    feedback::reward::RewardFunction,
};

use super::SummaryProducer;

type SequenceNumber = usize;

#[derive(Clone)]
pub struct VectorClock {
    /// Vector with MonotonicTimestamps.
    pub v_ts: HashMap<ProcessId, MonotonicTimestamp>,

    /// Vector with consecutive integer IDs.
    pub v: HashMap<ProcessId, SequenceNumber>,
}

impl fmt::Display for VectorClock {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut procs = self.v_ts.keys().cloned().collect::<Vec<_>>();
        let last_idx = procs.len() - 1;
        procs.sort_unstable();
        // We produce JSON output that can be consumed by ShiViz.
        write!(f, "{{")?;
        for (i, proc) in procs.iter().enumerate() {
            let separator = if i == last_idx { "" } else { ", " };
            write!(f, "\"{}\": {}{}", proc, self.get_seqnum(*proc), separator)?
        }
        write!(f, "}}")?;
        Ok(())
    }
}

impl fmt::Debug for VectorClock {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut procs = self.v_ts.keys().cloned().collect::<Vec<_>>();
        let last_idx = procs.len() - 1;
        procs.sort_unstable();
        write!(f, "VC[")?;
        for (i, proc) in procs.into_iter().enumerate() {
            let separator = if i == last_idx { "" } else { ", " };
            write!(
                f,
                "{}: {} ({}){}",
                proc,
                self.get_seqnum(proc),
                self.get_ts(proc),
                separator
            )?
        }
        write!(f, "]")?;
        Ok(())
    }
}

impl VectorClock {
    pub fn get_ts(&self, proc: ProcessId) -> MonotonicTimestamp {
        *self
            .v_ts
            .get(&proc)
            .unwrap_or(&MonotonicTimestamp::from(0, proc))
    }

    pub fn get_seqnum(&self, proc: ProcessId) -> SequenceNumber {
        *self.v.get(&proc).unwrap_or(&0)
    }

    /// This is an internal helper function. Use update_seqnum_and_ts instead!
    fn _merge_update_ts(&mut self, proc: ProcessId, ts: MonotonicTimestamp) -> bool {
        let old_ts = self.get_ts(proc);
        if ts > old_ts {
            self.v_ts.insert(proc, ts);
            return true;
        }
        false
    }

    fn update_seqnum_and_ts(&mut self, proc: ProcessId, ts: MonotonicTimestamp) {
        let updated = self._merge_update_ts(proc, ts);
        if updated {
            let new_seqnum = self.v.get(&proc).unwrap_or(&0) + 1;
            self.v.insert(proc, new_seqnum);
        }
    }

    fn merge_seqnum_and_ts(&mut self, other: &Self) {
        // It suffices to only inspect incoming's keys, as those are the only
        // ones that can make us change.
        for proc in other.v_ts.keys() {
            let other_ts = other.get_ts(*proc);
            self._merge_update_ts(*proc, other_ts);
            let new_seqnum = std::cmp::max(self.get_seqnum(*proc), other.get_seqnum(*proc));
            self.v.insert(*proc, new_seqnum);
        }
    }

    /// Pairwise maximum of the components, where missing components are treated as 0,
    /// and return a vector of compoents that changed.
    pub fn merge_report(&mut self, incoming: &VectorClock) -> Vec<ProcessId> {
        // It suffices to only inspect incoming's keys, as those are the only
        // ones that can make us change.
        let mut changed = vec![];
        for proc in incoming.v_ts.keys() {
            if self._merge_update_ts(*proc, incoming.get_ts(*proc)) {
                changed.push(*proc);
            }
        }
        changed
    }
}

impl SummaryProducer for VectorClock {
    fn new() -> VectorClock {
        VectorClock {
            v_ts: HashMap::new(),
            v: HashMap::new(),
        }
    }

    fn reset(&mut self) {
        self.v_ts.clear();
        self.v.clear();
    }

    /// Update the timestamp for a process. If in the past, do nothing.
    fn update(&mut self, new_event: &LamportEvent, _unused_var: f64) {
        let proc = new_event.proc();
        let ts = new_event.ts();
        self.update_seqnum_and_ts(proc, ts);
    }

    /// Pairwise maximum of the components, where missing components are treated as 0.
    fn merge(&mut self, _this_event: &LamportEvent, other: &Self, _other_event: &LamportEvent) {
        self.merge_seqnum_and_ts(other);
    }

    /// Union doesn't really make sense for vector clocks, but eh...
    fn union(&mut self, other: &Self) {
        self.merge_seqnum_and_ts(other);
    }
}

impl RewardFunction for VectorClock {}
