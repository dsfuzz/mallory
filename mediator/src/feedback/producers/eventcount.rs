/***************
 * EVENT COUNT *
 ***************/

use std::fmt;

use hashbrown::HashMap;

use crate::{
    event::{LamportEvent, ProcessId},
    feedback::{reward::RewardFunction},
};

use super::SummaryProducer;

#[derive(Clone)]
pub struct EventCount {
    count: HashMap<ProcessId, usize>,
}

impl SummaryProducer for EventCount {
    fn new() -> Self {
        EventCount {
            count: HashMap::new(),
        }
    }

    fn reset(&mut self) {
        self.count.clear();
    }

    fn update(&mut self, new_event: &LamportEvent, _unused_var: f64) {
        let count = self.count.entry(new_event.proc()).or_insert(0);
        *count += 1;
    }

    fn merge(&mut self, this_event: &LamportEvent, other: &Self, other_event: &LamportEvent) {
        for (proc, count) in other.count.iter() {
            let our_count = self.count.entry(*proc).or_insert(0);
            *our_count = std::cmp::max(*our_count, *count);
        }
    }

    fn union(&mut self, other: &Self) {
        for (proc, count) in other.count.iter() {
            let our_count = self.count.entry(*proc).or_insert(0);
            *our_count += *count;
        }
    }
}

impl Default for EventCount {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for EventCount {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut procs = self.count.keys().cloned().collect::<Vec<_>>();
        let mut total: usize = 0;
        procs.sort_unstable();
        write!(f, "EC[")?;
        for proc in procs {
            let c = self.count.get(&proc).unwrap();
            total += c;
            write!(f, "{}: {}, ", proc, c)?
        }
        write!(f, "total: {}", total)?;
        write!(f, "]")?;
        Ok(())
    }
}

impl RewardFunction for EventCount {}
