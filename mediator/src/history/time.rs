use std::fmt;
use std::ops::{Add, Sub};

use crate::event::NodeId;

use chrono::{DateTime, TimeZone, Utc};
use hashbrown::HashMap;
use nix::time;
use state::Storage;

// This module helps us be resilient to clock changes (which nemeses can perform)
// AND helps us convert MonotonicTimestamps from different nodes into
// AbsoluteTimestamps for that node.

static OWN_CLOCK: Storage<ClockDescriptor> = Storage::new();

pub type UnsourcedMonotonicTimestamp = u64;
pub type TimestampOffset = i128;

/// Per-node logical clock. Logical clocks belonging to the same node are
/// comparable. Logical clocks belonging to different nodes are not comparable.
/// In the actual implementation, this is a CLOCK_MONOTONIC timestamp with
/// nanosecond precision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MonotonicTimestamp {
    pub ts: UnsourcedMonotonicTimestamp,
    pub source: NodeId,
}

impl MonotonicTimestamp {
    pub const fn from(ts: u64, source: NodeId) -> Self {
        Self { ts, source }
    }

    pub const fn at_infinity(source: NodeId) -> Self {
        Self::from(std::u64::MAX, source)
    }

    pub fn zero(source: NodeId) -> Self {
        Self::from(0, source)
    }

    pub fn is_zero(&self) -> bool {
        self.ts == 0
    }
}

impl fmt::Display for MonotonicTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.ts)
    }
}

impl Ord for MonotonicTimestamp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.source != other.source {
            panic!(
                "Cannot compare timestamps from different nodes! Tried to compare {} and {}",
                self.source, other.source
            );
        }
        self.ts.cmp(&other.ts)
    }
}

impl PartialOrd for MonotonicTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.source != other.source {
            None
        } else {
            Some(self.cmp(other))
        }
    }
}

impl Add<TimestampOffset> for MonotonicTimestamp {
    type Output = MonotonicTimestamp;

    fn add(self, rhs: TimestampOffset) -> Self::Output {
        let ts: Result<UnsourcedMonotonicTimestamp, _> = (self.ts as TimestampOffset)
            .checked_add(rhs)
            .expect("Addition of offset over/underflowed!")
            .try_into();

        match ts {
            Ok(ts) => MonotonicTimestamp {
                ts,
                source: self.source,
            },
            Err(err) => {
                // Implement "wrapping"
                let wrapped_ts = if rhs < 0 { 0 } else { std::u64::MAX };
                let res = MonotonicTimestamp {
                    ts: wrapped_ts,
                    source: self.source,
                };
                log::debug!(
                    "[OVERFLOW] {:?} + {} threw arithmetic error `{}`! Returning wrapped result {:?}",
                    self,
                    rhs,
                    err,
                    res
                );
                res
            }
        }
    }
}

impl Sub for MonotonicTimestamp {
    type Output = TimestampOffset;

    fn sub(self, rhs: Self) -> Self::Output {
        if self.source != rhs.source {
            panic!(
                "Cannot subtract/offset timestamps from different nodes! Tried to offset {:?} and {:?}",
                self.source, rhs.source
            );
        }

        let res = (self.ts as TimestampOffset).checked_sub(rhs.ts as TimestampOffset);
        match res {
            Some(res) => res,
            None => panic!(
                "Subtraction of timestamps over/underflowed! Tried to offset {:?} and {:?}",
                self, rhs
            ),
        }
    }
}

/// All absolute timestamps are nanoseconds since the epoch in UTC,
/// as returned by DateTime<Utc>.timestamp_nanos() on a system with
/// accurate clocks. This should be UTC CLOCK_REALTIME.
pub type AbsoluteTimestamp = i64;

/// A system's clock (not the current value, but the clock itself)
/// is described by a pair of timestamps, obtained from CLOCK_REALTIME
/// and CLOCK_MONOTONIC respectively (ideally atomically, but in short succession
/// is good enough for us).
#[derive(Debug, Clone, Copy)]
struct ClockDescriptor {
    absolute_origin_ns: AbsoluteTimestamp,
    monotonic_origin_ns: MonotonicTimestamp,
}

impl ClockDescriptor {
    pub fn new(
        absolute_origin_ns: AbsoluteTimestamp,
        monotonic_origin_ns: MonotonicTimestamp,
    ) -> ClockDescriptor {
        ClockDescriptor {
            absolute_origin_ns,
            monotonic_origin_ns,
        }
    }

    fn init() -> ClockDescriptor {
        let absolute_origin = time::clock_gettime(time::ClockId::CLOCK_REALTIME).unwrap();
        let monotonic_origin = time::clock_gettime(time::ClockId::CLOCK_MONOTONIC).unwrap();

        let absolute_origin_ns = absolute_origin
            .tv_sec()
            .checked_mul(1_000_000_000)
            .unwrap()
            .checked_add(absolute_origin.tv_nsec() as i64)
            .unwrap();

        let monotonic_origin_ns = (monotonic_origin.tv_sec() as u64)
            .checked_mul(1_000_000_000)
            .unwrap()
            .checked_add(monotonic_origin.tv_nsec() as u64)
            .unwrap();

        // TODO: should we have a separate node ID for the mediator?
        let monotonic_origin_ns = MonotonicTimestamp::from(monotonic_origin_ns, NodeId::default());
        ClockDescriptor {
            absolute_origin_ns,
            monotonic_origin_ns,
        }
    }
}

pub struct ClockManager {
    // The mediator's (i.e., this process') clock is OWN_CLOCK.
    /// Clock descriptors for nodes.
    clocks: HashMap<NodeId, ClockDescriptor>,

    /// We assume nodes' clocks are synchronized to within this many milliseconds.
    pub synchronized_to_within_ms: u64,
}

impl ClockManager {
    pub fn new(synchronized_to_within_ms: u64) -> ClockManager {
        let initialized_before = !OWN_CLOCK.set(ClockDescriptor::init());
        assert!(
            !initialized_before,
            "ClockManager should only be constructed once!"
        );

        ClockManager {
            clocks: HashMap::new(),
            synchronized_to_within_ms,
        }
    }

    pub fn get_relative_time_ns() -> UnsourcedMonotonicTimestamp {
        let monotonic_origin = time::clock_gettime(time::ClockId::CLOCK_MONOTONIC).unwrap();

        (monotonic_origin.tv_sec() as UnsourcedMonotonicTimestamp)
            .checked_mul(1_000_000_000)
            .unwrap()
            .checked_add(monotonic_origin.tv_nsec() as UnsourcedMonotonicTimestamp)
            .unwrap()
    }

    /// Get the absolute time in nanoseconds, without being affected by wall clock changes.
    fn get_absolute_time_ns() -> Option<AbsoluteTimestamp> {
        let now_monotonic_ns = Self::get_relative_time_ns();
        match OWN_CLOCK.try_get() {
            Some(own_clock) => {
                let monotonic_delta_ns = now_monotonic_ns - own_clock.monotonic_origin_ns.ts;
                Some(own_clock.absolute_origin_ns + monotonic_delta_ns as AbsoluteTimestamp)
            }
            None => None,
        }
    }

    /// Get a DateTime<Utc> that is not affected by clock changes if possible.
    pub fn utc_now() -> DateTime<Utc> {
        match Self::get_absolute_time_ns() {
            Some(now_ns) => Utc.timestamp_nanos(now_ns),
            None => Utc::now(),
        }
    }

    pub fn register_clock(
        &mut self,
        node_id: NodeId,
        abs_ts: AbsoluteTimestamp,
        rel_ts: MonotonicTimestamp,
    ) {
        log::info!(
            "[CLOCK] Registering clock for node {}: abs_ts = {}, rel_ts = {}",
            node_id,
            abs_ts,
            rel_ts
        );
        assert!(node_id == rel_ts.source);
        self.clocks
            .insert(node_id, ClockDescriptor::new(abs_ts, rel_ts));
    }

    pub fn register_nemesis_clock(&mut self, nemesis_node_id: NodeId) {
        let oc = OWN_CLOCK.get();
        let nc = ClockDescriptor::new(
            oc.absolute_origin_ns,
            MonotonicTimestamp::from(oc.monotonic_origin_ns.ts, nemesis_node_id),
        );

        log::info!(
            "[CLOCK] Registering clock for nemesis {}: {:?}",
            nemesis_node_id,
            nc
        );
        self.clocks.insert(nemesis_node_id, nc);
    }

    pub fn get_abs_if_registered(
        &self,
        node_id: NodeId,
        rel_ts: MonotonicTimestamp,
    ) -> Option<AbsoluteTimestamp> {
        self.clocks
            .get(&node_id)
            // underflow protection implemented in Sub for MonotonicTimestamp
            .map(|clock| {
                let diff = match AbsoluteTimestamp::try_from(rel_ts - clock.monotonic_origin_ns) {
                    Ok(diff) => diff,
                    Err(_) => {
                        panic!("Could not get absolute timestamp for {:?} because it led to under/overflow when computing {:?} - {:?}",
                         rel_ts, rel_ts, clock.monotonic_origin_ns);
                    }
                };
                clock.absolute_origin_ns + diff
            })
    }

    pub fn get_rel_with_offset_if_registered(
        &self,
        node_id: NodeId,
        offset_from_origin: TimestampOffset,
    ) -> Option<MonotonicTimestamp> {
        self.clocks.get(&node_id).map(|clock| {
            let base = clock.monotonic_origin_ns;
            base.add(offset_from_origin)
        })
    }

    pub fn get_rel_if_registered(
        &self,
        node_id: NodeId,
        abs_ts: AbsoluteTimestamp,
    ) -> Option<MonotonicTimestamp> {
        self.clocks.get(&node_id).map(|clock| {
            let base = clock.monotonic_origin_ns;
            let diff = (abs_ts as i128)
                .checked_sub(clock.absolute_origin_ns as i128)
                .expect("Substraction relative to origin over/underflowed!");
            base + diff
        })
    }
}
