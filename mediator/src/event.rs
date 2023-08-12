pub use crate::history::time::MonotonicTimestamp;
use crate::{
    feedback::reward::SummaryTask,
    net::packet::{NetworkId, PacketUid},
};
use std::fmt;

/// Identifies code locations for lock acquisitions and releases.
/// Event type changes into u16 for history summary with the AFL strategy.
pub type BlockId = u16;
pub type FunctionId = u16;

pub type BranchHitCount = u8;
pub type BranchId = u16;

pub type NodeId = u8;
pub type ProcessId = NodeId;

/* We have two event _kinds_:
 *  - "generic" events that are not attached to a Lamport diagram and simply describe that
 *     something happened without identifying _when_ or _where_.
 *  - events that are attached to a Lamport diagram (i.e., have a timestamp and process)
 *
 * The former are useful for summarising histories (which consist of "Lamport" events).
 */

#[derive(Clone, PartialEq, Eq, Debug, PartialOrd, Ord, Hash)]
pub enum ResponseStatus {
    Succeeded, // :ok
    Failed,    // :fail
    Unknown,   // :info
}

/// Special events for manipulating summaries.
#[derive(Clone, PartialEq, Eq, Debug, PartialOrd, Ord, Hash)]
pub enum AdministrativeEvent {
    // IMPORTANT: the order matters. We want EndWindow(n) to sort
    // before StartWindow(n+1) and after CollateSummaries in the timelines.
    /// We link EndWindow from all processes to a single CollateSummaries event.
    CollateSummaries {
        task: SummaryTask,
    },
    EndWindow {
        schedule_id: usize,
        step_id: usize,
    },
    StartWindow {
        schedule_id: usize,
        step_id: usize,
    },
    EndSchedule {
        schedule_id: usize,
        schedule_start: MonotonicTimestamp,
    },
}

impl AdministrativeEvent {
    pub fn should_store_summary(&self) -> Option<SummaryTask> {
        if let AdministrativeEvent::CollateSummaries { task } = self {
            Some(task.clone())
        } else {
            None
        }
    }

    pub fn should_clean_up(&self) -> bool {
        matches!(self, AdministrativeEvent::EndSchedule { .. })
    }
}

#[derive(Clone, PartialEq, Eq, Debug, PartialOrd, Ord, Hash)]
pub enum Event {
    BlockExecute {
        block_id: BlockId,
    },
    FunctionExecute {
        function_id: FunctionId,
    },
    AFLHitCount {
        branch_events: Vec<BranchHitCount>,
    },
    // TODO: we might want to automatically identify broadcasts
    // and distinguish between Broadcast and Unicast.
    PacketSend {
        data: u32,
        to: NodeId,
        network_id: NetworkId,
        mediator_id: PacketUid, // unique linearly increasing ID for the packet
        dropped: bool,          // was this dropped by the mediator?
    },
    PacketReceive {
        data: u32,
        from: NodeId,
        network_id: NetworkId,
        mediator_id: PacketUid, // unique linearly increasing ID for the packet
    },
    ClientRequest {
        /// corresponds to the :f keyword in the edn
        kind: String,
        /// This is a Jepsen-level "process" (thread), not a node within the system under test.
        process: String,
        // TODO: do we want value?
    },
    ClientResponse {
        /// corresponds to the :f keyword in the edn
        kind: String,
        /// corresponds to the :type keyword in the edn
        status: ResponseStatus,
        /// This is a Jepsen-level "process" (thread), not a node within the system under test.
        process: String,
    },
    // TODO: we should probably disambiguate between faults that happen "on the network"
    // and faults that happen at a specific node.
    Fault {
        /// corresponds to the :f keyword in the edn
        kind: String,
        /// corresponds to the :value keyword in the edn
        value: String,
    },
    TimelineEvent(AdministrativeEvent),
    Unknown,
}

impl Event {
    pub const fn default() -> Self {
        Event::Unknown
    }

    pub fn is_administrative_event(&self) -> bool {
        matches!(self, Event::TimelineEvent(_))
    }

    pub fn is_end_window(&self) -> bool {
        matches!(
            self,
            Event::TimelineEvent(AdministrativeEvent::EndWindow { .. })
        )
    }

    pub fn is_collate_summaries(&self) -> bool {
        matches!(
            self,
            Event::TimelineEvent(AdministrativeEvent::CollateSummaries { .. })
        )
    }

    pub fn is_packet_send(&self) -> bool {
        matches!(self, Event::PacketSend { .. })
    }

    pub fn is_packet_receive(&self) -> bool {
        matches!(self, Event::PacketReceive { .. })
    }

    pub fn is_dropped_packet_send(&self) -> bool {
        match self {
            Event::PacketSend { dropped, .. } => *dropped,
            _ => false,
        }
    }
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Event::BlockExecute { block_id } => write!(f, "BB({})", block_id),
            Event::FunctionExecute { function_id } => write!(f, "Func({})", function_id),
            Event::AFLHitCount { branch_events: _ } => {
                write!(f, "AFLBranchEvent(<not printed>)")
            }
            ev => write!(f, "{:?}", ev),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug, PartialOrd, Ord, Hash)]
/// NOTE: the order of fields is important, as it is used to derive the comparison function.
pub struct LamportEvent {
    pub clock: MonotonicTimestamp,
    pub ev: Event,
}

/// Helper functions to derive ranges of Lamport Events.
impl LamportEvent {
    pub fn proc(&self) -> ProcessId {
        self.clock.source
    }

    pub fn ts(&self) -> MonotonicTimestamp {
        self.clock
    }

    pub fn first_ev_at(clock: MonotonicTimestamp) -> LamportEvent {
        LamportEvent {
            clock,
            ev: Event::BlockExecute { block_id: 0 },
        }
    }

    pub fn last_ev_at(clock: MonotonicTimestamp) -> LamportEvent {
        LamportEvent {
            clock,
            ev: Event::Unknown,
        }
    }

    /// Get the underlying event for this LamportEvent.
    pub fn bare_event(&self) -> &Event {
        &self.ev
    }

    pub fn is_packet_send(&self) -> bool {
        self.ev.is_packet_send()
    }

    pub fn is_packet_receive(&self) -> bool {
        self.ev.is_packet_receive()
    }

    pub fn is_dropped_packet_send(&self) -> bool {
        self.ev.is_dropped_packet_send()
    }

    pub fn is_administrative_event(&self) -> bool {
        self.ev.is_administrative_event()
    }

    pub fn is_collate_summaries(&self) -> bool {
        self.ev.is_collate_summaries()
    }

    pub fn is_end_window(&self) -> bool {
        self.ev.is_end_window()
    }
}

impl fmt::Display for LamportEvent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} @ {}:{}", self.ev, self.proc(), self.clock)
    }
}

// Causal processing

pub type EventMatchKey = String;
pub struct EventMatchInstruction(pub EventMatchKey, pub MatchingBehaviour, pub ArrowPosition);

// NOTE: this is significantly more general than needed in the implementation.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MatchingBehaviour {
    /// Create a causal link with the matching entry. For instance,
    /// Sends and Receives are matched intrinsically (in the sense that
    /// the receive always matches with its respective send), not with
    /// the "previous (according to real-time order)" send.
    Intrinsic,

    /// Create a causal link with the "previous (according to real-time order)"
    /// matching entry. E.g. a Read matches with the previous Write.
    /// NOTE: extrinsic matching only works within a single node,
    /// not across nodes (since we don't assume a global clock).
    Extrinsic,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ArrowPosition {
    Source, // e.g. message send
    Target, // e.g. message receive
    Any,    // can be both source and target, e.g. a CAS both reads and writes
}

impl fmt::Display for MatchingBehaviour {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MatchingBehaviour::Intrinsic => write!(f, "Intrinsic"),
            MatchingBehaviour::Extrinsic => write!(f, "Extrinsic"),
        }
    }
}

impl fmt::Display for ArrowPosition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ArrowPosition::Source => write!(f, "Source"),
            ArrowPosition::Target => write!(f, "Target"),
            ArrowPosition::Any => write!(f, "Any"),
        }
    }
}

impl fmt::Display for EventMatchInstruction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} ({} / {})", self.0, self.1, self.2)
    }
}

impl LamportEvent {
    /// Should we store the summary for this event permanently?
    pub fn should_store_summary(&self) -> Option<SummaryTask> {
        match self.bare_event() {
            Event::TimelineEvent(ev) => ev.should_store_summary(),
            _ => None,
        }
    }

    pub fn should_clean_up(&self) -> bool {
        match self.bare_event() {
            Event::TimelineEvent(ev) => ev.should_clean_up(),
            _ => false,
        }
    }

    /// Get the matching key for the given event.
    /// This is used to identify which other event this event forms a causal
    /// link with. Specifically, there is a causal link between A and B
    /// if A and B have the same matching key and A is a Source or Any
    /// and B is a Target or Any.
    pub fn match_instruction(&self) -> Option<EventMatchInstruction> {
        let node_id = self.proc();
        match &self.ev {
            Event::PacketSend { mediator_id, .. } => Some(EventMatchInstruction(
                format!("Packet-{}", mediator_id),
                MatchingBehaviour::Intrinsic,
                ArrowPosition::Source,
            )),

            Event::PacketReceive { mediator_id, .. } => Some(EventMatchInstruction(
                format!("Packet-{}", mediator_id),
                MatchingBehaviour::Intrinsic,
                ArrowPosition::Target,
            )),

            // We want to link all nodes' `EndWindow` events to the
            // `CollateSummaries` for the same task.
            Event::TimelineEvent(AdministrativeEvent::EndWindow {
                schedule_id,
                step_id,
            }) => Some(EventMatchInstruction(
                format!("EndWindow-{}-{}-{}", node_id, schedule_id, step_id),
                MatchingBehaviour::Intrinsic,
                ArrowPosition::Source,
            )),

            // TODO: should we create causal links between requests and responses?
            _ => None,
        }
    }

    /// Get the match instruction to match with source events from node `node_id`.
    pub fn match_instruction_for_node(&self, node_id: NodeId) -> Option<EventMatchInstruction> {
        match &self.ev {
            Event::TimelineEvent(AdministrativeEvent::CollateSummaries { task }) => {
                Some(EventMatchInstruction(
                    format!(
                        "EndWindow-{}-{}-{}",
                        node_id, task.schedule_id, task.step_id
                    ),
                    MatchingBehaviour::Intrinsic,
                    ArrowPosition::Target,
                ))
            }

            _ => None,
        }
    }
}

/// A potentially-dangling causal link ("arrow") between two events.
#[derive(Clone, PartialEq, Eq, Debug, Hash)]
pub struct CausalLink {
    /// The source event of the causal link.
    pub source: LamportEvent,
    /// The target event of the causal link.
    pub target: Option<LamportEvent>,
}

impl CausalLink {
    pub fn new_dangling(source: LamportEvent) -> CausalLink {
        CausalLink {
            source,
            target: None,
        }
    }

    pub fn new(source: LamportEvent, target: Option<LamportEvent>) -> CausalLink {
        CausalLink { source, target }
    }

    pub fn with_target(&self, target: &LamportEvent) -> CausalLink {
        CausalLink {
            source: self.source.clone(),
            target: Some(target.clone()),
        }
    }
}

impl fmt::Display for CausalLink {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.target {
            Some(target) => write!(f, "{} -> {}", self.source, target),
            None => write!(f, "{} -> ?", self.source),
        }
    }
}
