pub mod producers;
pub mod reward;
pub mod summary;

use std::collections::BTreeMap;
use std::io::{Cursor, Seek, SeekFrom};
use std::sync::atomic::{AtomicBool, Ordering};
use std::{sync::Arc, time::Instant};

use antidote::{Mutex, RwLock};
use byteorder::{BigEndian, ByteOrder, LittleEndian, ReadBytesExt};
use chrono::{DateTime, Duration, Utc};
use dashmap::{mapref::entry::Entry, DashMap};
use edn_format::{Keyword, Value};
use hashbrown::HashMap;

use self::reward::SummaryTask;
use crate::event::{AdministrativeEvent, ResponseStatus};
use crate::event::{Event, LamportEvent};
use crate::history::timeline::WindowDescriptor;
use crate::history::{
    common::{MonotonicTimestamp, NodeId, NodeIp, SeqNum},
    packets::Packets,
    time::{AbsoluteTimestamp, ClockManager, UnsourcedMonotonicTimestamp},
    timeline::DynamicTimeline,
};
use crate::nemesis::schedules::END_SCHEDULE_STEP_ID;

const JEPSEN_NODE_NAME: &str = "jepsen";
pub const JEPSEN_NODE_ID: NodeId = 0;
const JEPSEN_NEMESIS_THREAD: &str = "nemesis";

const BLOCK_EVENT_TYPE: u64 = 1;
const FUNC_EVENT_TYPE: u64 = 2;
const PACKET_SEND_EVENT_TYPE: u64 = 3;
const PACKET_RECV_EVENT_TYPE: u64 = 4;

const LOCAL_EVENT_SIZE: u16 = 24;

/// Manages receiving feedback from the nodes' coverage servers.
pub struct FeedbackManager {
    pub has_started: AtomicBool,
    pub timeline: Arc<DynamicTimeline>,

    nemesis_node_id: NodeId,
    // Fields related to the identities of nodes and processes.
    /// Node (named by its IP address) to integer ID mapping.
    /// Easier than working with references to strings.
    node_ids: DashMap<NodeIp, NodeId>,

    clocks: Arc<RwLock<ClockManager>>,
    packets: Arc<Packets>,

    // Fields related to event submissions from nodes.
    /// Each node gives us _its instrumented events_ in real-time order, but
    /// batches from the same node may arrive out-of-order. We therefore add
    /// a sequence number mechanism to make sure we maintain the watermark
    /// correctly.
    will_not_receive_events_before: DashMap<NodeId, MonotonicTimestamp>,
    last_processed_seq_num: DashMap<NodeId, SeqNum>,
    unprocessed_submissions: Mutex<BTreeMap<(NodeId, SeqNum), MonotonicTimestamp>>,
}

// TODO: we probably want to separate the coverage-server parts of this
// from the Jepsen parts, in case we want to integrate with other tools.
impl FeedbackManager {
    pub fn new(
        node_ips: &Vec<NodeIp>,
        clocks: Arc<RwLock<ClockManager>>,
        packets: Arc<Packets>,
        timeline: Arc<DynamicTimeline>,
    ) -> Self {
        let node_ids = DashMap::new();
        // We always have the Jepsen process
        node_ids.insert(JEPSEN_NODE_NAME.to_string(), JEPSEN_NODE_ID);
        // Assign a unique ID to each node.
        for node_ip in node_ips {
            let new_id = node_ids.len() as NodeId;
            node_ids.insert(node_ip.clone(), new_id);
        }
        let nemesis_node_id = node_ids.len() as NodeId;
        node_ids.insert(JEPSEN_NEMESIS_THREAD.to_string(), nemesis_node_id);

        clocks.write().register_nemesis_clock(nemesis_node_id);

        let will_not_receive_events_before = DashMap::new();
        for kv in node_ids.iter() {
            let node_id = *kv.value();
            will_not_receive_events_before.insert(node_id, MonotonicTimestamp::from(0, node_id));
        }

        let last_processed_seq_num = DashMap::new();
        let unprocessed_submissions = Mutex::new(BTreeMap::new());

        Self {
            has_started: AtomicBool::new(false),
            timeline,
            nemesis_node_id,
            node_ids,
            clocks,
            packets,
            will_not_receive_events_before,
            last_processed_seq_num,
            unprocessed_submissions,
        }
    }

    // Functions related to the identities of nodes and processes.
    /// Get the ID of the given node. If the node is not known, assign it a new ID.
    pub fn node_id(&self, node_name: &NodeIp) -> NodeId {
        // FIXME: this has a data race if there are concurrent node_id inserts.
        let next_id = self.node_ids.len() as NodeId;
        let entry = self.node_ids.entry(node_name.clone());
        match entry {
            Entry::Occupied(e) => *e.get(),
            Entry::Vacant(e) => {
                e.insert(next_id);
                next_id
            }
        }
    }

    pub fn start(&self) {
        self.has_started.store(true, Ordering::SeqCst);
    }

    pub fn has_started(&self) -> bool {
        self.has_started.load(Ordering::Relaxed)
    }

    /// Mark as having received all events before the given timestamp.
    /// seq_num N is processed only after ALL seq_num < N have been processed.
    /// If a sequence number that has been submitted before is submitted again,
    /// the newly-submitted timestamp IS considered. (That is, we do not prevent
    /// duplicate messages). This is because sequence numbers might be reused when
    /// processes are restarted.
    fn submitted_all_before(&self, node_id: NodeId, seq_num: SeqNum, new_ts: MonotonicTimestamp) {
        if node_id != JEPSEN_NODE_ID {
            log::debug!(
                "Node {} submitted watermark {} (seq: {})",
                node_id,
                new_ts,
                seq_num
            );
        }

        // Add submission
        self.unprocessed_submissions
            .lock()
            .insert((node_id, seq_num), new_ts);

        self.process_watermarks();
    }

    // FIXME: this isn't actually used properly; We just call it after every operation.
    // We need to engineer Jepsen to send us watermarks correctly.
    fn jepsen_submitted_all_before(&self, ts: MonotonicTimestamp) {
        self.submitted_all_before(JEPSEN_NODE_ID, 0, ts);
    }

    pub fn nemesis_submitted_all_before(&self, abs_ts: AbsoluteTimestamp) {
        let rel_ts = self
            .clocks
            .read()
            .get_rel_if_registered(self.nemesis_node_id, abs_ts)
            .expect("nemesis requested reward before clock registration");
        self.submitted_all_before(self.nemesis_node_id, 0, rel_ts);
    }

    pub fn register_jepsen_clock(
        &self,
        abs_ts: AbsoluteTimestamp,
        rel_ts: UnsourcedMonotonicTimestamp,
    ) {
        let mut clocks = self.clocks.write();
        let rel_ts = MonotonicTimestamp::from(rel_ts, JEPSEN_NODE_ID);
        clocks.register_clock(JEPSEN_NODE_ID, abs_ts, rel_ts);
    }

    // Functions related to event submissions from nodes.
    /// Called by the feedback server on fuzz-start to let us know what its sequence number is.
    pub fn submit_initial_sequence_number(
        &self,
        node_ip: NodeIp,
        seq_num: SeqNum,
        abs_ts: AbsoluteTimestamp,
        rel_ts: UnsourcedMonotonicTimestamp,
    ) {
        let node_id = self.node_id(&node_ip);
        log::info!(
            "[WATERMARK] Node {} submitted initial sequence number {}",
            node_id,
            seq_num
        );

        // Create entry if not exists.
        self.last_processed_seq_num
            .entry(node_id)
            .or_insert(seq_num);

        // Update entry if needed.
        // TODO: should we reset rather than take max here?
        self.last_processed_seq_num
            .alter(&node_id, |_, _lp| std::cmp::max(seq_num, _lp));

        let mut clocks = self.clocks.write();
        let rel_ts = MonotonicTimestamp::from(rel_ts, node_id);
        clocks.register_clock(node_id, abs_ts, rel_ts);
    }

    fn process_watermarks(&self) {
        let mut to_remove: Vec<(NodeId, SeqNum)> = Vec::new();
        let mut unprocessed_submissions = self.unprocessed_submissions.lock();

        let mut have: Vec<(NodeId, MonotonicTimestamp)> = vec![];

        // log::debug!("[WATERMARKS] Processing watermarks. Submissions:");
        // Process all submissions in order of (NodeId, SeqNum)
        for ((node_id, seq_num), sub_ts) in unprocessed_submissions.iter() {
            // log::debug!("  ({}, {}) -> {}", node_id, seq_num, sub_ts);
            // If we don't have a last_processed_seq_num (i.e. we haven't processed any submissions)
            // we ensure we can always process the first submission we receive (this one).
            // NOTE: if the first submission received is out-of-order, this messes up things.
            let lp = *self
                .last_processed_seq_num
                .entry(*node_id)
                .or_insert(*seq_num)
                .value();

            // If this submission can be applied, apply it.
            if *seq_num <= lp || *seq_num == lp + 1 {
                // NOTE: alter doesn't do anything if the key doesn't exist!
                // this is handled by the line above, which inserts the key if it doesn't exist.
                assert!(self.last_processed_seq_num.contains_key(node_id));
                self.last_processed_seq_num
                    .alter(node_id, |_, _lp| std::cmp::max(*seq_num, _lp));

                // NOTE: alter doesn't do anything if the key doesn't exist!
                // we are assuming this is populated already.
                assert!(self.will_not_receive_events_before.contains_key(node_id));
                self.will_not_receive_events_before
                    .alter(node_id, |_, old_ts| std::cmp::max(*sub_ts, old_ts));

                have.push((
                    *node_id,
                    *self.will_not_receive_events_before.get(node_id).unwrap(),
                ));

                to_remove.push((*node_id, *seq_num));

                if *node_id != JEPSEN_NODE_ID {
                    log::debug!(
                        "Node {} applied watermark {} (seq: {})",
                        node_id,
                        sub_ts,
                        seq_num
                    );
                }
            }
        }

        // Let the timeline know about the new watermarks.
        self.timeline.have_events_before(have);

        // Cleanup
        for r in to_remove.iter() {
            unprocessed_submissions.remove(r);
        }
    }

    /// Given an event returned by the code instrumentation, add it to the timeline.
    pub fn add_events_from_execution_instrumentation(
        &self,
        batch_id: usize,
        ts: UnsourcedMonotonicTimestamp,
        db_raw_data: &str,
        pkt_raw_data: &str,
        afl_raw_data: &str,
        node_ip: String,
    ) {
        // We assume the endianness of the remote machine is the same as the endianness of this machine.
        // TODO: This is a reasonable assumption for now, but we should fix this in the future.
        if cfg!(target_endian = "big") {
            self.parse_and_add_instrumented_events::<BigEndian>(
                batch_id,
                ts,
                db_raw_data,
                pkt_raw_data,
                afl_raw_data,
                node_ip,
            );
        } else {
            self.parse_and_add_instrumented_events::<LittleEndian>(
                batch_id,
                ts,
                db_raw_data,
                pkt_raw_data,
                afl_raw_data,
                node_ip,
            );
        }
    }

    // This is generic in the ByteOrder, which can be either LittleEndian or BigEndian.
    fn parse_and_add_instrumented_events<BOrd: ByteOrder>(
        &self,
        batch_id: usize,
        batch_ts: UnsourcedMonotonicTimestamp,
        db_raw_data: &str,
        pkt_raw_data: &str,
        afl_raw_data: &str,
        node_ip: String,
    ) {
        // // Don't add anything to the timeline before the start time.
        // if !self.has_started() {
        //     return;
        // }

        let start = Instant::now();
        let node_id = self.node_id(&node_ip);
        // Watermark timestamp for this node.
        let mut min_ts: MonotonicTimestamp = MonotonicTimestamp::from(0, node_id);

        let batch_ts = MonotonicTimestamp::from(batch_ts, node_id);
        let starting_offset: u64 = LOCAL_EVENT_SIZE as u64;

        // Parse the block/function events.
        let db_cached_data = Self::decode_decomp(db_raw_data);
        let mut db_rdr = Cursor::new(db_cached_data);
        let db_evt_counter = db_rdr.read_u16::<BOrd>().unwrap();
        db_rdr.seek(SeekFrom::Start(starting_offset)).unwrap();

        // Parse packet sent/received events
        let pkt_cached_data = Self::decode_decomp(pkt_raw_data);
        let pkt_evt_counter = pkt_cached_data.len() / (LOCAL_EVENT_SIZE as usize);
        let mut pkt_rdr = Cursor::new(pkt_cached_data);
        // pkt_rdr.seek(SeekFrom::Start(starting_offset)).unwrap();

        log::debug!(
            "[FEEDBACK] Received batch {} consisting of {} execution events and {} packet events from {}",
            batch_id,
            db_evt_counter,
            pkt_evt_counter,
            node_ip
        );

        // Add DB events.
        for db_entry_index in 1..=db_evt_counter {
            let etype = db_rdr.read_u64::<BOrd>().unwrap();
            let ts = MonotonicTimestamp::from(db_rdr.read_u64::<BOrd>().unwrap(), node_id);

            // Maintain watermark timestamp
            if ts < min_ts || min_ts.is_zero() {
                min_ts = ts;
            }

            let ev = match etype {
                // BlockExecute
                BLOCK_EVENT_TYPE => {
                    let eid = db_rdr.read_u64::<BOrd>().unwrap();
                    log::debug!(
                        "[Node {} Batch {} Entry {} / {}] BlockExecute {} @ {}",
                        node_id,
                        batch_id,
                        db_entry_index,
                        db_evt_counter,
                        eid,
                        ts
                    );
                    Event::BlockExecute {
                        block_id: eid as u16,
                    }
                }

                // FunctionExecute
                FUNC_EVENT_TYPE => {
                    let function_id = db_rdr.read_u64::<BOrd>().unwrap();
                    log::debug!(
                        "[Node {} Batch {} Entry {} / {}] FunctionExecute {} @ {}",
                        node_id,
                        batch_id,
                        db_entry_index,
                        db_evt_counter,
                        function_id,
                        ts
                    );
                    Event::FunctionExecute {
                        function_id: function_id as u16,
                    }
                }

                _ => {
                    // The event index generated by atomic::fetch_add(relaxed) is not continuous,
                    // so we need to skip the empty slots.
                    let id = db_rdr.read_u64::<BOrd>().unwrap();
                    log::debug!("ME: type: {}, ts: {}, rid: {}", etype, ts, id);
                    log::error!(
                        "(Malformed instrumented event) unknown event: type {}, eid {}, ts {}",
                        etype,
                        id,
                        ts
                    );
                    // Fail gracefully: process the next event, skipping this one.
                    continue;
                }
            };

            let lev = LamportEvent { ev, clock: ts };

            self.add_event_to_timeline(lev, Some(batch_id));
        }

        // Parse packet events.
        for pkt_entry_index in 1..=pkt_evt_counter {
            let etype = pkt_rdr.read_u64::<BOrd>().unwrap();
            let ts = MonotonicTimestamp::from(pkt_rdr.read_u64::<BOrd>().unwrap(), node_id);

            // Maintain watermark timestamp
            if ts < min_ts || min_ts.is_zero() {
                min_ts = ts;
            }

            let ev = match etype {
                // Packet Sent (3) / Received (4)
                pkt_type
                    if pkt_type == PACKET_SEND_EVENT_TYPE || pkt_type == PACKET_RECV_EVENT_TYPE =>
                {
                    let network_id = pkt_rdr.read_u64::<BOrd>().unwrap();
                    let evt_type = if pkt_type == PACKET_SEND_EVENT_TYPE {
                        "PacketSend"
                    } else {
                        "PacketReceive"
                    };

                    log::debug!(
                        "[Node {} Batch {} Entry {} / {}] {} {}",
                        node_id,
                        batch_id,
                        pkt_entry_index,
                        pkt_evt_counter,
                        evt_type,
                        network_id
                    );
                    let mediator_id = match self.packets.get_mediator_id(network_id) {
                        Some(id) => id,
                        None => {
                            log::warn!("[PACKET] Got {} with network_id {} from {}, but mediator has not seen this packet! Ignoring it.", evt_type, network_id, node_ip);

                            // There are two reasons this might happen (one which is expected, one which is a bug)
                            // (a) the packet was dropped by the network and it never reached us, OR
                            // (b) FIXME: [batch_before_packet]
                            // it can be the case that we receive a batch with a PacketSend before
                            // we receive the actual packet. In this case, we will not be able to recognize
                            // (identify) it. Ideally we would have something like the watermark mechanism
                            // to keep track of _when_ we have seen every packet a batch refers to, and only
                            // apply that batch then.
                            //
                            // This should happen relatively rarely. For now, we just ignore the event.
                            continue;
                        }
                    };
                    let data: u32 = self.packets.get_abstract_data(network_id).unwrap();
                    let dropped = self.packets.was_certainly_dropped(network_id);
                    let (sender_ip, receiver_ip) =
                        self.packets.get_sender_receiver(network_id).unwrap();
                    let sender_id = self.node_id(&sender_ip);
                    let receiver_id = self.node_id(&receiver_ip);

                    match pkt_type {
                        PACKET_SEND_EVENT_TYPE => {
                            assert_eq!(
                                sender_id, node_id,
                                "PacketSend event {} from node {} but packet was sent from node {}",
                                network_id, node_id, sender_id
                            );
                            Event::PacketSend {
                                data,
                                to: receiver_id,
                                network_id,
                                mediator_id,
                                dropped,
                            }
                        }
                        PACKET_RECV_EVENT_TYPE => {
                            assert_eq!(
                                receiver_id, node_id,
                                "PacketReceive event {} from node {} but packet was sent to node {}",
                                network_id, node_id, receiver_id
                            );

                            Event::PacketReceive {
                                data,
                                from: sender_id,
                                network_id,
                                mediator_id,
                            }
                        }
                        _ => unreachable!(),
                    }
                }

                _ => panic!("unknown event: type {}, ts {}", etype, ts),
            };

            let lev = LamportEvent { ev, clock: ts };

            self.add_event_to_timeline(lev, Some(batch_id));
        }

        // Parse AFL events
        let afl_cached_data = Self::decode_decomp(afl_raw_data);

        log::debug!(
            "[Node: {} Batch {}] AFLBranchEvent @ {}",
            node_id,
            batch_id,
            batch_ts.ts
        );

        let afl_branch_event = LamportEvent {
            clock: batch_ts,
            ev: Event::AFLHitCount {
                branch_events: afl_cached_data,
            },
        };
        self.add_event_to_timeline(afl_branch_event, Some(batch_id));

        let end = Instant::now();
        log::debug!(
            "[PERF] Took {} ms to parse and add {} events from {} (batch {})",
            end.duration_since(start).as_millis(),
            db_evt_counter as usize + pkt_evt_counter,
            node_ip,
            batch_id
        );

        // Submit watermark timestamp for this node.
        // If there are no events in this batch, then the watermark timestamp is the batch timestamp.
        min_ts = if min_ts.is_zero() { batch_ts } else { min_ts };

        // NOTE: we _must_ submit even if no events were added, to ensure the sequence numbers
        // are updated correctly.
        self.submitted_all_before(node_id, batch_id, min_ts);
    }

    /// Decompress data received from coverage server.
    fn decode_decomp(raw_data: &str) -> Vec<u8> {
        let decoded_data = base64::decode(raw_data).expect("Mediator: decoding error");
        lz4_flex::decompress_size_prepended(&decoded_data).expect("Mediator: decompression error")
    }

    /// Given a Jepsen operation and the timestamp of the origin point of this history,
    /// add this event to the history (and return it as well).
    pub fn add_event_from_jepsen_op(&self, _op: &str) {
        // We log these to the mediator log as well, to ease debugging.
        log::debug!("[OP] Jepsen OP: {}", _op);
        assert!(
            self.has_started(),
            "Received Jepsen OP before test started!"
        );

        let op = edn_format::parse_str(_op);
        match op {
            Ok(Value::Map(map)) => {
                let (ev_process, is_nemesis) =
                    match map.get(&Value::from(Keyword::from_name("process"))) {
                        Some(Value::Integer(proc)) => (proc.to_string(), false),
                        // Any non-integer process is a nemesis process.
                        // We collate them all into one.
                        Some(Value::String(_))
                        | Some(Value::Keyword(_))
                        | Some(Value::Symbol(_)) => (JEPSEN_NEMESIS_THREAD.to_string(), true),
                        Some(_) => {
                            log::error!(
                                "(Malformed operation) :process is not a valid value in op: {}",
                                _op
                            );
                            return;
                        }
                        None => {
                            log::error!("(Malformed operation) no :process in op: {}", _op);
                            return;
                        }
                    };

                // Get the relative timestamp of this event by adding the relative offset to the origin.
                let rel_ts = match map.get(&Value::from(Keyword::from_name("time"))) {
                    Some(Value::Integer(offset)) => *offset,
                    _ => {
                        log::error!("(Malformed operation) no :time in op: {}", _op);
                        return;
                    }
                } as UnsourcedMonotonicTimestamp;
                // Jepsen events have timestamps relative to the start of the test.
                // The start of the test is the `rel_ts` that Jepsen gives us when registering
                // its clock. We add the `rel_ts` from the event to that origin to get the actual
                // timestamp of the event.
                let clocks = self.clocks.read();
                let rel_ts = clocks
                    .get_rel_with_offset_if_registered(JEPSEN_NODE_ID, rel_ts as i128)
                    .expect("Cannot assign timestamp to Jepsen event before clock registration!");

                // Two big types of operations: a fault (:process = nemesis) or a normal/client event.
                // Both will have :f, :value, and :type (one of :invoke, :ok, :fail or :info).
                // NOTE: instructions like "sleep" or "log" have no :f, but they are not really
                // either nemesis or client operations, so it's probably OK to skip them.
                let ev_kind = match map.get(&Value::from(Keyword::from_name("f"))) {
                    Some(Value::Keyword(f_data)) => f_data.name().to_string(),
                    _ => {
                        log::error!("(Malformed operation) no :f in op: {}", _op);
                        return;
                    }
                };

                let ev_type = match map.get(&Value::from(Keyword::from_name("type"))) {
                    Some(Value::Keyword(ev_type)) => ev_type.name(),
                    _ => {
                        log::error!("(Malformed operation) no :type in op: {}", _op);
                        return;
                    }
                };

                let ev_value = match map.get(&Value::from(Keyword::from_name("value"))) {
                    Some(x) => x.to_string(),
                    _ => {
                        // Some nemesis operations have no :value, e.g. Dqlite's membership :grow and :shrink.
                        if is_nemesis {
                            "".to_string()
                        } else {
                            log::error!("(Malformed operation) no :value in op: {}", _op);
                            return;
                        }
                    }
                };

                let ev = match ev_type {
                    "invoke" => Event::ClientRequest {
                        kind: ev_kind,
                        process: ev_process,
                    },
                    "ok" => Event::ClientResponse {
                        kind: ev_kind,
                        status: ResponseStatus::Succeeded,
                        process: ev_process,
                    },
                    "fail" => Event::ClientResponse {
                        kind: ev_kind,
                        status: ResponseStatus::Failed,
                        process: ev_process,
                    },
                    // :info is used for operations that _may_ have failed
                    // AND for nemesis operations.
                    "info" => match is_nemesis {
                        // Awkardly, nemesis operations use :info both when they invoke
                        // and when they complete.
                        true => Event::Fault {
                            kind: ev_kind,
                            value: ev_value,
                        },
                        false => Event::ClientResponse {
                            kind: ev_kind,
                            status: ResponseStatus::Unknown,
                            process: ev_process,
                        },
                    },

                    _ => {
                        log::error!("(Malformed operation) unknown :type in op: {}", _op);
                        return;
                    }
                };

                let lev = LamportEvent { ev, clock: rel_ts };
                self.add_event_to_timeline(lev, None);
                // FIXME: this is not correct; we need to add watermarks to Jepsen
                // operations as well.
                self.jepsen_submitted_all_before(rel_ts);
            }

            _ => {
                log::error!("Unexpected Jepsen operation: {} (skipping)", _op);
            }
        }
    }

    fn add_event_to_timeline(&self, ev: LamportEvent, batch_id: Option<SeqNum>) {
        // Check that we are satisfying the guarantee not to add events in the past.
        let source = ev.proc();
        let past_ts = self.will_not_receive_events_before.get(&ev.proc());
        let guaranteed_past_ts = match past_ts {
            None => MonotonicTimestamp::zero(source),
            Some(r) => *r.value(),
        };

        // FIXME: We can still have "in the past" Jepsen operations. We might need a separate watermark.
        if ev.clock < guaranteed_past_ts
            && ev.proc() != JEPSEN_NODE_ID
            && !ev.is_administrative_event()
        {
            let diff_ns: i64 = (guaranteed_past_ts - ev.clock) as i64;
            // FIXME: this means there's something wrong with what the coverage
            // server is sending us, but it should be safe to ignore.
            log::error!(
                "[HISTORY] Adding event {} (from batch {}) that was guaranteed to be in the past (watermark: {} / diff: {} ms)",
                ev,
                batch_id.map_or_else(|| "None".to_string(), |x| x.to_string()),
                guaranteed_past_ts,
                Duration::nanoseconds(diff_ns).num_milliseconds());
        }

        self.timeline.add_event(ev);
    }

    pub fn start_new_history(
        &self,
        old_schedule_id: usize,
        old_start_time: DateTime<Utc>,
        old_end_time: DateTime<Utc>,
        new_start_time: DateTime<Utc>,
    ) {
        log::info!("[HISTORY] Starting new history at {}", new_start_time);
        let clocks = self.clocks.read();

        let mut cleanup_markers = HashMap::new();
        // We need to create EndWindow markers at the beginning of the new schedule,
        // so that events get processed until that point during the reset period
        // AND the summaries get cleaned-up before the new schedule starts.
        let mut end_window_markers = WindowDescriptor::new(old_schedule_id, END_SCHEDULE_STEP_ID);

        // Add an EndSchedule marker for every process.
        for ip_id in self.node_ids.iter() {
            let node_id = *ip_id;
            if let (Some(old_start_ts), Some(old_end_ts), Some(new_start_ts)) = (
                clocks.get_rel_if_registered(node_id, old_start_time.timestamp_nanos()),
                clocks.get_rel_if_registered(node_id, old_end_time.timestamp_nanos()),
                clocks.get_rel_if_registered(node_id, new_start_time.timestamp_nanos()),
            ) {
                // Add EndWindow markers.
                end_window_markers.add_marker(node_id, old_end_ts, new_start_ts);

                let end_marker = LamportEvent {
                    ev: Event::TimelineEvent(AdministrativeEvent::EndSchedule {
                        schedule_id: old_schedule_id,
                        schedule_start: old_start_ts,
                    }),
                    clock: old_end_ts,
                };

                log::debug!(
                    "[HISTORY] Node {} added EndSchedule marker event {:?}.",
                    node_id,
                    end_marker
                );
                self.add_event_to_timeline(end_marker, None);
                // Clean before start_ts (IMPORTANT: not end_ts!)
                cleanup_markers.insert(node_id, old_start_ts);
            }
        }
        // Declare windows for all processes.
        //  self.timeline.declare_window(end_window_markers);

        // Clean-up timeline.
        self.timeline.clean_up_all_committed_before(cleanup_markers);
    }

    /// Introduce administrative events into the timeline and collate a summary from all processes
    /// to report reward for back to the nemesis.
    pub fn request_reward_for_window(&self, task: SummaryTask) {
        log::info!("[FEEDBACK] Requesting reward for task {:?}", task);
        let clocks = self.clocks.read();

        let mut window_markers = WindowDescriptor::new(task.schedule_id, task.step_id);

        // Collate a summary from all processes at the nemesis node.
        if let Some(collate_ts) = clocks.get_rel_if_registered(self.nemesis_node_id, task.end_ts) {
            for ip_id in self.node_ids.iter() {
                let node_id = *ip_id;

                if let (Some(rel_start_ts), Some(rel_end_ts)) = (
                    // We add a +1 such that we don't intersect with the previous end.
                    clocks.get_rel_if_registered(node_id, task.start_ts + 1),
                    clocks.get_rel_if_registered(node_id, task.end_ts),
                ) {
                    // Create the WindowDescriptor for this node.
                    window_markers.add_marker(node_id, rel_start_ts, rel_end_ts);

                    // Introduce WindowStart and WindowEnd event for every node
                    // except JEPSEN_NODE_ID. We don't want Jepsen events to
                    // be included in the summary.
                    // NOTE: if you decide to add EndWindow events for the Jepsen
                    // node, you will need to modify `attach_link_targets` in timeline.rs as well!
                    if node_id == JEPSEN_NODE_ID {
                        continue;
                    }

                    let start_ev = LamportEvent {
                        clock: rel_start_ts,
                        ev: Event::TimelineEvent(AdministrativeEvent::StartWindow {
                            schedule_id: task.schedule_id,
                            step_id: task.step_id,
                        }),
                    };

                    let end_ev = LamportEvent {
                        clock: rel_end_ts,
                        ev: Event::TimelineEvent(AdministrativeEvent::EndWindow {
                            schedule_id: task.schedule_id,
                            step_id: task.step_id,
                        }),
                    };

                    log::debug!(
                        "[WINDOW] Node {} added markers for window {:?} to {:?}.",
                        node_id,
                        start_ev,
                        end_ev
                    );
                    self.add_event_to_timeline(start_ev, None);
                    self.add_event_to_timeline(end_ev, None);
                }
            }

            let end_ts = task.end_ts;

            // Introduce a collate event for the Nemesis node
            let collate_ev = LamportEvent {
                clock: collate_ts,
                ev: Event::TimelineEvent(AdministrativeEvent::CollateSummaries { task }),
            };
            log::info!("[WINDOW] Adding CollateSummaries event {:?}.", collate_ev);
            self.add_event_to_timeline(collate_ev, None);

            // Declare windows for all processes.
            self.timeline.declare_window(window_markers);
        }
    }

    pub fn print_watermarks(&self) {
        let mut node_status = vec!["\n".to_string()];
        let mut watermarks: Vec<(NodeId, MonotonicTimestamp)> = self
            .will_not_receive_events_before
            .iter()
            .map(|x| (*x.key(), *x.value()))
            .collect();
        watermarks.sort_unstable();

        let clocks = self.clocks.read();
        for (node_id, rel_ts) in watermarks {
            let seq_num: String = match self.last_processed_seq_num.get(&node_id) {
                Some(ns) => (*ns.value()).to_string(),
                None => String::from("???"),
            };
            let abs_str = match clocks.get_abs_if_registered(node_id, rel_ts) {
                Some(abs_ts) => format!("(abs: {})", abs_ts),
                None => String::from(""),
            };
            let str = format!(
                "{} -> rel_ts: {} {} / seq: {}\n",
                node_id, rel_ts, abs_str, seq_num
            );
            node_status.push(str);
        }

        log::info!("[WATERMARK] (entries = {})", node_status.concat(),);
    }
}
