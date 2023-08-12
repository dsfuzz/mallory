#[macro_use]
extern crate rocket;
extern crate state;

mod event;
mod feedback;
mod history;
mod nemesis;
mod net;

use crate::history::time::{AbsoluteTimestamp, ClockManager, UnsourcedMonotonicTimestamp};
use crate::history::History;
use crate::nemesis::AdaptiveNemesis;
use crate::net::{firewall::Firewall, packet::Packet};

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use antidote::Mutex;
use byte_unit::Byte;
use chrono::{DateTime, TimeZone, Utc};
use config::Config;
use crossbeam_queue::SegQueue;
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt, TryFutureExt};
use ipnetwork::IpNetwork;
use nemesis::interfaces::jepsen::JepsenAdaptiveNemesis;
use nix::sys::select::{select, FdSet};
use nix::sys::time::TimeValLike;
use rocket::form::Form;
use rocket::serde::{json::Json, Deserialize};
use state::Storage;
use std::collections::VecDeque;
use std::env;
use std::error::Error;
use std::fs::File;
use std::net::SocketAddr;
use std::ops::Mul;
use std::os::unix::io::{AsRawFd, RawFd};
use std::panic;
use std::path::Path;
use std::process;
use std::process::Command;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tikv_jemalloc_ctl::{epoch, stats};

const MS_PER_SEC: f32 = 1000.0;

/*****************************************
 * Global state and associated functions *
 *****************************************/

pub struct MediatorConfig {
    experiment_network: IpNetwork,
    node_name_format: String,
    unfiltered_ports: Vec<u16>,
    bucket_interval_ms: u64,

    clocks_synchronized_to_within_ms: u64,
    statistics_print_interval_secs: u64,

    nfqueue_queue_num: u16,
    nfqueue_max_len: u32,
    nfqueue_select_timeout_ns: i64,

    nemesis_schedule_duration_ms: u64,
    nemesis_schedule_interval_ms: u64,
    nemesis_reset_duration_ms: u64,

    mediator_log_filename: String,
    shiviz_log_filename: String,
    event_log_filename: String,
    iptables_rules_filename: String,

    num_nodes: u8,
    node_ips: Vec<String>,

    schedule_type: String,
    feedback_type: String,
    state_similarity_threshold: f64,
}

struct BucketProcessingStats {
    last_interval: Duration,
    last_time: Instant,
    last_exec_time: Duration,
    last_num_packets: usize,
}

struct PacketState {
    // used to give IDs to packets
    packet_counter: AtomicUsize,
    dropped_counter: AtomicUsize,
    // queue of packets for which no decision was made
    unprocessed_packets: SegQueue<Packet>,
    // queue of packets for which a decision was made, but need to be released
    // we use a lock because we _DO_ want all of the queue (bucket) to be released at once
    unreleased_packets: Mutex<VecDeque<Packet>>,

    // number of packets for which a decision was made
    num_processed_packets: AtomicUsize,

    // timing-related
    // TODO: we are using this for all statistics, not just packet-related,
    // so we should move it somewhere more appropriate.
    last_statistics_print_time: Mutex<Box<Instant>>,
    bucket_stats: Mutex<Box<BucketProcessingStats>>,
}

// We have no synchronization on CFG, as it is only written to at startup.
static CFG: Storage<MediatorConfig> = Storage::new();
static PACKETS: Storage<PacketState> = Storage::new();
static FIREWALL: Storage<Firewall> = Storage::new();
static HISTORY: Storage<Arc<History>> = Storage::new();
static NEMESIS: Storage<Arc<JepsenAdaptiveNemesis>> = Storage::new();
// We reuse a single Client rather than spawning one whenever needed.
static CLIENT: Storage<reqwest::Client> = Storage::new();

/************************************
 * Test related functions and logic *
 ************************************/

#[derive(FromForm)]
struct TestStart {
    start_time: String,
    store_path: String,
}

#[post("/test/start", data = "<data>")]
fn start_test(data: Form<TestStart>) {
    let run = HISTORY.get();

    let mut store_path = run.store_path.write();
    *store_path = data.store_path.clone();

    let ts = data.start_time.clone();
    let dt = DateTime::parse_from_rfc3339(&ts).unwrap();
    let dt_utc: DateTime<Utc> = DateTime::from_utc(dt.naive_utc(), Utc);
    let mut start_time = run.start_time.write();
    **start_time = dt_utc;

    let run_name = ts.trim().replace([':', '-'], "");
    let mut name = run.name.write();
    *name = run_name;

    run.start();
    log::info!("Started test at {}", data.store_path);
}

#[post("/test/end")]
fn end_test(shutdown: rocket::Shutdown) {
    let started = HISTORY.get().has_started();
    if !started {
        log::warn!("Received end test request, but test has not started!");
        return;
    }
    let run = HISTORY.get();
    let mut end_time = run.end_time.write();
    **end_time = ClockManager::utc_now();

    // let n = NEMESIS.get();
    // n.execution_ended();

    log::info!("Ended test stored at {}", *run.store_path.read());
    // let h = &HISTORY.get().history;
    // h.summarise();

    dump_run();
    shutdown.notify();
}

// Note: you need to collect::<Vec<_>>() to actually execute these!
fn batch_post_reqs(
    urls: impl Iterator<Item = String>,
) -> FuturesUnordered<impl Future<Output = Result<reqwest::Response, reqwest::Error>>> {
    let client = CLIENT.get();
    let futures = FuturesUnordered::new();
    for url in urls {
        futures.push(client.post(&url).send().and_then(|x| async move {
            log::debug!("Received a response from POST request to {}", url);
            Ok(x)
        }));
    }
    futures
}

#[derive(FromForm)]
struct TestStartTime {
    absolute: String,
    relative: String,
}

#[post("/test/start_time", data = "<data>")]
async fn start_time(data: Form<TestStartTime>) {
    let node_ips = &CFG.get().node_ips;
    let urls = node_ips
        .iter()
        .map(|ip| format!("http://{}:58080/state/fuzz-starting", ip));
    let reqs = batch_post_reqs(urls);
    let _resps = reqs.collect::<Vec<_>>().await;

    log::info!(
        "[TEST] Received start time: ABS {} / REL {}",
        data.absolute,
        data.relative
    );

    // Jepsen gives us two timestamps:
    //  - an absolute real-time timestamp for the start of the test
    //  - a high-precision "relative" timestamp for the start of the test, i.e., a logical clock
    // Operations received from Jepsen get high-precision relative offsets from the "relative" origin.
    // We convert these to absolute timestamps by adding the offset to the "absolute" origin.
    // As such, strictly speaking, we don't need to store the relative origin, but anyway...
    let relative_ts_ns = data
        .relative
        .parse::<UnsourcedMonotonicTimestamp>()
        .unwrap();

    // Jepsen uses Java's System.currentTimeMillis for absolute timestamps, which we
    // convert to nanosecond precision.
    let absolute_ts_ms = data.absolute.parse::<AbsoluteTimestamp>().unwrap();
    let absolute_ts_ns: AbsoluteTimestamp = Utc
        .timestamp_millis_opt(absolute_ts_ms)
        .unwrap()
        .timestamp_nanos();

    // Let the history know when time started for the Jepsen process.
    let history = &HISTORY.get();
    history
        .feedback
        .register_jepsen_clock(absolute_ts_ns, relative_ts_ns);
    history.start();
}

#[post("/test/before_tear_down")]
async fn before_tear_down() {
    let node_ips = &CFG.get().node_ips;
    let urls = node_ips
        .iter()
        .map(|ip| format!("http://{}:58080/state/fuzz-stopping", ip));
    let reqs = batch_post_reqs(urls);
    let _resps = reqs.collect::<Vec<_>>().await;
}

#[derive(Deserialize)]
struct SeqNum {
    seq_num: String,
    absolute_origin_ns: String,
    monotonic_origin_ns: String,
}

#[post("/feedback/submit-sequence-number", data = "<data>")]
fn submit_sequence_number(data: Json<SeqNum>, remote_addr: SocketAddr) {
    // This is submitted by the feedback server on fuzz-start to let us know
    // its sequence number, so we can match requests correctly.
    // FIXME: There may be an issue if this message is delayed.
    // We need to ensure we always wait for this.
    let seq_num: usize = data.seq_num.parse().unwrap();
    let abs_ts = data
        .absolute_origin_ns
        .parse::<AbsoluteTimestamp>()
        .unwrap();
    let rel_ts = data
        .monotonic_origin_ns
        .parse::<UnsourcedMonotonicTimestamp>()
        .unwrap();
    let node_ip = remote_addr.ip().to_string();
    let h = &HISTORY.get();
    h.feedback
        .submit_initial_sequence_number(node_ip, seq_num, abs_ts, rel_ts);
}

#[derive(Deserialize)]
struct LocalEvent {
    batch_id: String,
    rel_ts: String,
    db_cached_data: String,
    pkt_cached_data: String,
    afl_cached_data: String,
}

#[post("/feedback/collect-local-events", data = "<data>")]
fn collect_local_events(data: Json<LocalEvent>, remote_addr: SocketAddr) {
    let batch_id: usize = data.batch_id.parse().unwrap();
    let ts: UnsourcedMonotonicTimestamp = data.rel_ts.parse().unwrap();
    let db_raw_data = &data.db_cached_data;
    let pkt_raw_data = &data.pkt_cached_data;
    let afl_raw_data = &data.afl_cached_data;
    let h = &HISTORY.get();
    let remote_ip = remote_addr.ip().to_string();
    h.feedback.add_events_from_execution_instrumentation(
        batch_id,
        ts,
        db_raw_data,
        pkt_raw_data,
        afl_raw_data,
        remote_ip,
    );
}

#[derive(FromForm)]
struct ClientEvent {
    op: String,
}

#[post("/client/invoke", data = "<data>")]
fn invoke_operation(data: Form<ClientEvent>) {
    // If this is a nemesis operation, we let the AdaptiveNemesis know
    // that is has completed. (/client/invoke gets nemesis ops.)
    if JepsenAdaptiveNemesis::is_nemesis_operation(&data.op) {
        if let Some(n) = NEMESIS.try_get() {
            n.invoke(&data.op);
        }
    }

    let h = &HISTORY.get();
    h.feedback.add_event_from_jepsen_op(&data.op);
}

#[post("/client/complete", data = "<data>")]
fn complete_operation(data: Form<ClientEvent>) {
    // If this is a nemesis operation, we let the AdaptiveNemesis know
    // that is has completed. (/client/complete gets nemesis ops.)
    if JepsenAdaptiveNemesis::is_nemesis_operation(&data.op) {
        if let Some(n) = NEMESIS.try_get() {
            n.complete(&data.op);
        }
    }

    let h = &HISTORY.get();
    h.feedback.add_event_from_jepsen_op(&data.op);
}

fn dump_run() {
    let path_base = HISTORY.get().dump();
    log::info!("[SAVE] Dumping run to {}", path_base);

    // Flush log. Important when we panic.
    log::logger().flush();
    // Copy the mediator log into the Jepsen run directory.
    let tmp_mediator_log_path = CFG.get().mediator_log_filename.clone();
    let mediator_log_path = Path::new(&path_base).join("mediator.log");
    std::fs::copy(tmp_mediator_log_path, mediator_log_path)
        .expect("Could not store mediator.log in Jepsen run directory.");

    // Copy the events log into the Jepsen run directory.
    let tmp_events_log_path = CFG.get().event_log_filename.clone();
    if Path::new(&tmp_events_log_path).exists() {
        let schedule_tyep = &CFG.get().schedule_type;
        let feedback_type = &CFG.get().feedback_type;
        let event_log_file = format!("events-{}-{}.log", schedule_tyep, feedback_type);
        let events_log_path = Path::new(&path_base).join(event_log_file);
        std::fs::copy(tmp_events_log_path, events_log_path)
            .expect("Could not store events.log in Jepsen run directory.");
    }

    // Copy the ShiViz trace into the Jepsen run directory.
    let tmp_shiviz_log_filename = CFG.get().shiviz_log_filename.clone();
    if Path::new(&tmp_shiviz_log_filename).exists(){
        let shiviz_log_path = Path::new(&path_base).join("shiviz.log");
        std::fs::copy(tmp_shiviz_log_filename, shiviz_log_path)
            .expect("Could not store shiviz.log in Jepsen run directory.");
    }
}

/************
 * Firewall *
 ************/

/* Endpoints for Jepsen to interact with firewall */
#[derive(FromForm)]
struct DropInstruction<'r> {
    src: &'r str,
    dst: &'r str,
}

#[post("/firewall/drop", data = "<instr>")]
fn firewall_drop(instr: Form<DropInstruction>) -> Json<&Firewall> {
    let DropInstruction { src, dst } = instr.into_inner();
    {
        let src = src.to_string();
        let dst = dst.to_string();
        log::debug!("[FIREWALL] Dropping packets from {} to {}", src, dst);
        FIREWALL.get().drop(src, dst);
    }
    Json(FIREWALL.get())
}

#[post("/firewall/heal")]
fn firewall_heal() -> Json<&'static Firewall> {
    log::debug!("[FIREWALL] Healing the network");
    FIREWALL.get().heal();
    Json(FIREWALL.get())
}

#[derive(FromForm)]
struct EnactInstruction {
    pairs: Json<Vec<(String, String)>>,
}

#[post("/firewall/enact", data = "<instr>")]
fn firewall_enact(instr: Form<EnactInstruction>) -> Json<&'static Firewall> {
    let EnactInstruction { pairs } = instr.into_inner();
    let pairs = pairs.into_inner();
    log::debug!(
        "[FIREWALL] Enacting a new firewall configuration: {:?}",
        pairs
    );
    let fw = FIREWALL.get();
    fw.heal();
    for (src, dst) in pairs {
        fw.drop(src, dst);
    }
    Json(FIREWALL.get())
}

/*********************
 * Nemesis functions *
 *********************/

#[derive(FromForm)]
struct NemesisSetup {
    /// edn-formatted map of nemesis configurations
    nemesis: String,
}

#[post("/nemesis/setup", data = "<instr>")]
fn nemesis_setup(instr: Form<NemesisSetup>) {
    let cfg = CFG.get();
    let history = HISTORY.get();
    let new_nem = JepsenAdaptiveNemesis::new(&instr.nemesis, history.clone(), cfg);

    match NEMESIS.try_get() {
        // Initialize NEMESIS if needed.
        None => {
            log::info!(
                "[NEMESIS] Initializing nemesis with configuration: {}",
                &instr.nemesis
            );
            let nemesis = Arc::new(new_nem);
            let h = HISTORY.get();

            // Register the nemesis with the history (for feedback) and set it
            h.register_nemesis(nemesis.clone());
            NEMESIS.set(nemesis);
        }

        // If already initialized, change it in place.
        Some(nem) => {
            nem.replace_configuration(&instr.nemesis);
            // TODO: replace nemesis in History?
        }
    }
}

#[derive(FromForm)]
struct NemesisGetOp {
    ctx: String,
    reltime: String,
}

#[post("/nemesis/op", data = "<instr>")]
fn nemesis_get_op(instr: Form<NemesisGetOp>) -> String {
    let NemesisGetOp { ctx, .. } = instr.into_inner();
    log::debug!("[NEMESIS] Getting next nemesis operation with :ctx {}", ctx);
    let nem = NEMESIS.get();
    let op = nem.op(&ctx);
    log::debug!("[NEMESIS] Got operation: {}", op);
    op.to_string()
}

#[derive(FromForm)]
struct NemesisUpdate {
    ctx: String,
    event: String,
    reltime: String,
}

#[post("/nemesis/update", data = "<instr>")]
fn nemesis_update(instr: Form<NemesisUpdate>) {
    let NemesisUpdate { ctx, event, .. } = instr.into_inner();
    log::debug!("[NEMESIS] Completing: event {} in :ctx {}", event, ctx);
    let nem = NEMESIS.get();
    nem.update(&ctx, &event);
}

/************************
 * Networking functions *
 ************************/

fn setup_networking() -> Result<nfq::Queue, Box<dyn Error>> {
    // Initialise packet-processing data structures
    PACKETS.set(PacketState {
        packet_counter: AtomicUsize::new(0),
        dropped_counter: AtomicUsize::new(0),
        num_processed_packets: AtomicUsize::new(0),
        unprocessed_packets: SegQueue::new(),
        unreleased_packets: Mutex::new(VecDeque::new()),
        last_statistics_print_time: Mutex::new(Box::new(Instant::now())),
        bucket_stats: Mutex::new(Box::new(BucketProcessingStats {
            last_interval: Duration::from_secs(0),
            last_time: Instant::now(),
            last_exec_time: Duration::new(0, 0),
            last_num_packets: 0,
        })),
    });

    CLIENT.set(reqwest::Client::new());

    let exp_net = CFG.get().experiment_network;
    let unfiltered_ports = CFG.get().unfiltered_ports.clone();
    let exp_ifaces = net::util::get_experiment_interfaces(exp_net)?;
    FIREWALL.set(Firewall::new(unfiltered_ports, exp_net));

    let queue_num = CFG.get().nfqueue_queue_num;
    let queue_max_len = CFG.get().nfqueue_max_len;

    // Save existing (OS, not mediator) firewall rules to file
    let fname = &CFG.get().iptables_rules_filename;
    let iptables_rules_file = File::create(fname)?;
    let _saved_rules = Command::new("iptables-save")
        .stdout(iptables_rules_file)
        .output()?;
    log::info!("Saved iptables rules to file {}", fname);

    let nfqueue = net::util::install_nfqueue(exp_ifaces, queue_num, queue_max_len)?;
    Ok(nfqueue)
}

fn reset_iptables_rules() -> Result<(), Box<dyn Error>> {
    let fname = &CFG.get().iptables_rules_filename;
    // Flush existing IPTables rules
    let _flush = Command::new("iptables").arg("-F").output()?;
    // Restore rules we've saved previously
    match File::open(fname) {
        Err(e) => {
            log::info!(
                "File {} not found: {}. No firewall rules to restore.",
                fname,
                e
            );
            Ok(())
        }
        Ok(iptables_rules_file) => {
            let restored = Command::new("iptables-restore")
                .stdin(iptables_rules_file)
                .output()?;
            log::info!(
                "Restored iptables rules from file {}:\n{}",
                fname,
                String::from_utf8(restored.stdout)?
            );
            // Delete the file
            std::fs::remove_file(fname)?;
            Ok(())
        }
    }
}

/****************************************
 * Packet interception and manipulation *
 ****************************************/

fn print_packet_processing_statistics() {
    let num_pkts = PACKETS.get().packet_counter.load(Ordering::Relaxed);
    let num_dropped = PACKETS.get().dropped_counter.load(Ordering::Relaxed);
    log::info!(
        "[STATS] Processed {} packets so far, of which {} were dropped.",
        num_pkts,
        num_dropped
    );
}

fn print_firewall_statistics() {
    log::info!("[STATS][FIREWALL] {}", FIREWALL.get().statistics());
}

fn print_bucket_statistics(stats: &BucketProcessingStats) {
    let total_processed = PACKETS.get().num_processed_packets.load(Ordering::Relaxed);
    log::debug!(
        "[STATS] Latency: {:.2} ms; Took {:.2} ms to process {} packet(s). Total processed: {}.",
        stats.last_interval.as_secs_f32().mul(MS_PER_SEC),
        stats.last_exec_time.as_secs_f32().mul(MS_PER_SEC),
        stats.last_num_packets,
        total_processed
    );
}

fn bucket_loop() -> Result<(), Box<dyn Error>> {
    // Input: PACKETS.unprocessed_packets
    // Output: PACKETS.unreleased_packets

    // Is it time to process a packet bucket?
    let mut stats = PACKETS.get().bucket_stats.lock();
    let init = Instant::now();
    let bucket_interval = Duration::from_millis(CFG.get().bucket_interval_ms);

    // Abort if it's not yet time to collect the bucket.
    if init.saturating_duration_since(stats.last_time) < bucket_interval {
        return Ok(());
    }

    // Collect all the unprocessed packets, passing them through firewall rules
    let mut pkts: Vec<Packet> = Vec::new();
    let unproc = &PACKETS.get().unprocessed_packets;
    let fw = FIREWALL.get();
    while let Some(mut pkt) = unproc.pop() {
        fw.firewall_set_verdict(&mut pkt);
        pkts.push(pkt);
    }
    let num_processed_packets = pkts.len();

    // Order bucket in lexicographic order of sessions
    // and put them in the bucket to be released next.
    pkts.sort_by_key(|a| a.packet_identifier());

    // Mark bucket ready for release
    let mut unreleased = PACKETS.get().unreleased_packets.lock();
    for pkt in pkts {
        unreleased.push_back(pkt);
    }

    // Record statistics
    stats.last_interval = init.saturating_duration_since(stats.last_time);
    stats.last_time = init;
    stats.last_exec_time = Instant::now().saturating_duration_since(init);
    stats.last_num_packets = num_processed_packets;
    if num_processed_packets > 0 {
        print_bucket_statistics(&stats);
    }
    PACKETS
        .get()
        .num_processed_packets
        .fetch_add(num_processed_packets, Ordering::Relaxed);

    // TODO: Sleep rather than busy-wait

    Ok(())
}

fn nfqueue_loop(nfqueue: &mut nfq::Queue) -> Result<(), Box<dyn Error>> {
    print_statistics_if_time_elapsed();

    // Used for maintaining watermarks on the history of packets
    let _loop_start_time = ClockManager::get_relative_time_ns();
    // Issue verdicts for processed packets.
    let pending = PACKETS.get().unreleased_packets.try_lock();
    if let Ok(mut pending) = pending {
        let packets = &HISTORY.get().packets;
        while let Some(mut pkt) = pending.pop_front() {
            log::debug!("{}", pkt);

            if pkt.has_been_dropped() {
                PACKETS
                    .get()
                    .dropped_counter
                    .fetch_add(1, Ordering::Relaxed);
            }

            // I am not sure why a verdict might fail to be issued.
            // Maybe the queue filled up and the packet got dropped?
            if let Err(msg) = nfqueue.verdict(
                pkt.msg
                    .expect("tried to issue verdict for packet without nfqueue `msg`"),
            ) {
                log::warn!("Error issuing verdict for {:#?}!", msg);
            }

            // Ditch nfqueue msg and add packet to history
            pkt.msg = None; // FIXME: is there some way to not need this? mark_emitted() does it
            pkt.mark_emitted();
            packets.emitted_packet(pkt);
        }
    }

    let fd: RawFd = nfqueue.as_raw_fd();
    let mut readfds = FdSet::new();
    readfds.insert(fd);
    let timeout_ns = CFG.get().nfqueue_select_timeout_ns;
    let mut wait_timeout = nix::sys::time::TimeVal::nanoseconds(timeout_ns);

    // Consume as many packets as are available on the NFQUEUE.
    loop {
        // We use select() rather than busy-looping to keep CPU usage sensible
        let ready = select(
            fd + 1,
            Some(&mut readfds),
            None,
            None,
            Some(&mut wait_timeout),
        )?;
        if ready > 0 {
            let msg = match nfqueue.recv() {
                Ok(msg) => msg,
                // Terminate early if the recv() would block; not an actual error
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
                Err(err) => return Err(err)?,
            };

            // Put the Packet into our internal queue
            // Increment packet counter
            let pkt_id = PACKETS.get().packet_counter.fetch_add(1, Ordering::Relaxed);
            let pkt = Packet::new(msg, pkt_id);
            let packets = &HISTORY.get().packets;
            packets.received_packet(&pkt);
            let unproc = &PACKETS.get().unprocessed_packets;
            unproc.push(pkt);

            // We do not make an accept/drop decision when we receive the packet.
            // Rather, we postpone the decision until the packet is ready to be sent.
            // WARNING: because the kernel uses a linked-list to store the queued packets,
            // performance may degrade if packets are delayed for a long time.
            // Also, if `nfqueue_max_len` is exceeded, packets are dropped without warning.
        } else {
            // We've cleared the kernel queue, so it should be the case that
            // we will not receive packets with timestamp before the start of this function.
            // Moreover, if we've emitted all packets, i.e., both `unprocessed_packets`
            // and `unreleased_packets` are empty, then we can be sure no more packets
            // will ever be added with a timestamp before `loop_start_time`.

            // NOTE that we may still receive Jepsen ops "in the past", which is
            // why the `submitted_all_before` function actually registers a watermark
            // a few seconds before the passed argument.
            // TODO: we should handle Jepsen ops properly, without relying on delays.
            // That requires adding a sequence number/heartbeat mechanism to Jepsen.

            // Exit out of the loop.
            break;
        }
    }
    Ok(())
}

/*****************************
 * Configuration and logging *
 *****************************/

fn read_configuration(schedule_type: &String, feedback_type: &String, state_similarity_threshold: f64) -> Result<(), Box<dyn Error>> {
    let settings = Config::builder()
        .add_source(config::File::with_name("Mediator"))
        // Add in settings from env, with a prefix of MED, e.g. MED_DEBUG=1
        .add_source(config::Environment::with_prefix("MED"))
        .build()?;

    let _exp_net = settings.get_string("experiment_network")?;
    let experiment_network = IpNetwork::from_str(&_exp_net)?;
    let unfiltered_ports: Vec<u16> = settings
        .get_array("unfiltered_ports")?
        .into_iter()
        .map(|v| v.into_uint().unwrap() as u16)
        .collect();
    let node_name_format = settings.get_string("node_name_format")?;
    let bucket_interval_ms = settings.get("bucket_interval_ms")?;

    let clocks_synchronized_to_within_ms = settings.get("clocks_synchronized_to_within_ms")?;
    let statistics_print_interval_secs = settings.get("statistics_print_interval_secs")?;

    let nfqueue_queue_num = settings.get("nfqueue_queue_num")?;
    let nfqueue_max_len = settings.get("nfqueue_max_len")?;
    let nfqueue_select_timeout_ns = settings.get("nfqueue_select_timeout_ns")?;

    let nemesis_schedule_duration_ms = settings.get("nemesis_schedule_duration_ms")?;
    let nemesis_schedule_interval_ms = settings.get("nemesis_schedule_interval_ms")?;
    let nemesis_reset_duration_ms = settings.get("nemesis_reset_duration_ms")?;

    let mediator_log_filename = settings.get_string("mediator_log_filename")?;
    let shiviz_log_filename = settings.get_string("shiviz_log_filename")?;
    let event_log_filename = settings.get_string("event_log_filename")?;
    let iptables_rules_filename = settings.get_string("iptables_rules_filename")?;

    let exp_ifaces = net::util::get_experiment_interfaces(experiment_network)?;
    let num_nodes = exp_ifaces.len() as u8;
    // Logging is not yet configured when configuration is read, so we print to stderr
    eprintln!("[CFG] Found {} nodes:", num_nodes);
    let node_ips = (1..=num_nodes)
        // we cannot use format! on a non-literal string, so we cheat a little bit
        .map(|i| node_name_format.replacen("{}", &i.to_string(), 1))
        .map(|name| {
            let ip = net::util::resolve_to_experiment_ip(&name, experiment_network);
            eprintln!("  {} -> {}", name, ip);
            ip
        })
        .collect();

    let schedule_type = schedule_type.clone();
    let feedback_type = feedback_type.clone();
    let state_similarity_threshold = state_similarity_threshold;

    let cfg = MediatorConfig {
        experiment_network,
        node_name_format,
        unfiltered_ports,
        bucket_interval_ms,

        clocks_synchronized_to_within_ms,
        statistics_print_interval_secs,

        nfqueue_queue_num,
        nfqueue_max_len,
        nfqueue_select_timeout_ns,

        nemesis_schedule_duration_ms,
        nemesis_schedule_interval_ms,
        nemesis_reset_duration_ms,

        mediator_log_filename,
        shiviz_log_filename,
        event_log_filename,
        iptables_rules_filename,

        num_nodes,
        node_ips,

        schedule_type,
        feedback_type,
        state_similarity_threshold,
    };

    CFG.set(cfg);
    Ok(())
}

fn setup_logging() -> Result<(), fern::InitError> {
    let log_file = File::create(CFG.get().mediator_log_filename.clone())?;
    let log_level = if cfg!(debug_assertions) {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Info
    };

    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}] {}",
                ClockManager::utc_now().format("[%Y-%m-%d %H:%M:%S.%6f]"),
                record.level(),
                message
            ))
        })
        .level(log_level)
        .chain(std::io::stdout())
        .chain(log_file)
        .apply()?;
    Ok(())
}

fn setup_history(feedback_type: &String) {
    let node_ips = &CFG.get().node_ips;
    let event_log_filename = &CFG.get().event_log_filename;
    let shiviz_log_filename = &CFG.get().shiviz_log_filename;
    let sync_ms = CFG.get().clocks_synchronized_to_within_ms;
    let state_similarity_threshold = CFG.get().state_similarity_threshold;
    HISTORY.set(Arc::new(History::new(
        node_ips,
        event_log_filename,
        shiviz_log_filename,
        sync_ms,
        feedback_type,
        state_similarity_threshold,
    )));
}

/// Print memory usage statistics.
fn print_memory_usage_statistics() {
    let e = epoch::mib().unwrap();
    let allocated = stats::allocated::mib().unwrap();
    let resident = stats::resident::mib().unwrap();

    // Many stats are only updated when the epoch is advanced.
    e.advance().unwrap();
    let allocated = allocated.read().unwrap();
    let resident = resident.read().unwrap();
    log::info!(
        "[STATS][MEM] {} allocated / {} resident",
        Byte::from_bytes(allocated as u128).get_appropriate_unit(false),
        Byte::from_bytes(resident as u128).get_appropriate_unit(false),
    )
}

/// Print history statistics.
fn print_history_statistics() {
    let h = HISTORY.get();
    h.print_summary();
}

fn print_nemesis_statistics() {
    if let Some(n) = NEMESIS.try_get() {
        n.print_statistics();
    } else {
        log::info!("[STATS][NEMESIS] Nemesis not initialised yet, so statistics are not printed.");
    }
}

fn print_statistics_if_time_elapsed() {
    let now = Instant::now();
    let last_printed = PACKETS.get().last_statistics_print_time.try_lock();
    if let Ok(mut last_printed) = last_printed {
        let print_interval = CFG.get().statistics_print_interval_secs;
        if now.saturating_duration_since(**last_printed).as_secs() >= print_interval {
            log::info!("--- BEGIN Statistics ---");
            print_packet_processing_statistics();
            print_firewall_statistics();
            print_memory_usage_statistics();
            print_history_statistics();
            print_nemesis_statistics();
            log::info!("--- END Statistics ---");
            **last_printed = now;
        }
    }
}

/**********************
 * History processing *
 **********************/
fn history_and_summary_loop() -> Result<(), Box<dyn Error>> {
    let h = HISTORY.get();
    h.tick();
    std::thread::sleep(Duration::from_millis(10));
    Ok(())
}

/****************************
 * Main program entry point *
 ****************************/

#[rocket::main]
async fn main() -> Result<(), rocket::Error> {
    // Collect command-line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        eprintln!(
            "Usage: {} schedule_type feedback_type state_similarity_threshold\n schedule_type: one of [power, qlearning]\n feedback_type: one of [event_history, afl_branch, afl_branch_and_event_history] \n state_similarity_threshold: a float between 0 and 1",
            args[0]
        );
        std::process::exit(1);
    }
    let schedule_type = &args[1];
    let feedback_type = &args[2];
    let state_similarity_threshold = args[3].parse::<f64>().unwrap();

    // Initialisation
    read_configuration(schedule_type, feedback_type, state_similarity_threshold).expect("could not parse configuration");
    setup_logging().expect("could not set up logging");
    let mut nfqueue = setup_networking().expect("could not set up networking");
    setup_history(feedback_type);

    // Restore firewall rules to their initial state upon Ctrl + C or at normal program exit
    ctrlc::set_handler(move || {
        log::info!("Ctrl-C received.");
        log::logger().flush();
        reset_iptables_rules().expect("could not reset firewall rules to original value");
        dump_run();
    })
    .expect("could not set ctrl-C handler");

    let orig_hook = panic::take_hook();
    // Add a panic hook such that any thread-panic ends the entire process
    panic::set_hook(Box::new(move |panic_info| {
        log::error!("Panic!");
        log::logger().flush();
        orig_hook(panic_info);
        log::error!("thread panic: {:?}", panic_info);
        reset_iptables_rules().expect("could not reset firewall rules to original value");
        dump_run();
        process::exit(1);
    }));

    // Spawn a thread for the nfqueue loop. This runs forever.
    let _nfqueue_loop = thread::spawn(move || loop {
        match nfqueue_loop(&mut nfqueue) {
            Ok(_) => (),
            Err(err) => panic!("Nfqueue loop failed: {:#?}", err),
        }
    });

    // Spawn a thread for the packet bucket loop. This runs forever.
    let _bucket_loop = thread::spawn(move || loop {
        match bucket_loop() {
            Ok(_) => (),
            Err(err) => panic!("Bucket loop failed: {:#?}", err),
        }
    });

    // Spawn a thread to maintain the history and summaries.
    let _history_and_summary_loop = thread::spawn(move || loop {
        match history_and_summary_loop() {
            Ok(_) => (),
            Err(err) => panic!("History and summary loop failed: {:#?}", err),
        }
    });

    // Spawn the web framework.
    let rocket = rocket::build()
        .mount(
            "/",
            routes![
                firewall_drop,
                firewall_heal,
                firewall_enact,
                nemesis_setup,
                nemesis_get_op,
                nemesis_update,
                start_test,
                end_test,
                start_time,
                before_tear_down,
                submit_sequence_number,
                collect_local_events,
                invoke_operation,
                complete_operation,
            ],
        )
        .ignite()
        .await?;
    let _rocket = rocket.launch().await?;

    Ok(())
}
