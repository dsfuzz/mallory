#[macro_use]
extern crate rocket;
use chrono::Utc;
use config::Config;
use dns_lookup::lookup_host;
use ipnetwork::IpNetwork;
use local_ip_address::local_ip;
use nix::time;
use reqwest::header::HeaderMap;
use rocket::serde::json::Json;
use state::Storage;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs::{self, File};
use std::path::Path;
use std::process::Command;
use std::str::FromStr;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::{AtomicI32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;
use std::time::UNIX_EPOCH;
use tokio::time::{sleep, Duration};

use libc::{c_void, shmat, shmdt, shmget};
use lz4_flex::compress_prepend_size;

mod net_util;
mod raw_packet;

#[derive(Debug)]
pub struct CoverageConfig {
    iptables_rules_filename: String,

    experiment_network: IpNetwork,
    nfqueue_queue_num: u16,
    nfqueue_max_len: u32,

    batch_duration_ms: u64,
    mediator_events_url: String,
    mediator_seq_num_url: String,
}

static CFG: Storage<CoverageConfig> = Storage::new();

static BATCH_ID: AtomicU64 = AtomicU64::new(0);
static PACKETS: Mutex<Vec<PacketEntry>> = Mutex::new(Vec::new());

// To save DSFuzz events
static DB_SHM_SIZE: usize = 24 * (1 << 16);
static DB_SHM_ID: AtomicI32 = AtomicI32::new(0);

// To save AFL branch coverage
static AFL_SHM_SIZE: usize = 1 << 16;
static AFL_SHM_ID: AtomicI32 = AtomicI32::new(0);

const PACKET_SEND_EVENT_TYPE: u64 = 3;
const PACKET_RECV_EVENT_TYPE: u64 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ClearType {
    FirstEntry,
    All,
}

/// To conduct periodic bug detection
/// To record the current detected line
static UNTIL_LOG_LINE: AtomicUsize = AtomicUsize::new(0);
/// We use created time of the log file as the log identifier to
/// detect whether the log file is replaced
static LOG_IDENTIFIER: AtomicU64 = AtomicU64::new(0);
/// bug patterns to match bugs in log files
const BUG_PATTERNS: &str = "panic|fatal|error: AddressSanitizer|exception|assert|Assertion|segmentation fault|core dumped|aborted|bug|stack trace";

/// packet event entry
/// The size of each entry is 18 bytes. If we need larger size, do note correspondingly
/// to adjust the size of shared memory and the event parser in the mediator.
#[repr(C, align(8))]
#[derive(Copy, Clone)]
struct PacketEntry {
    event_type: u64,     // 3 for packet sent, 4 for packet received
    monotonic_time: u64, // monotonic timestamp
    packet_id: u64,      // hashed packet identifier
}

#[repr(C, align(8))]
struct CounterEntry {
    event_counter: AtomicU16, // event counter in 0th entry
    event_batch_num: u64,     // event batch number
}

/// union of event counter and packet entry
#[repr(C, align(8))]
union Event {
    counter: std::mem::ManuallyDrop<CounterEntry>, // event counter in 0th entry
    // This is technically an instrumented event, but it's the same size as a packet event
    entry: PacketEntry, // packet event
}

fn setup_logging() {
    let log_level = if cfg!(debug_assertions) {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Info
    };

    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}] {}",
                Utc::now().format("[%Y-%m-%d %H:%M:%S.%6f]"),
                record.level(),
                message
            ))
        })
        .level(log_level)
        .chain(std::io::stdout())
        .apply()
        .expect("Failed to setup logging!");
}

/// Sniff packets and record packet events into shared memory
fn packet_sniff_loop() {
    let experiment_network = CFG.get().experiment_network;
    let host_addr = local_ip().unwrap();
    assert!(
        experiment_network.contains(host_addr),
        "The host address should be in the experiment network!"
    );
    let host_ip = host_addr.to_string();

    let mediator_name = "control";
    let mediator_addr = lookup_host(mediator_name).unwrap()[0];
    let mediator_ip = mediator_addr.to_string();

    let queue_num = CFG.get().nfqueue_queue_num;
    let queue_max_len = CFG.get().nfqueue_max_len;

    // Save existing (OS, not coverage-server) firewall rules to file
    let fname = &CFG.get().iptables_rules_filename;
    let iptables_rules_file = File::create(fname).expect("Cannot create iptables rules file");
    let _saved_rules = Command::new("iptables-save")
        .stdout(iptables_rules_file)
        .output()
        .expect("Cannot save iptables rules");
    log::info!("Saved iptables rules to file {}", fname);

    let mut nfqueue = net_util::install_nfqueue(host_ip, mediator_ip, queue_num, queue_max_len)
        .expect("Cannot install nfqueue");

    loop {
        match nfqueue.recv() {
            Ok(mut msg) => {
                let payload = msg.get_payload().to_owned();

                let monotonic_ns = get_relative_time_ns();
                msg.set_verdict(nfq::Verdict::Accept);
                if let Err(msg) = nfqueue.verdict(msg) {
                    log::warn!("Error issuing verdict for {:#?}!", msg);
                }

                let raw_pkt = raw_packet::RawPacket::new(payload);
                let (packet_identifier, _transport_payload) = raw_pkt.parse_packet();
                let network_id = packet_identifier.network_id();

                if let (Some(src_ip), Some(dst_ip)) =
                    (packet_identifier.src_ip, packet_identifier.dst_ip)
                {
                    if !experiment_network.contains(src_ip)
                        || !experiment_network.contains(dst_ip)
                        || src_ip == mediator_addr
                        || dst_ip == mediator_addr
                    {
                        log::debug!(
                        "[MEDIATOR] Packet from {} to {} is not in the experiment network. Skipping {:?} (network_id {}) | pkt: {:?} @ {}.",
                        src_ip,
                        dst_ip,
                        packet_identifier,
                        network_id,
                        raw_pkt.packet,
                        monotonic_ns
                    );
                        continue;
                    }

                    let _batch_id = BATCH_ID.load(Ordering::Relaxed);
                    let event_type = if src_ip == host_addr {
                        PACKET_SEND_EVENT_TYPE
                    } else {
                        PACKET_RECV_EVENT_TYPE
                    };

                    let entry = PacketEntry {
                        event_type,
                        monotonic_time: monotonic_ns,
                        packet_id: network_id,
                    };

                    let mut packets = PACKETS.lock().expect("Another thread panicked!");
                    packets.push(entry);
                    let event_index = packets.len() - 1;

                    log::debug!(
                        "[Batch {} Entry {}] {} {:?} (network_id {}) | pkt: {:?} @ {}",
                        _batch_id,
                        event_index,
                        if event_type == PACKET_RECV_EVENT_TYPE {
                            "RECV"
                        } else {
                            "SEND"
                        },
                        packet_identifier,
                        network_id,
                        raw_pkt.packet,
                        monotonic_ns
                    );
                } else {
                    log::info!(
                        "[UNPARSED] {:?} (network_id {}) | pkt: {:?} @ {}",
                        packet_identifier,
                        network_id,
                        raw_pkt.packet,
                        monotonic_ns
                    );
                }
            }
            Err(_e) => {}
        }
    }
}

fn log_detection_loop(log_file: String) {
    // Get the starting time to calculate the passed time
    let starting_time = get_relative_time_ns();
    let mut bug_counter: u64 = 0;

    // Conduct periodic bug detection
    loop {
        // Check whether the log file exists,
        // since cov-server starts earlier than target applications
        if !Path::new(&log_file).exists() {
            thread::sleep(Duration::from_millis(CFG.get().batch_duration_ms));
            continue;
        }

        // Get the created time of the log file to check whether the log file is replaced
        let metadata = fs::metadata(&log_file).expect("Reading metadata failed");
        let created_time = metadata.created().expect("Reading created time failed");
        let cur_log_identifier = created_time.duration_since(UNIX_EPOCH).unwrap().as_secs();
        let historical_log_identifier = LOG_IDENTIFIER.load(Ordering::Relaxed);

        // Log file is replaced
        if historical_log_identifier != cur_log_identifier {
            LOG_IDENTIFIER.store(cur_log_identifier, Ordering::Relaxed);
            UNTIL_LOG_LINE.store(0, Ordering::Relaxed);
        }

        // Get the total line number in the log file
        // Use `wc -l` to get line number
        let output_line = Command::new("wc")
            .arg("-l")
            .arg(&log_file)
            .output()
            .expect("Failed to execute wc command");

        // Extract the line number
        let total_line = String::from_utf8(output_line.stdout)
            .expect("Failed to convert output to string")
            .split_whitespace()
            .next()
            .expect("Failed to get line number")
            .parse::<usize>()
            .expect("Failed to parse line number");

        // Search the log file to find keywords
        // Use the `ag` command to match the bug patterns
        let bug_output = Command::new("ag")
            .arg("-n")
            .arg("-i")
            .arg("-w")
            .arg(BUG_PATTERNS)
            .arg(&log_file)
            .output()
            .expect("Failed to execute ag command");

        // Get the bug contents lied in between the current line and the total line
        let bug_contents = String::from_utf8(bug_output.stdout)
            .expect("Failed to convert output to string")
            .lines()
            .map(|line| {
                let line_number = line.split(':').next().unwrap().parse::<usize>().unwrap();
                (line_number, line.to_string())
            })
            .filter(|(line_num, _)| {
                *line_num > UNTIL_LOG_LINE.load(Ordering::Relaxed) && *line_num <= total_line
            })
            .map(|(_, line_content)| line_content)
            .collect::<Vec<String>>();

        // Update the current line number
        UNTIL_LOG_LINE.store(total_line, Ordering::Relaxed);

        if !bug_contents.is_empty() {
            let now = get_relative_time_ns();
            let pass_time_ns = now - starting_time;
            log::info!(
                "[BUG Summary] {}/{} bugs found after {} ns shown as follows:",
                bug_contents.len(),
                bug_counter + bug_contents.len() as u64,
                pass_time_ns,
            );

            for bug_content in bug_contents {
                log::info!(
                    "[BUG DETECTION] #{}. {} (after {} ns)",
                    bug_counter,
                    bug_content,
                    pass_time_ns
                );

                // increase the bug counter
                bug_counter += 1;
            }
        }

        // sleep for BATCH_DURATION_MS
        let break_time = Duration::from_millis(CFG.get().batch_duration_ms);
        thread::sleep(break_time);
    }
}

fn clear_shared_memory(shm_id: &AtomicI32, shm_size: usize, clear_type: ClearType) {
    if clear_type == ClearType::FirstEntry {
        let shm =
            unsafe { shmat(shm_id.load(Ordering::Relaxed), std::ptr::null_mut(), 0) } as *mut Event;
        if shm.is_null() {
            log::info!("Failed to attach to shared memory");
            // Should be unreachable
            return;
        }
        let event_index = unsafe { &mut *shm };
        unsafe {
            event_index
                .counter
                .event_counter
                .store(0, Ordering::Relaxed)
        };

        // detach the shared memory
        unsafe {
            shmdt(shm as *mut c_void);
        }
    } else if clear_type == ClearType::All {
        let shm =
            unsafe { shmat(shm_id.load(Ordering::Relaxed), std::ptr::null_mut(), 0) } as *mut u8;
        if shm.is_null() {
            log::info!("Failed to attach to shared memory");
            // Should be unreachable
            return;
        }

        // clear the whole shared memory
        unsafe {
            std::ptr::write_bytes(shm, 0, shm_size);
        }

        // detach the shared memory
        unsafe {
            shmdt(shm as *mut c_void);
        }
    }
}

fn collect_events(shm_id: &AtomicI32, shm_size: usize) -> String {
    let shm = unsafe { shmat(shm_id.load(Ordering::Relaxed), std::ptr::null_mut(), 0) } as *mut u8;

    if shm.is_null() {
        log::info!("Failed to attach to shared memory");
        // Should be unreachable
        return "".to_string();
    }

    let cached_data = compress_prepend_size(unsafe { std::slice::from_raw_parts(shm, shm_size) });

    // detach the shared memory
    unsafe {
        shmdt(shm as *mut c_void);
    }

    base64::encode(cached_data)
}

unsafe fn any_as_u8_slice<T: Sized>(p: &T) -> &[u8] {
    ::std::slice::from_raw_parts((p as *const T) as *const u8, ::std::mem::size_of::<T>())
}

fn collect_and_clear_packets() -> String {
    let mut packets = PACKETS.lock().unwrap();
    let mut bytes = vec![];
    for p in packets.drain(..) {
        let p_bytes = unsafe { any_as_u8_slice(&p) }.to_vec();
        bytes.push(p_bytes);
    }
    // Concat all packet bytes
    let bytes = bytes.concat();
    let cached_data = compress_prepend_size(bytes.as_slice());
    base64::encode(cached_data)
}

#[get("/")]
fn index() -> Json<String> {
    Json(format!("Welcome to coverage server!"))
}

#[post("/coverage/clear-event-shm")]
fn clear_shm() -> String {
    // Clear shared memory for code events
    clear_shared_memory(&DB_SHM_ID, DB_SHM_SIZE, ClearType::FirstEntry);

    // Clear the packets
    let mut packets = PACKETS.lock().unwrap();
    packets.clear();

    // Clear shared memory for AFL events
    clear_shared_memory(&AFL_SHM_ID, AFL_SHM_SIZE, ClearType::All);

    format!("Clear shared memory ...")
}

#[post("/state/fuzz-starting")]
async fn start_fuzzing() -> String {
    // Before starting fuzzing, there are some communications between nodes.
    // If we clear the shared memory here, we would miss these packets.
    // So instead, we ensure to post all the packets after fuzzing stopping.
    // clear_shm();
    post_and_increase_seq_num().await;
    // IS_FUZZING.store(true, Ordering::SeqCst);
    format!("Start fuzzing ...")
}

fn get_relative_time_ns() -> u64 {
    let monotonic_origin = time::clock_gettime(time::ClockId::CLOCK_MONOTONIC).unwrap();
    let monotonic_origin_ns = (monotonic_origin.tv_sec() as u64)
        .checked_mul(1_000_000_000)
        .unwrap()
        .checked_add(monotonic_origin.tv_nsec() as u64)
        .unwrap();
    monotonic_origin_ns
}

fn get_time_ns() -> (i64, u64) {
    let absolute_origin = time::clock_gettime(time::ClockId::CLOCK_REALTIME).unwrap();
    let monotonic_origin = time::clock_gettime(time::ClockId::CLOCK_MONOTONIC).unwrap();

    let absolute_origin_ns = absolute_origin
        .tv_sec()
        .checked_mul(1_000_000_000)
        .unwrap()
        .checked_add(absolute_origin.tv_nsec())
        .unwrap();

    let monotonic_origin_ns = (monotonic_origin.tv_sec() as u64)
        .checked_mul(1_000_000_000)
        .unwrap()
        .checked_add(monotonic_origin.tv_nsec() as u64)
        .unwrap();

    (absolute_origin_ns, monotonic_origin_ns)
}

/// Send our sequence number to the mediator and increase it afterwards.
/// We increase it so it doesn't conflict with the next-submitted batch.
/// We also report our CLOCK_REALTIME and CLOCK_MONOTONIC to the mediator.
async fn post_and_increase_seq_num() {
    let client = reqwest::Client::new();
    // add headers
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", "application/json".parse().unwrap());
    let id = BATCH_ID.load(Ordering::SeqCst);
    let (absolute_origin_ns, monotonic_origin_ns) = get_time_ns();
    let mut req = HashMap::new();
    req.insert("seq_num", id.to_string());
    req.insert("absolute_origin_ns", absolute_origin_ns.to_string());
    req.insert("monotonic_origin_ns", monotonic_origin_ns.to_string());
    let resp = client
        .post(&CFG.get().mediator_seq_num_url)
        .headers(headers)
        .json(&req)
        .send()
        .await;
    if resp.is_err() {
        log::info!(
            "Failed to send SeqNum & ClockDescriptor to mediator: {:?}",
            resp
        );
    }
}

#[post("/state/fuzz-stopping")]
fn stop_fuzzing() -> String {
    // Post remaining data into mediator after fuzzing stopping.
    // tokio::spawn(post_events_to_mediator())
    //     .await
    //     .map_err(|err| log::info!("{:?}", err))
    //     .ok();
    // IS_FUZZING.store(false, Ordering::Relaxed);
    format!("Stop fuzzing ...")
}

async fn post_events_to_mediator() {
    // post events while fuzzing
    // if IS_FUZZING.load(Ordering::Relaxed) {
    let now = get_relative_time_ns();

    // Collect data from DSFuzz shared memory
    // Get code events and clear db shared memory
    let db_data = collect_events(&DB_SHM_ID, DB_SHM_SIZE);
    clear_shared_memory(&DB_SHM_ID, DB_SHM_SIZE, ClearType::FirstEntry);

    // Get packet events and clear packet shared memory
    let pkt_data = collect_and_clear_packets();

    // Get AFL events and clear AFL shared memory
    let afl_data = collect_events(&AFL_SHM_ID, AFL_SHM_SIZE);
    clear_shared_memory(&AFL_SHM_ID, AFL_SHM_SIZE, ClearType::All);

    // Get and update batch id
    let id = BATCH_ID.fetch_add(1, Ordering::SeqCst);

    let client = reqwest::Client::new();

    // add headers
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", "application/json".parse().unwrap());

    // add data
    let mut req = HashMap::new();
    req.insert("batch_id", id.to_string());
    req.insert("rel_ts", now.to_string());
    req.insert("db_cached_data", db_data);
    req.insert("pkt_cached_data", pkt_data);
    req.insert("afl_cached_data", afl_data);

    log::info!("[POST_DATA] Post batch_id: {}", id);

    // post message
    let response = client
        .post(&CFG.get().mediator_events_url)
        .headers(headers)
        .json(&req)
        .send()
        .await;
    if response.is_err() {
        log::info!(
            "[POST_DATA] Failed to post events to mediator: {:?}",
            response
        );
    }

    // drop the client
    drop(client);
    // }
}

async fn create_server_client() -> Result<(), rocket::Error> {
    // Spawn a separate thread for the main loop. This runs forever.
    let _post_events_loop = tokio::spawn(async {
        loop {
            post_events_to_mediator().await;
            let break_time = Duration::from_millis(CFG.get().batch_duration_ms);
            sleep(break_time).await;
        }
    });

    // Spawn the web framework.
    let figment = rocket::Config::figment()
        .merge(("port", 58080))
        .merge(("address", "0.0.0.0"));
    let rocket = rocket::custom(figment)
        .mount("/", routes![index, start_fuzzing, stop_fuzzing, clear_shm])
        .ignite()
        .await?;
    let _rocket = rocket.launch().await?;
    Ok(())
}

fn read_configuration() -> Result<(), Box<dyn Error>> {
    // Get the configuration file from the current directory, if it exists
    // otherwise from a standard directory.
    let cwd_path = Path::new("Coverage.toml");
    let abs_path = Path::new("/opt/cov-server/Coverage.toml");
    let cfg_path = if cwd_path.exists() {
        cwd_path
    } else if abs_path.exists() {
        abs_path
    } else {
        panic!("No configuration file found!");
    };

    let settings = Config::builder()
        .add_source(config::File::new(
            cfg_path.to_str().unwrap(),
            config::FileFormat::Toml,
        ))
        // Add in settings from env, with a prefix of COV, e.g. COV_DEBUG=1
        .add_source(config::Environment::with_prefix("COV"))
        .build()?;

    let iptables_rules_filename = settings.get_string("iptables_rules_filename")?;

    let _exp_net = settings.get_string("experiment_network")?;
    let experiment_network = IpNetwork::from_str(&_exp_net)?;

    let nfqueue_queue_num = settings.get("nfqueue_queue_num")?;
    let nfqueue_max_len = settings.get("nfqueue_max_len")?;

    let batch_duration_ms = settings.get("batch_duration_ms")?;
    let mediator_events_url = settings.get("mediator_events_url")?;
    let mediator_seq_num_url = settings.get("mediator_seq_num_url")?;

    let cfg = CoverageConfig {
        iptables_rules_filename,
        experiment_network,
        nfqueue_queue_num,
        nfqueue_max_len,
        batch_duration_ms,
        mediator_events_url,
        mediator_seq_num_url,
    };

    CFG.set(cfg);
    Ok(())
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

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // Init
    setup_logging();
    log::info!(
        "Working dir: {}",
        std::env::current_dir().unwrap().display()
    );
    read_configuration().expect("could not parse configuration");
    log::info!("Configuration {:?}", CFG.get());

    // Restore firewall rules to their initial state upon Ctrl + C or at normal program exit
    ctrlc::set_handler(move || {
        reset_iptables_rules().expect("could not reset firewall rules to original value");
    })
    .expect("could not set ctrl-C handler");

    let orig_hook = std::panic::take_hook();
    // Add a panic hook such that any thread-panic ends the entire process
    std::panic::set_hook(Box::new(move |panic_info| {
        orig_hook(panic_info);
        log::error!("thread panic: {:?}", panic_info);
        reset_iptables_rules().expect("could not reset firewall rules to original value");

        std::process::exit(1);
    }));

    // Create shared memory
    log::info!("Init shared memory ...");

    // Create shared memory for DSFuzz block/function events
    let db_shm_id = unsafe { shmget(libc::IPC_PRIVATE, DB_SHM_SIZE, libc::IPC_CREAT | 0o666) };
    if db_shm_id == -1 {
        log::info!("Fail to create shared memory");
        return;
    }
    DB_SHM_ID.store(db_shm_id, Ordering::Relaxed);
    fs::write("/opt/shm/dsfuzz_shm_id", db_shm_id.to_string())
        .expect("Cannot write db SHM ID into file");

    // Create shared memory for AFL
    let afl_shm_id = unsafe { shmget(libc::IPC_PRIVATE, AFL_SHM_SIZE, libc::IPC_CREAT | 0o666) };
    if afl_shm_id == -1 {
        log::info!("Fail to create AFL shared memory");
        return;
    }
    AFL_SHM_ID.store(afl_shm_id, Ordering::Relaxed);
    fs::write("/opt/shm/afl_shm_id", afl_shm_id.to_string())
        .expect("Cannot write AFL SHM ID into file");

    // Spawn a thread to sniff packets
    let _packet_sniff_loop = thread::spawn(packet_sniff_loop);

    // Spawn a thread to detect bugs in logs
    let args = env::args().collect::<Vec<String>>();
    if args.len() < 2 {
        log::info!("Please provide the log file");
        return;
    }
    let log_file = args[1].clone();
    let _log_detection_loop = thread::spawn(move || log_detection_loop(log_file));

    create_server_client()
        .await
        .map_err(|err| log::info!("{:?}", err))
        .ok();
}
