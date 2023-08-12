pub mod common;
pub mod packets;
pub mod time;
pub mod timeline;

use std::path::Path;
use std::sync::Arc;

use antidote::Mutex;
use antidote::RwLock;
use chrono::{DateTime, Utc};

use crate::feedback::reward::SummaryTask;
use crate::feedback::summary::SummaryManager;
use crate::feedback::FeedbackManager;
use crate::nemesis::AdaptiveNemesis;

use self::common::{NodeIp, OrderedSet};
use self::packets::Packets;
use self::time::ClockManager;
use self::timeline::CommitManager;
use self::timeline::DynamicTimeline;

/// A history of a run.
pub struct History {
    pub name: RwLock<String>,
    pub store_path: RwLock<String>,
    pub start_time: RwLock<Box<DateTime<Utc>>>,
    pub end_time: RwLock<Box<DateTime<Utc>>>,

    pub clocks: Arc<RwLock<ClockManager>>,

    pub timeline: Arc<DynamicTimeline>,
    pub summaries: Arc<Mutex<SummaryManager>>,
    pub feedback: Arc<FeedbackManager>,
    pub packets: Arc<Packets>,

    pub nemesis: Mutex<Option<Arc<dyn AdaptiveNemesis>>>,
    pub task_queue: OrderedSet<SummaryTask>,
}

impl History {
    pub fn new(
        node_ips: &Vec<NodeIp>,
        event_log_filename: &String,
        shiviz_log_filename: &String,
        clocks_synchronized_to_within_ms: u64,
        feedback_type: &String,
        state_similarity_threshold: f64,
    ) -> Self {
        let clocks = Arc::new(RwLock::new(ClockManager::new(
            clocks_synchronized_to_within_ms,
        )));
        let packets = Arc::new(Packets::new());
        let now = ClockManager::utc_now();

        let commit_manager = Arc::new(CommitManager::new(clocks.clone()));

        let summaries = Arc::new(Mutex::new(SummaryManager::new(
            commit_manager.clone(),
            event_log_filename,
            shiviz_log_filename,
            feedback_type,
            state_similarity_threshold,
        )));
        let timeline = Arc::new(DynamicTimeline::new(summaries.clone(), commit_manager));
        let feedback = Arc::new(FeedbackManager::new(
            node_ips,
            clocks.clone(),
            packets.clone(),
            timeline.clone(),
        ));

        Self {
            name: RwLock::new(String::new()),
            store_path: RwLock::new(String::new()),
            start_time: RwLock::new(Box::new(now)),
            end_time: RwLock::new(Box::new(now)),

            clocks,
            timeline,
            summaries,
            feedback,
            packets,
            nemesis: Mutex::new(None),
            task_queue: OrderedSet::new(),
        }
    }

    pub fn tick(&self) {
        self.timeline.tick();
        self.summaries.lock().tick();
    }

    pub fn start(&self) {
        self.feedback.start();
    }

    pub fn has_started(&self) -> bool {
        self.feedback.has_started()
    }

    pub fn start_new_history(
        &self,
        old_schedule_id: usize,
        old_start_time: DateTime<Utc>,
        old_end_time: DateTime<Utc>,
        new_start_time: DateTime<Utc>,
    ) {
        self.feedback.start_new_history(
            old_schedule_id,
            old_start_time,
            old_end_time,
            new_start_time,
        );
    }

    /// Must request rewards in monotonically increasing order of time!
    /// NOTE: Causal links don't get added until the reward is requested.
    pub fn request_reward(&self, task: SummaryTask) {
        let will_not_request_before = task.end_ts;
        self.feedback.request_reward_for_window(task);
        self.feedback
            .nemesis_submitted_all_before(will_not_request_before);
    }

    pub fn register_nemesis(&self, nemesis: Arc<dyn AdaptiveNemesis>) {
        let mut nem = self.nemesis.lock();
        self.summaries.lock().register_nemesis(nemesis.clone());
        *nem = Some(nemesis);
    }

    pub fn print_summary(&self) {
        self.timeline.print_summary();
        self.feedback.print_watermarks();
    }

    /// Returns the path where we should copy the mediator log.
    pub fn dump(&self) -> String {
        let store_path_str = self.store_path.read();
        let store_path = Path::new(store_path_str.as_str());
        self.packets.dump(store_path).expect("Could not dump packets!");
        store_path_str.clone()
    }
}
