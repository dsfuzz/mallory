pub mod noop;
pub mod power;
pub mod qlearning;

use std::{
    fmt,
    sync::atomic::{AtomicUsize, Ordering},
};

use antidote::RwLock;
use atomic_enum::atomic_enum;
use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;
use hashbrown::HashMap;

use crate::{
    feedback::{producers::SummaryProducerIdentifier, reward::RewardEntry},
    history::{
        time::{AbsoluteTimestamp, ClockManager},
        History,
    },
};

use self::{noop::NoopScheduler, power::PowerScheduler, qlearning::agent::QLearningScheduler};

use super::{AdaptiveNemesis, RewardRequestStatus};

pub type ScheduleId = usize;
pub type StepId = usize;
pub type StateId = usize;
pub type ActionId = usize;
pub const DEFAULT_ACTION_ID: ActionId = 0;
pub const DEFAULT_STATE_ID: StateId = 0;
pub const END_SCHEDULE_STEP_ID: StepId = StepId::MAX;

pub enum ScheduleOpStatus {
    New,
    AlreadyReturned,
    InResetPeriod,
}

/*******************************************************
 * Timing manager. Splits "runs"/schedules into steps. *
 *******************************************************/
#[derive(Debug, Default, Copy, Clone)]
pub struct ScheduleTiming {
    pub start_of_reset_period: Option<DateTime<Utc>>,
    pub start_of_schedule: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub struct TimingManager {
    schedule_duration_ms: u64,
    schedule_interval_ms: u64,
    reset_duration_ms: u64,

    current_schedule: RwLock<ScheduleTiming>,
}

impl TimingManager {
    pub fn new(
        schedule_duration_ms: u64,
        schedule_interval_ms: u64,
        reset_duration_ms: u64,
    ) -> TimingManager {
        TimingManager {
            schedule_duration_ms,
            schedule_interval_ms,
            reset_duration_ms,
            current_schedule: RwLock::new(ScheduleTiming::default()),
        }
    }

    fn start_new_schedule(&self) -> ScheduleTiming {
        let mut current_schedule = self.current_schedule.write();
        let now = ClockManager::utc_now();
        let schedule_start = now + Duration::milliseconds(self.reset_duration_ms as i64);
        current_schedule.start_of_reset_period = Some(now);
        current_schedule.start_of_schedule = Some(schedule_start);
        current_schedule.end_time = None;
        *current_schedule
    }

    /// Get the current step.
    fn current_step(&self) -> (StepId, AbsoluteTimestamp, AbsoluteTimestamp) {
        let now = ClockManager::utc_now();
        let schedule_start = self
            .current_schedule
            .read()
            .start_of_reset_period
            .unwrap_or(now);

        // Compute the StepId
        let elapsed = now.signed_duration_since(schedule_start);
        let elapsed_ms = elapsed.num_milliseconds() as u64;
        let step = elapsed_ms / self.schedule_interval_ms;

        // Compute the interval (nanosecond precision)
        let step_duration_ns = Duration::milliseconds(self.schedule_interval_ms as i64)
            .num_nanoseconds()
            .unwrap();
        let step_start = schedule_start.timestamp_nanos() + (step as i64 * step_duration_ns);
        let step_end = step_start + step_duration_ns;

        (step as StepId, step_start, step_end)
    }

    /// Get the interval for the given step.
    pub fn step_interval(
        &self,
        timing: &ScheduleTiming,
        step: StepId,
    ) -> (AbsoluteTimestamp, AbsoluteTimestamp) {
        let schedule_start = timing.start_of_reset_period.unwrap();

        // Compute the interval (nanosecond precision)
        let step_duration_ns = Duration::milliseconds(self.schedule_interval_ms as i64)
            .num_nanoseconds()
            .unwrap();
        let step_start = schedule_start.timestamp_nanos() + (step as i64 * step_duration_ns);
        let step_end = step_start + step_duration_ns;

        (step_start, step_end)
    }

    /// Are we in the "reset" period between schedules?
    /// If the schedule hasn't started, the answer is NO.
    pub fn in_reset_time(&self) -> bool {
        let now = ClockManager::utc_now();
        let schedule_start = match self.current_schedule.read().start_of_reset_period {
            Some(t) => t,
            None => return false,
        };

        let elapsed = now.signed_duration_since(schedule_start);
        let elapsed_ms = elapsed.num_milliseconds() as u64;

        let total_period_ms = self.schedule_duration_ms + self.reset_duration_ms;
        if elapsed_ms > self.schedule_duration_ms && elapsed_ms <= total_period_ms {
            return true;
        }
        false
    }

    fn schedule_length(&self) -> usize {
        (self.schedule_duration_ms / self.schedule_interval_ms) as usize
    }

    fn has_started(&self) -> bool {
        self.current_schedule.read().start_of_reset_period.is_some()
    }

    /// Are we in the "schedule" period?
    /// If the schedule hasn't started, the answer is NO.
    pub fn in_schedule_time(&self) -> bool {
        self.has_started() && (self.current_step().0 < self.schedule_length())
    }
}

/***************************************
 * Actual schedule-related definitions *
 ***************************************/

#[derive(Debug, Clone)]
pub struct ConcreteSchedule {
    produced_by: SchedulerIdentifier,
    schedule_id: usize,
    timing: ScheduleTiming,

    next_step_to_add: StepId,

    // A complete schedule should have N entries.
    /// Which actions did we exercise in this schedule?
    sched: Vec<ActionId>,
    /// Which states did we observe in this schedule?
    states: Vec<Option<StateId>>,
    /// Which rewards did we receive in this schedule?
    rewards: Vec<RewardRequestStatus>,
}

impl fmt::Display for ConcreteSchedule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} Schedule {}: [", self.produced_by, self.schedule_id)?;
        for (i, action_id) in self.sched.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", action_id)?;
        }
        write!(f, "]")
    }
}

impl ConcreteSchedule {
    /// Create an uninitialized schedule.
    fn new_unitialized() -> Self {
        let now = ClockManager::utc_now();
        Self {
            produced_by: SchedulerIdentifier::default(),
            schedule_id: 0,
            timing: ScheduleTiming {
                start_of_reset_period: Some(now),
                start_of_schedule: Some(now),
                end_time: None,
            },
            next_step_to_add: 0,
            sched: Vec::new(),
            states: Vec::new(),
            rewards: Vec::new(),
        }
    }

    /// Create and start a new schedule.
    fn start_new(
        scheduler: SchedulerIdentifier,
        schedule_id: usize,
        timing: ScheduleTiming,
    ) -> Self {
        Self {
            produced_by: scheduler,
            schedule_id,
            timing,
            next_step_to_add: 0,
            sched: Vec::new(),
            states: Vec::new(),
            rewards: Vec::new(),
        }
    }

    fn add_action(&mut self, step: usize, action_id: ActionId) {
        assert!(
            step >= self.next_step_to_add,
            "ConcreteSchedule: tried adding action at step < next_step_to_add."
        );
        self.sched.resize(step + 1, DEFAULT_ACTION_ID);
        self.sched[step] = action_id;
    }

    fn add_state(&mut self, step: usize, state_id: Option<StateId>) {
        assert!(
            step >= self.next_step_to_add,
            "ConcreteSchedule: tried adding state at step < next_step_to_add."
        );
        self.states.resize(step + 1, None);
        self.states[step] = state_id;
    }

    fn has_started(&self) -> bool {
        self.timing.start_of_reset_period.is_some()
    }

    fn start(&mut self) {
        self.timing.start_of_reset_period = Some(ClockManager::utc_now());
    }

    fn end(&mut self) -> DateTime<Utc> {
        let time = ClockManager::utc_now();
        self.timing.end_time = Some(time);
        time
    }

    fn get_recorded_action_at_step(&self, step: StepId) -> Option<ActionId> {
        self.sched.get(step as usize).copied()
    }
}

/*********************************************************************************
 * Helper for schedulers that divide time into "steps" and store past schedules. *
 *********************************************************************************/

pub struct ScheduleHistory {
    // The current schedule.
    current_schedule: ConcreteSchedule,
    // The last N schedules.
    past_schedules: HashMap<ScheduleId, ConcreteSchedule>,
}

impl ScheduleHistory {
    fn new() -> Self {
        Self {
            current_schedule: ConcreteSchedule::new_unitialized(),
            past_schedules: HashMap::new(),
        }
    }

    /// Record the reward in the appropriate schedule.
    fn record_reward(&mut self, reward: &RewardEntry) {
        let schedule_id = reward.schedule_id;

        // Identify which schedule (current, or a past one)
        let schedule = match self.past_schedules.get_mut(&schedule_id) {
            Some(s) => s,
            None if self.current_schedule.schedule_id == schedule_id => &mut self.current_schedule,
            None => {
                log::warn!(
                    "[REWARD] Received reward {:?} for unknown schedule ({:?}, {})",
                    reward,
                    reward.scheduler,
                    schedule_id
                );
                return;
            }
        };

        // Record the reward.
        let step = reward.step_id;
        let new_len = step + 1;
        if new_len > schedule.rewards.len() {
            schedule
                .rewards
                .resize(new_len, RewardRequestStatus::NotRequested);
        }
        schedule.rewards[step] = RewardRequestStatus::Received(reward.reward_for_step);

        log::info!(
            "[REWARD] Recorded reward {:?} for schedule ({:?}, {})",
            reward,
            reward.scheduler,
            schedule_id
        );
    }
}

impl Default for ScheduleHistory {
    fn default() -> Self {
        Self::new()
    }
}

/// A schedule manager keeps histories for different schedulers, and provides
/// a unified view of time and "time steps" for all schedulers.
pub struct ScheduleManager {
    histories: DashMap<SchedulerIdentifier, ScheduleHistory>,
    schedulers: DashMap<SchedulerIdentifier, Box<dyn DiscreteStepScheduler>>,

    /// The number of schedules that have been produced by all schedulers.
    /// We use this to assign unique IDs to schedules.
    num_schedules: AtomicUsize,

    /// WARNING: should only be changed at the beginning of a new schedule.
    active_scheduler: AtomicSchedulerIdentifier,
    timing: TimingManager,
}

impl ScheduleManager {
    pub fn new(
        schedule_duration_ms: u64,
        schedule_interval_ms: u64,
        reset_duration_ms: u64,
        num_actions: usize,
        schedule_type: &String,
    ) -> Self {
        let timing = TimingManager::new(
            schedule_duration_ms,
            schedule_interval_ms,
            reset_duration_ms,
        );

        // TODO: add suport for multiple schedulers
        let dlearning: Box<dyn DiscreteStepScheduler> = Box::new(QLearningScheduler::new(
            timing.schedule_length(),
            num_actions,
        ));

        let power: Box<dyn DiscreteStepScheduler> =
            Box::new(PowerScheduler::new(timing.schedule_length(), num_actions));

        let noop: Box<dyn DiscreteStepScheduler> =
            Box::new(NoopScheduler::new(timing.schedule_length(), num_actions));

        let schedulers = DashMap::new();
        schedulers.insert(SchedulerIdentifier::QLearning, dlearning);
        schedulers.insert(SchedulerIdentifier::Power, power);
        schedulers.insert(SchedulerIdentifier::Noop, noop);

        let active_scheduler = if schedule_type.as_str() == "qlearning" {
            AtomicSchedulerIdentifier::new(SchedulerIdentifier::QLearning)
        } else if schedule_type.as_str() == "power" {
            AtomicSchedulerIdentifier::new(SchedulerIdentifier::Power)
        } else {
            AtomicSchedulerIdentifier::new(SchedulerIdentifier::Noop)
        };

        Self {
            histories: DashMap::new(),
            schedulers,
            num_schedules: AtomicUsize::new(0),
            active_scheduler,
            timing,
        }
    }

    pub fn report_reward(
        &self,
        reward: &RewardEntry,
        summary_producers: &Vec<SummaryProducerIdentifier>,
    ) {
        let scheduler = reward.scheduler;
        // Record the reward in the history;
        if let Some(mut h) = self.histories.get_mut(&scheduler) {
            h.record_reward(reward)
        }

        // Let the scheduler know about the reward.
        if let Some(s) = self.schedulers.get(&scheduler) {
            s.report_reward(reward, summary_producers)
        }
    }

    pub fn get_next_op(
        &self,
        nemesis: &dyn AdaptiveNemesis,
        history: &History,
    ) -> (StepId, ActionId, ScheduleOpStatus) {
        // If we're in the reset period, we don't return any operations.
        // The nemesis itself is responsible for sending the "reset" ops.
        if self.in_reset_time() {
            return (0, DEFAULT_ACTION_ID, ScheduleOpStatus::InResetPeriod);
        }

        let active_scheduler = self.active_scheduler.load(Ordering::Relaxed);

        // Get the active scheduler's history.
        let mut our_history = self.histories.entry(active_scheduler).or_default();
        let our_history = our_history.value_mut();

        // Start a new schedule once we're done with the previous one and
        // have gone through the reset period as well.
        if !self.in_schedule_time() {
            self.start_new_schedule(our_history, Some(nemesis), history);
        }

        let current_schedule = &mut our_history.current_schedule;

        // If we are getting the first op in a schedule, record the start time.
        if !current_schedule.has_started() {
            current_schedule.start();
        }
        let (step, _step_start_ts, _step_end_ts) = self.timing.current_step();

        // Return :pending and don't record if we've been called on the same step twice.
        if let Some(action) = current_schedule.get_recorded_action_at_step(step) {
            return (step, action, ScheduleOpStatus::AlreadyReturned);
        };

        // Get the next action from the active scheduler.
        // (And record it in the current schedule.)
        let action = self
            .schedulers
            .get(&active_scheduler)
            .expect("Active scheduler not found in scheduler map!")
            .get_and_record_action_at_step(our_history, step, &self.timing, history);

        (step, action, ScheduleOpStatus::New)
    }

    fn num_current_schedule(&self) -> usize {
        self.num_schedules.load(Ordering::Relaxed)
    }

    fn num_completed_schedules(&self) -> usize {
        let num_sched = self.num_current_schedule();
        if num_sched == 0 {
            0
        } else {
            num_sched - 1
        }
    }

    fn new_schedule_id(&self) -> usize {
        self.num_schedules.fetch_add(1, Ordering::Relaxed)
    }

    fn start_new_schedule(
        &self,
        our_history: &mut ScheduleHistory,
        nemesis: Option<&dyn AdaptiveNemesis>,
        history: &History,
    ) {
        let active_id = self.active_scheduler.load(Ordering::Relaxed);

        // Make a copy of the schedule to store.
        let mut past_schedule = our_history.current_schedule.clone();
        let past_start_time = past_schedule
            .timing
            .start_of_reset_period
            .expect("Schedule ended, but has no start time!");
        let past_end_time = past_schedule.end();

        // If the schedule is not empty, store it.
        if past_schedule.timing.start_of_reset_period.is_some() && !past_schedule.sched.is_empty() {
            our_history
                .past_schedules
                .insert(past_schedule.schedule_id, past_schedule.clone());

            self.schedulers
                .get(&active_id)
                .expect("Active scheduler not found in scheduler map!")
                .notify_schedule_end(&past_schedule, history);
        }

        // Let the TimingManager know that we're starting a new schedule.
        let new_timing = self.timing.start_new_schedule();
        // Add metadata to the new schedule.
        let new_schedule =
            ConcreteSchedule::start_new(active_id, self.new_schedule_id(), new_timing);

        let start_time = new_schedule
            .timing
            .start_of_schedule
            .expect("Schedule has no start time!");
        let schedule_id = new_schedule.schedule_id;

        // Make this the current schedule.
        our_history.current_schedule = new_schedule;

        // Notify that we're starting a new schedule.
        if let Some(nemesis) = nemesis {
            nemesis.notify_schedule_start(
                past_schedule.schedule_id,
                past_start_time,
                past_end_time,
                start_time,
            );
        }
        // Notify the active scheduler that we're starting a new schedule.
        self.schedulers
            .get(&active_id)
            .expect("Active scheduler not found in scheduler map!")
            .notify_schedule_start(schedule_id);

        // TODO: handle changes in scheduler.

        // Log the end of the previous schedule.
        if past_schedule.timing.start_of_reset_period.is_some() {
            let start = past_schedule.timing.start_of_reset_period.unwrap();
            let end = past_schedule.timing.end_time.unwrap();
            let duration_ms = (end - start).num_milliseconds();
            log::info!(
                "[SCHEDULE] Finished executing schedule (from {} to {} / span {} ms): {}",
                start,
                end,
                duration_ms,
                past_schedule,
            );
        }
    }

    fn in_schedule_time(&self) -> bool {
        self.timing.in_schedule_time()
    }

    pub fn in_reset_time(&self) -> bool {
        self.timing.in_reset_time()
    }

    pub fn print_statistics(&self) {
        let active_scheduler = self.active_scheduler.load(Ordering::Relaxed);
        let our_history = self.histories.try_entry(active_scheduler);

        if let Some(our_history) = our_history {
            let our_history = our_history.or_default();

            log::info!(
                "[STATS][NEMESIS] Completed {} schedules. At step {}/{} in schedule {}: {}",
                self.num_completed_schedules(),
                self.timing.current_step().0,
                self.timing.schedule_length(),
                self.num_current_schedule(),
                our_history.current_schedule,
            );
        }
    }
}

/***********************
 * Scheduler interface *
 ***********************/

#[atomic_enum]
#[derive(PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub enum SchedulerIdentifier {
    #[default]
    Noop,
    QLearning,
    Power,
}

pub trait DiscreteStepScheduler: Send + Sync {
    fn get_and_record_action_at_step(
        &self,
        our_history: &mut ScheduleHistory,
        step: StepId,

        // Pass in case the scheduler needs to know when the step is happening,
        // e.g. to know which portion of the history to inspect or request
        // rewards for.
        timing: &TimingManager,

        // Pass in case the scheduler needs to look at the history to decide
        // what to do or to request post-hoc feedback for its action.
        history: &History,
    ) -> ActionId;

    /// Note that this can be called multiple times for the same
    /// (schedule_id, step_id, action_id) triplet, with the intention
    /// that the calls are additive (from different kinds of feedback).
    fn report_reward(
        &self,
        reward: &RewardEntry,
        summary_producers: &Vec<SummaryProducerIdentifier>,
    );

    fn notify_schedule_start(&self, schedule_id: usize);

    fn notify_schedule_end(&self, past_schedule: &ConcreteSchedule, history: &History);

    fn execution_ended(&self);
}
