use antidote::RwLock;
use hashbrown::HashMap;
use rand::Rng;
use std::cmp;
use std::fmt;
use std::sync::atomic::{AtomicI32, AtomicU16, Ordering};

use crate::{
    feedback::{
        producers::SummaryProducerIdentifier,
        reward::{RewardEntry, RewardValue, SummaryTask},
    },
    history::{common::OrderedSet, History},
    nemesis::RewardRequestStatus,
};

use super::ScheduleHistory;
use super::TimingManager;
use super::{
    ActionId, ConcreteSchedule, DiscreteStepScheduler, ScheduleId, SchedulerIdentifier, StepId,
};

const MAX_SEED_ENTRIES: u16 = 1000;
const DEFAULT_SEED_POWER: i32 = 10;
const MIN_MUTATED_SEED_NUM: u16 = 10;

pub struct PowerScheduler {
    my_id: SchedulerIdentifier,

    seed_queue: OrderedSet<SeedEntry>,
    seed_num: AtomicU16,

    // The currecnt schedule to execute
    current_schedule_to_execute: RwLock<Vec<ActionId>>,

    /// Cached schedule to mutate.
    cached_schedule_to_mutate: RwLock<Vec<ActionId>>,
    left_times_to_mutate: AtomicI32,

    /// Schedules waiting for total rewards
    schedules_waiting_rewards: RwLock<HashMap<ScheduleId, RewardValue>>,

    /// Default configutation
    schedule_length: usize,
    num_actions: usize,
}

impl PowerScheduler {
    pub fn new(schedule_length: usize, num_actions: usize) -> Self {
        let current_schedule_to_execute = RwLock::new(vec![]);
        let cached_schedule_to_mutate = RwLock::new(vec![]);
        let schedule_length = schedule_length;
        let num_actions = num_actions;
        Self {
            my_id: SchedulerIdentifier::Power,
            seed_queue: OrderedSet::new(),
            seed_num: AtomicU16::new(0),
            current_schedule_to_execute,
            cached_schedule_to_mutate,
            left_times_to_mutate: AtomicI32::new(0),
            schedules_waiting_rewards: RwLock::new(HashMap::new()),
            schedule_length,
            num_actions,
        }
    }

    fn add_seed_entry(&self, schedule_id: ScheduleId, total_reward: i32) {
        let guard = &crossbeam_epoch::pin();
        if self.seed_num.load(Ordering::Relaxed) < MAX_SEED_ENTRIES {
            // Case 1: if the seed queue is not full (< MAX_SEED_ENTRIES)
            // we just insert the new entry
            let entry = SeedEntry {
                total_reward,
                schedule_id,
            };

            self.seed_queue.insert(entry, (), guard);
            self.seed_num.fetch_add(1, Ordering::Relaxed);
        } else {
            // Case 2: if the seed queue is full, we check if the reward of
            // the new entry is greater than the reward of the last entry;
            // if so, we remove the last entry and insert the new entry
            if let Some((seed_entry, _)) = self.seed_queue.pop_last(guard) {
                if total_reward > seed_entry.total_reward {
                    self.seed_queue.insert(
                        SeedEntry {
                            total_reward,
                            schedule_id,
                        },
                        (),
                        guard,
                    );
                } else {
                    self.seed_queue.insert(seed_entry, (), guard);
                }
            }
        }
    }

    /// Mutate or create a new schedule
    fn produce_schedule(&self, old_sched: Vec<ActionId>) -> Vec<ActionId> {
        if !old_sched.is_empty() {
            // mutate the old schedule
            let mut new_sched = old_sched;
            let mut rng = rand::thread_rng();
            // randomly select a position to mutate
            let pos = rng.gen_range(0..new_sched.len());

            // randomly select one operation to mutate
            // deletion: 0, insertion: 1, replacement: 2
            let op = rng.gen_range(0..3);
            match op {
                0 => {
                    // deletion
                    new_sched.remove(pos);
                }
                1 => {
                    // insertion
                    let action_id = rng.gen_range(0..self.num_actions);
                    new_sched.insert(pos, action_id);
                }
                2 => {
                    // replacement
                    let action_id = rng.gen_range(0..self.num_actions);
                    new_sched[pos] = action_id;
                }
                _ => unreachable!(),
            }

            // adjust smaller or larger schedule length
            match new_sched.len().cmp(&self.schedule_length) {
                cmp::Ordering::Less => {
                    // add new actions to the end of the schedule
                    let mut rng = rand::thread_rng();
                    for _ in 0..(self.schedule_length - new_sched.len()) {
                        let action_id = rng.gen_range(0..self.num_actions);
                        new_sched.push(action_id);
                    }
                }
                cmp::Ordering::Greater => {
                    // remove actions from the end of the schedule
                    new_sched.truncate(self.schedule_length);
                }
                cmp::Ordering::Equal => {}
            }

            new_sched
        } else {
            // randomly produce a new schedule
            let mut new_sched = vec![0; self.schedule_length];
            for item in new_sched.iter_mut().take(self.schedule_length) {
                let mut rng = rand::thread_rng();
                let action_id = rng.gen_range(0..self.num_actions);
                *item = action_id;
            }
            new_sched
        }
    }

    /// Select the next seed to mutate.
    /// We select the seed from the queue after some initial runs (e.g., 10 runs)
    fn select_next_schedule(
        &self,
        past_schedules: &HashMap<ScheduleId, ConcreteSchedule>,
    ) -> Vec<ActionId> {
        if self.seed_num.load(Ordering::Relaxed) < MIN_MUTATED_SEED_NUM {
            // Case 1: less than MIN_MUTATED_SEED_NUM (i.e., 10) seeds,
            // return one random schedule
            let new_sched = self.produce_schedule(vec![]);
            log::info!("[PowerScheduler] Select a random schedule: {:?}", new_sched);
            new_sched
        } else if self.left_times_to_mutate.load(Ordering::Relaxed) > 0 {
            // Case 2: less than the power scheduling, mutate the same base
            // schedule and return the mutated schedule
            self.left_times_to_mutate.fetch_sub(1, Ordering::Relaxed);
            let cached_schedule = self.cached_schedule_to_mutate.read();
            let base_schedule = &*cached_schedule;
            let base_schedule = base_schedule.clone();
            let new_sched = self.produce_schedule(base_schedule);
            log::info!(
                "[PowerScheduler] Select the same schedule to mutate: {:?}",
                new_sched
            );
            new_sched
        } else {
            // Case 3: select one seed from the queue to mutate

            let guard = &crossbeam_epoch::pin();
            // get the schedule from the seed queue
            if let Some((seed_entry, _)) = self.seed_queue.pop_first(guard) {
                let schedule_id = seed_entry.schedule_id;

                // remove the schedule from the past_schedules
                self.seed_num.fetch_sub(1, Ordering::Relaxed);

                let schedule = past_schedules.get(&schedule_id);
                // update the cached schedule
                let mut cached_schedule = self.cached_schedule_to_mutate.write();

                let new_schedule = match schedule {
                    Some(s) => {
                        *cached_schedule = s.sched.clone();
                        self.left_times_to_mutate
                            .store(seed_entry.total_reward - 1, Ordering::Relaxed);
                        self.produce_schedule(s.sched.clone())
                    }
                    None => {
                        // should reach here only when the schedule is removed from the past schedules
                        self.left_times_to_mutate
                            .store(DEFAULT_SEED_POWER, Ordering::Relaxed);
                        let next_schedule = self.produce_schedule(vec![]);
                        *cached_schedule = next_schedule.clone();
                        next_schedule
                    }
                };

                log::info!(
                    "[PowerScheduler] Select a schedule from the queue: {:?}",
                    new_schedule
                );
                new_schedule
            } else {
                // should reach here only when the queue is empty
                log::info!("[PowerScheduler] Select a random schedule because the queue is empty");
                let next_schedule = self.produce_schedule(vec![]);
                self.left_times_to_mutate
                    .store(DEFAULT_SEED_POWER, Ordering::Relaxed);
                let mut cached_schedule = self.cached_schedule_to_mutate.write();
                *cached_schedule = next_schedule.clone();
                next_schedule
            }
        }
    }

    /// Request all rewards for actions in the current schedule that we have not requested yet.
    /// For the power scheduling, we also request one reward for each step to facilitate the
    /// event triplets calculation.
    fn request_rewards(
        &self,
        current_schedule: &mut ConcreteSchedule,
        timing: &TimingManager,
        history: &History,
    ) {
        assert!(
            current_schedule.rewards.len() <= current_schedule.sched.len(),
            "ConcreteSchedule: len(rewards) > len(sched)",
        );

        let new_len = current_schedule.sched.len();

        // Request rewards for all actions in the current schedule.
        if new_len > current_schedule.rewards.len() {
            current_schedule
                .rewards
                .resize(new_len, RewardRequestStatus::NotRequested);
        }

        let ConcreteSchedule {
            schedule_id,
            rewards,
            sched,
            timing: sched_timing,
            ..
        } = current_schedule;

        // Request rewards for all (unclaimed) actions in the current schedule.
        for (step, status) in rewards.iter_mut().enumerate() {
            if *status == RewardRequestStatus::NotRequested {
                let action = sched[step];
                let (step_start_ts, step_end_ts) = timing.step_interval(&sched_timing, step);
                let task = SummaryTask::new(
                    self.my_id,
                    *schedule_id,
                    step,
                    None,
                    action,
                    step_start_ts,
                    step_end_ts,
                );
                log::info!("[PowerScheduler] Requesting reward for task: {:?}", task);
                history.request_reward(task);
                *status = RewardRequestStatus::Requested;
            }
        }
    }
}

impl DiscreteStepScheduler for PowerScheduler {
    fn get_and_record_action_at_step(
        &self,
        our_history: &mut ScheduleHistory,
        step: StepId,
        timing: &TimingManager,
        history: &History,
    ) -> ActionId {
        // When the current schedule is empty, it means that we are starting a new schedule.
        // Then we select the next schedule and update it into the currect schedule to execute.
        let current_schedule = &mut our_history.current_schedule;
        if current_schedule.sched.is_empty() {
            let current_schedule = self.select_next_schedule(&our_history.past_schedules);

            let mut current_schedule_to_execute = self.current_schedule_to_execute.write();
            *current_schedule_to_execute = current_schedule;
        }

        let current_schedule_to_execute = self.current_schedule_to_execute.read();
        let action = current_schedule_to_execute[step];
        current_schedule.add_action(step, action);
        self.request_rewards(current_schedule, timing, history);

        action
    }

    fn report_reward(
        &self,
        reward_entry: &RewardEntry,
        summary_producers: &Vec<SummaryProducerIdentifier>,
    ) {
        let producer = reward_entry.producer;

        // Only accept rewards from the summary producers.
        if !summary_producers.contains(&producer) {
            return;
        }

        let schedule_id = reward_entry.schedule_id;
        log::info!(
            "[PowerScheduler] Report reward: {:?}, {:?}",
            schedule_id,
            reward_entry,
        );

        let reward_for_step = reward_entry.reward_for_step;

        let mut schedules_waiting_rewards = self.schedules_waiting_rewards.write();

        // Get the reward value based on the schedule id
        let schedule_entry = schedules_waiting_rewards
            .entry(schedule_id)
            .or_insert_with(|| 0);

        // Update the reward value
        *schedule_entry += reward_for_step;

        // Collect schedules less than the current schedule id
        let ready_schedules: Vec<ScheduleId> = schedules_waiting_rewards
            .iter()
            .filter(|(id, _)| **id < schedule_id)
            .map(|(id, _)| *id)
            .collect();

        for ready_schedule in ready_schedules {
            // Remove the entry from the map
            let total_reward = schedules_waiting_rewards.remove(&ready_schedule).unwrap();
            // Add the entry to the seed queue
            self.add_seed_entry(ready_schedule, total_reward);

            log::info!(
                "[PowerScheduler] Schedule {} is ready, total reward: {}",
                ready_schedule,
                total_reward
            );
        }
    }

    fn notify_schedule_start(&self, _schedule_id: usize) {
        // Nothing to do.
    }

    fn notify_schedule_end(&self, _past_schedule: &ConcreteSchedule, _history: &History) {
        // Nothing to do.
    }

    fn execution_ended(&self) {
        // Nothing to do.
    }
}

#[derive(Clone, PartialEq, Eq, Debug, PartialOrd, Ord, Hash)]
struct SeedEntry {
    total_reward: RewardValue,
    schedule_id: ScheduleId,
}

impl fmt::Display for SeedEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SeedQueue {{ total_reward: {}, schedule_id: {} }}",
            self.total_reward, self.schedule_id
        )
    }
}
