use rand::Rng;

use crate::{
    feedback::{
        producers::SummaryProducerIdentifier,
        reward::{RewardEntry, SummaryTask},
    },
    history::History,
    nemesis::RewardRequestStatus,
};

use super::{
    ActionId, ConcreteSchedule, DiscreteStepScheduler, ScheduleHistory, SchedulerIdentifier,
    StepId, TimingManager,
};

pub struct NoopScheduler {
    my_id: SchedulerIdentifier,

    /// N = How many actions are there in the schedule?
    _nrows: usize,
    /// M = How many choices do we have at each step?
    ncols: usize,
}

impl NoopScheduler {
    pub fn new(schedule_length: usize, num_actions: usize) -> Self {
        let (nrows, ncols) = (schedule_length, num_actions);

        Self {
            my_id: SchedulerIdentifier::Noop,
            _nrows: nrows,
            ncols,
        }
    }

    /// Request all rewards for actions in the current schedule that
    /// we haven't requested for yet.
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
                let (step_start_ts, step_end_ts) = timing.step_interval(sched_timing, step);
                let task = SummaryTask::new(
                    self.my_id,
                    *schedule_id,
                    step,
                    None,
                    action,
                    step_start_ts,
                    step_end_ts,
                );
                log::info!("[Noop] Requesting reward for task: {:?}", task);
                history.request_reward(task);
                *status = RewardRequestStatus::Requested;
            }
        }
    }

    fn num_actions(&self) -> usize {
        self.ncols
    }
}

impl DiscreteStepScheduler for NoopScheduler {
    fn get_and_record_action_at_step(
        &self,
        our_history: &mut ScheduleHistory,
        step: StepId,
        timing: &TimingManager,
        history: &History,
    ) -> ActionId {
        let mut rng = rand::thread_rng();
        let action = rng.gen_range(0..self.num_actions());

        let current_schedule = &mut our_history.current_schedule;
        // Add the action to the current schedule.
        current_schedule.add_action(step, action);
        // Request the reward for the action.
        self.request_rewards(current_schedule, timing, history);
        log::info!(
            "[Noop] Select action {} at step {} for schedule {}",
            action,
            step,
            current_schedule.schedule_id
        );
        return action;
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
            "[Noop] Report reward: {:?}, {:?}",
            schedule_id,
            reward_entry,
        );
    }

    fn notify_schedule_start(&self, _schedule_id: usize) {
        // Nothing to do. The matrix remains unchanged.
    }

    fn notify_schedule_end(&self, _past_schedule: &ConcreteSchedule, _history: &History) {
        // Nothing to do.
    }

    fn execution_ended(&self) {
        // Nothing to do.
    }
}
