use antidote::RwLock;

use hashbrown::HashMap;
use nalgebra::DVector;

use crate::{
    feedback::{
        producers::SummaryProducerIdentifier,
        reward::{RewardEntry, SummaryTask},
    },
    history::History,
    nemesis::{
        schedules::{
            ConcreteSchedule, DiscreteStepScheduler, ScheduleHistory, SchedulerIdentifier, StateId,
            TimingManager, DEFAULT_ACTION_ID,
        },
        RewardRequestStatus,
    },
};

use super::{
    policies::{SoftmaxPolicy, DEFAULT_TEMPERATURE},
    states::{ScheduleStepState, SummaryState},
    ActionId, ActionPolicy, StateManager, StepId,
};

const DEFAULT_LEARNING_RATE: f64 = 0.1;
const DEFAULT_DISCOUNT_FACTOR: f64 = 0.6;

type QValue = f64;
/// Vector of Q-values for a given state.
type QVector = DVector<QValue>;

/// N-armed bandit problem scheduler based on Q-learning.
/// The n "arms" are the nemesis actions we can perform. The run length is
/// the number of steps over which we are trying to maximise our reward.
/// I guess it's easier to learn a table over a shorter run length.
///
/// Q-learning is suited for learning a policy over a Markov decision process
/// (MDP). Recall the Markov assumption: all relevant information is encapsulated
/// in the current state.
///
/// See: https://www.cs.toronto.edu/~rgrosse/courses/csc421_2019/slides/lec21.pdf
pub struct QLearningScheduler {
    my_id: SchedulerIdentifier,

    /// How many steps are there in the schedule?
    /// We try to maximise our reward over this many steps.
    run_length: usize,
    /// How many choices do we have at each step?
    num_actions: usize,

    /// Estimates of the expected _value_ of actions at each state.
    q_table: RwLock<HashMap<StateId, QVector>>,

    /// How many times have we visited each state?
    hit_counts: RwLock<HashMap<StateId, usize>>,

    // Hyper-parameters
    /// Learning rate.
    alpha: f64,
    /// Discount factor.
    gamma: f64,

    /// Keeping track of which state we're in.
    state_manager: RwLock<Box<dyn StateManager>>,

    action_policy: Box<dyn ActionPolicy>,
}

impl QLearningScheduler {
    pub fn new(schedule_length: usize, num_actions: usize) -> Self {
        let _step_state: Box<dyn StateManager> = Box::new(ScheduleStepState::new(schedule_length));
        let summary_state: Box<dyn StateManager> = Box::new(SummaryState::new());
        let state_manager = RwLock::new(summary_state);

        let softmax = SoftmaxPolicy::new(DEFAULT_TEMPERATURE, num_actions);
        let action_policy = Box::new(softmax);

        // TODO: Seed the Q-table by sampling from the generator?
        Self {
            my_id: SchedulerIdentifier::QLearning,
            run_length: schedule_length,
            num_actions,
            q_table: RwLock::new(HashMap::new()),
            hit_counts: RwLock::new(HashMap::new()),
            alpha: DEFAULT_LEARNING_RATE,
            gamma: DEFAULT_DISCOUNT_FACTOR,
            state_manager,
            action_policy,
        }
    }

    /// Request all rewards for actions in the current schedule that
    /// we haven't requested for yet.
    fn request_rewards(
        &self,
        current_schedule: &mut ConcreteSchedule,
        current_state: Option<StateId>,
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
                    current_state,
                    action,
                    step_start_ts,
                    step_end_ts,
                );
                log::info!("[AGENT] Requesting reward for task: {:?}", task);
                history.request_reward(task);
                *status = RewardRequestStatus::Requested;
            }
        }
    }
}

impl DiscreteStepScheduler for QLearningScheduler {
    fn get_and_record_action_at_step(
        &self,
        our_history: &mut ScheduleHistory,
        step: StepId,
        timing: &TimingManager,
        history: &History,
    ) -> ActionId {
        let current_schedule = &mut our_history.current_schedule;
        let mut q_table = self.q_table.write();
        let mut state_manager = self.state_manager.write();
        let mut hit_counts = self.hit_counts.write();

        // Determine what action to perform
        let (action, current_state) = match state_manager.get_current_state_id() {
            Some(current_state) => {
                // Record that we've visited this state.
                let hc = hit_counts.entry(current_state).or_insert(0);
                *hc += 1;

                // Get the Q-values for the current state.
                let q_row = q_table
                    .entry(current_state)
                    .or_insert(QVector::zeros(self.num_actions));

                // Choose an action based on the Q-values and our policy.
                let action = self.action_policy.choose_action(q_row);
                log::info!(
                    "[ACTION STATE] Chose action {} at step {} (state {}) for schedule {}",
                    action,
                    step,
                    current_state,
                    current_schedule.schedule_id
                );
                (action, Some(current_state))
            }

            // If we don't know what state we're in, we do the default thing,
            // which is to do nothing, i.e. return :pending to Jepsen.
            None => (DEFAULT_ACTION_ID, None),
        };

        // Inform the state manager that we've taken an action.
        state_manager.notify_action_taken(action, step);

        // Add the action to the current schedule
        current_schedule.add_action(step, action);
        // Report the state we've seen.
        current_schedule.add_state(step, current_state);

        // Request the reward for the action.
        self.request_rewards(current_schedule, current_state, timing, history);

        action
    }

    fn report_reward(
        &self,
        reward_entry: &RewardEntry,
        summary_producers: &Vec<SummaryProducerIdentifier>,
    ) {
        // Only accept rewards from the summary producer.
        let producer = reward_entry.producer;
        if !summary_producers.contains(&producer) {
            return;
        }

        let schedule_id = reward_entry.schedule_id;
        log::info!(
            "[QLEARNING] Report reward: {:?}, {:?}",
            schedule_id,
            reward_entry,
        );

        let mut q_table = self.q_table.write();
        let mut state_manager = self.state_manager.write();

        let RewardEntry {
            // The summariser lets us know what it think the pre-state and post-state,
            // were, i.e. if the summarised window is taken as the "state". However, we
            // don't necessarily have to use this. The StateManager decides.
            reward_for_step,
            ..
        } = reward_entry;

        // If we know what the pre-state and post-state were, update the Q-table.
        // If we previously didn't know our state, now we know it (post_state),
        // so the next time we call this, we'll know the pre-state.
        if let (Some(pre_state), action_id, post_state) =
            state_manager.notify_reward_received(reward_entry)
        {
            let reward = *reward_for_step as f64;

            // new_value = (1 - alpha) * old_value + alpha * (reward + gamma * next_max)

            let post_q_row = q_table
                .entry(post_state)
                .or_insert(QVector::zeros(self.num_actions));
            let next_max = post_q_row.max();

            let pre_q_row = q_table
                .entry(pre_state)
                .or_insert(QVector::zeros(self.num_actions));
            let old_val = pre_q_row[action_id];

            let original_values = pre_q_row.iter().cloned().collect::<Vec<_>>();

            // Update the Q-value.
            let new_val =
                (1.0 - self.alpha) * old_val + self.alpha * (reward + self.gamma * next_max);
            pre_q_row[action_id] = new_val;

            let updated_values = pre_q_row.iter().cloned().collect::<Vec<_>>();

            log::info!(
            "[QLEARNING]\nRow {}: {:?}\nUpdated Q-value for state {} action {} with reward {}: {} -> {}\nRow {}: {:?}",
            pre_state,
            original_values,
            pre_state,
            action_id,
            reward,
            old_val,
            new_val,
            pre_state,
            updated_values,
        );
        }
    }

    fn notify_schedule_start(&self, _schedule_id: usize) {
        // Nothing to do. The matrix remains unchanged.
    }

    fn notify_schedule_end(&self, _past_schedule: &ConcreteSchedule, _history: &History) {
        // Print statistics.
        let q_table = self.q_table.read();
        let hit_counts = self.hit_counts.read();

        let mut most_hit = hit_counts.iter().map(|(a, b)| (*a, *b)).collect::<Vec<_>>();
        most_hit.sort_unstable_by(|(_, ac), (_, bc)| bc.cmp(ac));

        let unique_states = q_table.len();
        let total_visits = most_hit.iter().map(|(_, v)| *v).sum::<usize>();

        let first_n = 10;
        let top_n = most_hit.iter().take(first_n).cloned().collect::<Vec<_>>();

        log::info!(
            "[STATES] Q-Table has {} unique states (visited a total of {} times).\nThe {} most visited states: {:?}\nThis schedule's states: {:?}",
            unique_states,
            total_visits,
            first_n,
            top_n,
            _past_schedule.states,
        );
    }

    fn execution_ended(&self) {
        // Nothing to do.
    }
}
