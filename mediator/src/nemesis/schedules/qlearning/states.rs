use crate::feedback::reward::RewardEntry;

use super::{ActionId, StateId, StateManager, StepId};

/// The current "state" is simply how far along in the schedule
/// we are. The attempt here is to simply learn "good" nemesis
/// schedules directly, but it's a kind of straw-man approach
/// because "states" are not really identifying what the system
/// is doing -- this doesn't satisfy the Markov assumption.
pub struct ScheduleStepState {
    current_step: StepId,

    _schedule_length: usize,
}

impl ScheduleStepState {
    pub fn new(schedule_length: usize) -> Self {
        Self {
            current_step: 0,
            _schedule_length: schedule_length,
        }
    }
}

impl StateManager for ScheduleStepState {
    fn get_current_state_id(&self) -> Option<StateId> {
        Some(self.current_step as StateId)
    }

    fn notify_action_taken(&mut self, _action: ActionId, current_step: StepId) {
        self.current_step = current_step;
    }

    /// The post_state is the next step in the schedule.
    fn notify_reward_received(
        &mut self,
        reward: &RewardEntry,
    ) -> (Option<StateId>, ActionId, StateId) {
        (Some(reward.step_id), reward.action_id, reward.step_id + 1)
    }
}

/// A more principled state manager, that takes as its current state
/// the StateId of the latest summary (RewardEntry) received.
/// This lets the Q-learning agent react to what the system is actually
/// doing, albeit with a delay.
pub struct SummaryState {
    /// We know our state once we've received a summary, and
    /// we no longer know it once we've performed an action.
    current_state: Option<StateId>,
}

impl SummaryState {
    pub fn new() -> Self {
        Self {
            current_state: None,
        }
    }
}

impl StateManager for SummaryState {
    fn get_current_state_id(&self) -> Option<StateId> {
        log::info!("[STATE] Current state is {:?}", self.current_state);
        self.current_state
    }

    fn notify_action_taken(&mut self, action: ActionId, _current_step: StepId) {
        self.current_state = None;
        log::info!(
            "[STATE] At step {}, performed action {} => state is now {:?}",
            _current_step,
            action,
            self.current_state
        );
    }

    /// The post_state is the next step in the schedule.
    fn notify_reward_received(
        &mut self,
        reward: &RewardEntry,
    ) -> (Option<StateId>, ActionId, StateId) {
        let before_state = self.current_state;
        // We set our current_state to the post_state of the summary we just received.
        self.current_state = Some(reward.post_state);
        log::info!(
            "[STATE] In state {:?}, received reward {:?} => state is now {:?}",
            before_state,
            reward,
            self.current_state
        );

        // And report the identifiers of the summary we just received.
        (reward.pre_state, reward.action_id, reward.post_state)
    }
}
