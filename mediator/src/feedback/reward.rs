use std::fmt;
use std::hash::Hash;

use hashbrown::HashMap;

use crate::history::time::AbsoluteTimestamp;
use crate::nemesis::schedules::{ActionId, ScheduleId, SchedulerIdentifier, StateId, StepId};

use super::producers::SummaryProducerIdentifier;

// History summaries
pub type RewardValue = i32;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SummaryTask {
    // The order of the fields is important, since it defines
    // the comparison order: we want to sort Tasks first by end_ts.
    pub end_ts: AbsoluteTimestamp,
    pub start_ts: AbsoluteTimestamp,

    pub scheduler: SchedulerIdentifier,
    pub schedule_id: ScheduleId,
    pub step_id: StepId,

    /// The "State" for Q-Learning.
    pub state_id: Option<StateId>,
    /// The "Action" that was taken, also used for Q-Learning.
    pub action_id: ActionId,
}

impl fmt::Display for SummaryTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Task {{schedule: {}, step: {}, action: {}, start: {}, end: {}}}",
            self.schedule_id, self.step_id, self.action_id, self.start_ts, self.end_ts
        )
    }
}

impl SummaryTask {
    pub fn new(
        scheduler: SchedulerIdentifier,
        schedule_id: ScheduleId,
        step_id: StepId,
        state_id: Option<StateId>,
        action_id: ActionId,
        start_ts: AbsoluteTimestamp,
        end_ts: AbsoluteTimestamp,
    ) -> Self {
        Self {
            scheduler,
            schedule_id,
            step_id,
            state_id,
            action_id,
            start_ts,
            end_ts,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RewardEntry {
    pub producer: SummaryProducerIdentifier,
    pub scheduler: SchedulerIdentifier,
    pub schedule_id: ScheduleId,
    pub step_id: StepId,

    pub pre_state: Option<StateId>,
    pub action_id: ActionId,
    pub post_state: StateId,

    /// Reward for the action you just took at this step, e.g.
    /// "did we _just_ see incentivised behaviour"? This is the reward
    /// for the _edge_ from the previous to the current state.
    pub reward_for_step: RewardValue,
}

impl fmt::Display for RewardEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Reward {{producer: {:?}, schedule: {}, step: {}, action: {}, reward: {}}}",
            self.producer, self.schedule_id, self.step_id, self.action_id, self.reward_for_step
        )
    }
}

impl RewardEntry {
    pub fn new_with_post_state(
        producer: SummaryProducerIdentifier,
        task: SummaryTask,
        post_state: StateId,
        reward_for_step: RewardValue,
    ) -> Self {
        Self {
            producer,
            scheduler: task.scheduler,
            schedule_id: task.schedule_id,
            step_id: task.step_id,

            pre_state: task.state_id,
            action_id: task.action_id,
            post_state,

            reward_for_step,
        }
    }

    pub fn new(
        producer: SummaryProducerIdentifier,
        task: SummaryTask,
        reward_for_step: RewardValue,
    ) -> Self {
        Self::new_with_post_state(producer, task, StateId::default(), reward_for_step)
    }
}

pub trait RewardFunction
where
    Self: std::marker::Sized,
{
    /// See: https://openai.com/blog/faulty-reward-functions/
    fn reward_function(
        task: SummaryTask,
        _overall_cumulative: &Self,
        _schedule_cumulative: &Self,
        _schedule_window_summaries: &HashMap<StepId, Self>,
        _current: &mut Self,
        _state_similarity_threshold: f64,
    ) -> RewardEntry {
        RewardEntry::new(SummaryProducerIdentifier::default(), task, 0)
    }
}
