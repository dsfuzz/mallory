pub mod agent;
mod policies;
mod states;

use nalgebra::DVector;

pub use super::{ActionId, StateId, StepId};
use crate::nemesis::RewardEntry;

type QValue = f64;
/// Vector of Q-values for a given state.
type QVector = DVector<QValue>;

/// Policy for choosing actions.
trait ActionPolicy: Send + Sync {
    fn choose_action(&self, q_row: &QVector) -> ActionId;
}

/// Manages the "state" the agent is in.
trait StateManager: Send + Sync {
    /// Returns None if the state is not yet known.
    /// This should trigger the agent to return :pending and ask for a reward
    /// such that the state we're in can be determined.
    fn get_current_state_id(&self) -> Option<StateId>;

    /// Advance the state to the next step.
    fn notify_action_taken(&mut self, action: ActionId, current_step: StepId);

    /// Notify the state manager that a reward has been received.
    /// Returns the (pre_state, action_id, post_state) triplet that should
    /// be used to update the Q-value matrix.
    fn notify_reward_received(
        &mut self,
        reward: &RewardEntry,
    ) -> (Option<StateId>, ActionId, StateId);
}

/// Implemented by summaries which can be used as states.
pub trait StateDescriptor {
    fn get_state_id(&mut self, state_similarity_threshold: f64) -> StateId;
}
