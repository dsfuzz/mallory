pub mod interfaces;
pub mod schedules;

use chrono::{DateTime, Utc};

use crate::feedback::{
    producers::SummaryProducerIdentifier,
    reward::{RewardEntry, RewardValue},
};

use self::schedules::ScheduleId;

// TODO: Generalise this once we have a second interface (besides Jepsen).
// Right now it's probably too tightly linked to Jepsen, but it's still
// good to have a kind-of general structure in place, I think.
// Probably we want some type-safety here rather than just strings.

pub trait AdaptiveNemesis: Send + Sync {
    // We don't have new(). Instead, `main` constructs the appropriate
    // implementor of `AdaptiveNemesis` and then calls `initialize` on it.
    // Since we only have a single implementor right now, we integrate
    // `initialize` into `JepsenAdaptiveNemesis`'s constructor.

    // fn initialize(&mut self, setup_str: &str, history: Arc<History>, cfg: MediatorConfig);

    /// An operation is requested from us.
    fn op(&self, ctx_str: &str) -> String;

    /// We are informed that an operation _will be_ invoked or _has_ completed,
    /// so we can update our state in response.
    fn update(&self, ctx_str: &str, event_str: &str);

    /// We are informed that an operation _has been_ invoked.
    fn invoke(&self, event_str: &str);
    /// We are informed that an operation _has been_ completed.
    fn complete(&self, event_str: &str);

    /// Called by our "child" schedulers to inform they've started a new schedule.
    fn notify_schedule_start(
        &self,
        schedule_id: ScheduleId,
        past_start_time: DateTime<Utc>,
        past_end_time: DateTime<Utc>,
        new_start_after_reset_time: DateTime<Utc>,
    );

    fn report_reward(
        &self,
        reward_entry: &RewardEntry,
        summary_producers: &Vec<SummaryProducerIdentifier>,
    );
    fn print_statistics(&self);
}

#[derive(Debug, Copy, Clone, PartialEq, Default)]
enum RewardRequestStatus {
    #[default]
    NotRequested,
    Requested,
    Received(RewardValue),
}
