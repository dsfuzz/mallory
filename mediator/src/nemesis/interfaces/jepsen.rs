use std::{collections::BTreeMap, error::Error, fmt, sync::Arc};

use antidote::Mutex;
use chrono::{DateTime, Utc};
use edn_format::{Keyword, Value};

use crate::{
    feedback::{producers::SummaryProducerIdentifier, reward::RewardEntry},
    history::History,
    nemesis::{
        schedules::{ScheduleId, ScheduleManager, ScheduleOpStatus, DEFAULT_ACTION_ID},
        AdaptiveNemesis,
    },
    MediatorConfig,
};

pub struct JepsenAdaptiveNemesis {
    cfg: NemesisConfig,

    /// Handles timing of the schedule(s) and reset periods.
    schedule_manager: ScheduleManager,

    /// Implements the reset.
    reset: ResetManager,

    /// Handle to History manager from which we get feedback.
    history: Arc<History>,

    /// `actions` includes a no-op, :pending.
    actions: Vec<NemesisAction>,
}

impl AdaptiveNemesis for JepsenAdaptiveNemesis {
    fn op(&self, ctx_str: &str) -> String {
        let ctx = match parse_ctx(ctx_str) {
            Ok(ctx) => ctx,
            Err(err) => {
                log::error!("[NEMESIS] op(): {}", err);
                return NemesisAction::noop().into();
            }
        };

        // Return :pending if there are no free threads to execute our nemesis operation.
        let nemesis_thread = Value::from(Keyword::from_name("nemesis"));
        match ctx.get(&Value::from(Keyword::from_name("free-threads"))) {
            Some(Value::Set(threads)) => {
                if threads.is_empty() || !threads.contains(&nemesis_thread) {
                    log::debug!("[NEMESIS] op(): no free threads; returning :pending.");
                    return NemesisAction::noop().into();
                }
            }

            _ => {
                log::error!(
                    "[NEMESIS] op(): :ctx must contain a :free-threads set. It does not: {}",
                    ctx_str
                );
                return NemesisAction::noop().into();
            }
        }

        // We work through reset operations in the reset period
        if self.schedule_manager.in_reset_time() {
            let (state, op) = self.reset.get_next_op();
            log::info!(
                "[RESET] op(): in reset time (state: {:?}); returned {}.",
                state,
                op,
            );
            op.to_string()
        } else {
            // TODO: For now, it seems that we never get called with the same context twice,
            // but we should probably ensure determinism, i.e., that within a run,
            // we always return the same action for a given context.
            let (step_id, action_id, op_status) = self
                .schedule_manager
                .get_next_op(self, self.history.as_ref());
            let action = &self.actions[action_id];
            match op_status {
                ScheduleOpStatus::New => {
                    let new_sched_str = if step_id == 0 {
                        "(Start schedule) "
                    } else {
                        ""
                    };
                    log::info!(
                        "[NEMESIS] {} Generated action id {} ({}) at step {}.",
                        new_sched_str,
                        action,
                        action_id,
                        step_id
                    );
                    action.clone().into()
                }
                ScheduleOpStatus::AlreadyReturned => {
                    log::debug!("[NEMESIS] Already returned {} at step {}.", action, step_id);
                    // Return :pending if we've already given the nemesis
                    // something to do at this step.
                    NemesisAction::noop().into()
                }
                _ => panic!(
                    "[NEMESIS] Called get_next_op() in reset period. This should not happen!"
                ),
            }
        }
    }

    fn notify_schedule_start(
        &self,
        schedule_id: ScheduleId,
        past_start_time: DateTime<Utc>,
        past_end_time: DateTime<Utc>,
        new_start_time: DateTime<Utc>,
    ) {
        self.reset.new_run();
        self.history
            .start_new_history(schedule_id, past_start_time, past_end_time, new_start_time);
    }

    fn update(&self, ctx_str: &str, _event_str: &str) {
        let _ctx = match parse_ctx(ctx_str) {
            Ok(ctx) => ctx,
            Err(err) => {
                log::error!("[NEMESIS] update(): failed to parse {}", err);
                return;
            }
        };

        //`update()` is called as soon as the operation is invoked, and then
        // once again when it completes! Rather than trying to figure out in
        // here whether this is an invocation or a completion, we instead rely
        // on the handlers for /client/invoke and /client/complete to call us
        // with the appropriate event.
    }

    fn invoke(&self, event_str: &str) {
        let (event, _) = match parse_event(event_str) {
            Ok((event, ev_map)) => (event, ev_map),
            Err(err) => {
                log::error!("[NEMESIS] invoke(): failed to parse {}", err);
                return;
            }
        };

        // Log the invocation
        log::info!("[NEMESIS] Invocation: {}", event);

        // If we are in the reset period, pass this invocation to the ResetManager.
        if self.schedule_manager.in_reset_time() {
            let (old_st, new_st) = self.reset.handle_invocation();
            log::info!(
                "[RESET] Invocation in reset time; passing invocation to ResetManager: {:?} --> {:?}.",
                old_st,
                new_st
            );
        }
    }

    fn complete(&self, event_str: &str) {
        let (event, ev_map) = match parse_event(event_str) {
            Ok((event, ev_map)) => (event, ev_map),
            Err(err) => {
                log::error!("[NEMESIS] complete(): failed to parse {}", err);
                return;
            }
        };

        let failure_indications: Vec<Value> =
            vec![Keyword::from_name("error"), Keyword::from_name("exception")]
                .iter()
                .map(|kw| Value::from(kw.clone()))
                .collect();

        let failure = ev_map.iter().any(|(k, v)| {
            failure_indications.contains(k) || *v == Value::from(Keyword::from_name("fail"))
        });
        let success = !failure;

        // Log the completion
        log::info!(
            "[NEMESIS] Completion ({}): {}",
            if success { "OK" } else { "FAIL" },
            event
        );

        // If we are in the reset period, pass this completion to the ResetManager.
        if self.schedule_manager.in_reset_time() {
            let (old_st, new_st) = self.reset.handle_completion(success);
            log::info!(
                "[RESET] Completion in reset time; passing {} completion to ResetManager: {:?} --> {:?}.",
                success,
                old_st,
                new_st
            );
        }
    }

    fn report_reward(
        &self,
        reward_entry: &RewardEntry,
        summary_producers: &Vec<SummaryProducerIdentifier>,
    ) {
        self.schedule_manager
            .report_reward(reward_entry, summary_producers);
    }

    fn print_statistics(&self) {
        let actions = self
            .actions
            .iter()
            .enumerate()
            .map(|(id, action)| format!("{}: {}", id, action))
            .collect::<Vec<String>>();

        let reset_ops = self
            .reset
            .reset_ops
            .iter()
            .enumerate()
            .map(|(id, action)| format!("{}: {}", id, action))
            .collect::<Vec<String>>();

        log::info!(
            "[STATS][NEMESIS] Nemesis with {} actions ({:?}) and {} reset actions ({:?}).",
            actions.len(),
            actions,
            reset_ops.len(),
            reset_ops
        );

        self.schedule_manager.print_statistics();
    }
}

impl JepsenAdaptiveNemesis {
    pub fn new(setup_str: &str, history: Arc<History>, cfg: &MediatorConfig) -> Self {
        let nemeses = match parse_config_str(setup_str) {
            Ok(cfg) => cfg,
            Err(err) => panic!(
                "[NEMESIS] Could not parse nemesis config {}. Error: {}",
                setup_str, err
            ),
        };

        let mut actions: Vec<NemesisAction> = Vec::new();
        let mut reset_ops: Vec<Value> = Vec::new();
        // As an implementation detail, the no-op action always has index 0.
        actions.push(NemesisAction::noop());
        for nem in &nemeses {
            actions.extend(nem.actions.iter().cloned());
            reset_ops.extend(nem.reset_ops.iter().cloned());
        }
        assert!(
            actions[DEFAULT_ACTION_ID] == NemesisAction::noop(),
            "[NEMESIS] Default action must be no-op."
        );
        let num_actions = actions.len();

        // FIXME: there must be a better way to do this.
        let (schedule_duration_ms, schedule_interval_ms, reset_duration_ms) = (
            cfg.nemesis_schedule_duration_ms,
            cfg.nemesis_schedule_interval_ms,
            cfg.nemesis_reset_duration_ms,
        );

        let schedule_type = &cfg.schedule_type;

        let cfg = NemesisConfig { nemeses };

        let schedule_manager = ScheduleManager::new(
            schedule_duration_ms,
            schedule_interval_ms,
            reset_duration_ms,
            num_actions,
            schedule_type,
        );

        let reset = ResetManager::new(reset_ops);

        // Create a new AdaptiveNemesis with an empty schedule.
        Self {
            cfg,
            schedule_manager,
            reset,
            history,
            actions,
        }
    }

    pub fn is_nemesis_operation(unparsed_event: &str) -> bool {
        let ev_map = match parse_event(unparsed_event) {
            Ok((_, ev_map)) => ev_map,
            Err(_) => return false,
        };

        let nemesis_process = Value::from(Keyword::from_name("nemesis"));
        match ev_map.get(&Value::from(Keyword::from_name("process"))) {
            Some(process) => process == &nemesis_process,
            _ => false,
        }
    }

    pub fn replace_configuration(&self, setup_str: &str) {
        let nemeses = match parse_config_str(setup_str) {
            Ok(cfg) => cfg,
            Err(err) => panic!(
                "[NEMESIS] Could not parse nemesis config {}. Error: {}",
                setup_str, err
            ),
        };

        let mut actions: Vec<NemesisAction> = Vec::new();
        let mut reset_ops: Vec<Value> = Vec::new();
        // As an implementation detail, the no-op action always has index 0.
        actions.push(NemesisAction::noop());
        for nem in &nemeses {
            actions.extend(nem.actions.iter().cloned());
            reset_ops.extend(nem.reset_ops.iter().cloned());
        }
        assert!(
            actions[DEFAULT_ACTION_ID] == NemesisAction::noop(),
            "[NEMESIS] Default action must be no-op."
        );

        // Compare to the old configuration.
        // If the actions and reset_ops are the same, we don't change anything.
        if actions == self.actions && reset_ops == self.reset.reset_ops {
            log::info!("[NEMESIS] Configuration is unchanged. Not resetting schedule.");
            return;
        }

        // Otherwise, we crash because we don't currently support changing the configuration.
        panic!(
            "[NEMESIS] Changing the configuration is not supported.
            Please restart the mediator and re-run your Jepsen test!"
        );
    }
}

impl fmt::Display for JepsenAdaptiveNemesis {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let actions = self
            .actions
            .iter()
            .map(|a| a.to_string())
            .collect::<Vec<String>>()
            .join(", ");

        write!(
            f,
            "AdaptiveNemesis({} nemeses | actions: {})",
            self.cfg.nemeses.len(),
            actions
        )
    }
}

/**************************************************
 * Reset manager. Handles periods between "runs". *
 **************************************************/

type Index = usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResetState {
    Reset(Index),
    Issued(Index),
    Done,
}

struct ResetManager {
    /// What must we do to reset the SUT to a "stable" state?
    reset_ops: Vec<Value>,

    // The reset manager is a simple state machine with states:
    //  - Reset(idx) -- we're in the middle of a reset, and we must next issue reset_ops[idx].
    //     - When we get informed of the invocation, we move to Issued(idx).
    //  - Issued(idx) -- we've issued reset_ops[idx], and we're waiting for operation to complete.
    //     - If we receive a successful completion, we move to Reset(idx+1) or Done.
    //     - If we receive a failure, we move to Reset(idx).
    //  - Done -- we've issued all reset_ops, and we're done.
    state: Mutex<ResetState>,
}

impl ResetManager {
    fn new(reset_ops: Vec<Value>) -> ResetManager {
        ResetManager {
            reset_ops,
            state: Mutex::new(ResetState::Reset(0)),
        }
    }

    fn get_next_op(&self) -> (ResetState, Value) {
        let no_op = NemesisAction::noop().into();
        let state = self.state.lock();
        let old_state = *state;
        let op = match *state {
            ResetState::Reset(idx) if !self.reset_ops.is_empty() => self.reset_ops[idx].clone(),
            _ => no_op,
        };
        (old_state, op)
    }

    fn new_run(&self) {
        let mut state = self.state.lock();
        if *state != ResetState::Done {
            log::info!(
                "[NEMESIS] Run ending, but haven't finished resetting! RM state: {:?} ({} reset actions)",
                *state,
                self.reset_ops.len()
            );
        }
        *state = ResetState::Reset(0);
    }

    fn handle_invocation(&self) -> (ResetState, ResetState) {
        // TODO: we probably want to check that the op we expected was invoked.
        let mut state = self.state.lock();
        let old_state = *state;
        let next_state = match *state {
            ResetState::Reset(idx) => ResetState::Issued(idx),
            st => st,
        };
        *state = next_state;
        (old_state, next_state)
    }

    fn handle_completion(&self, success: bool) -> (ResetState, ResetState) {
        // TODO: we probably want to check that we got a completion for
        // the op we actually issued.
        let mut state = self.state.lock();
        let old_state = *state;
        let next_state = match *state {
            ResetState::Issued(idx) if success => {
                if idx + 1 < self.reset_ops.len() {
                    ResetState::Reset(idx + 1)
                } else {
                    ResetState::Done
                }
            }
            ResetState::Issued(idx) if !success => ResetState::Reset(idx),
            st => st,
        };
        *state = next_state;
        (old_state, next_state)
    }
}

/*************************************************************
 * Parsers and data representations for talking with Jepsen. *
 *************************************************************/

fn parse_ctx(ctx_str: &str) -> Result<BTreeMap<Value, Value>, Box<dyn Error>> {
    match edn_format::parse_str(ctx_str) {
        Ok(Value::Map(ctx)) => Ok(ctx),
        Ok(e) => Err(format!(":ctx must be a map. Instead it is: {}", e))?,
        Err(e) => Err(format!("failed to parse ctx {}: {}", ctx_str, e))?,
    }
}

fn parse_config_str(config_str: &str) -> Result<Vec<SingleNemesis>, Box<dyn Error>> {
    // { :nemeses ({:enabled true, :ops [], :reset-ops []}),
    //   :opts {}
    // }

    let map = match edn_format::parse_str(config_str) {
        Ok(Value::Map(map)) => map,
        Ok(e) => Err(format!(
            "Nemesis config must be a map. Instead it is: {}",
            e
        ))?,
        Err(e) => Err(format!("Could not parse nemesis setup string: {}", e))?,
    };

    let nemeses = match map.get(&Value::from(Keyword::from_name("nemeses"))) {
        Some(Value::Vector(nemeses) | Value::List(nemeses)) => nemeses,
        Some(e) => Err(format!(
            "Nemesis config :nemeses must be a vector. Instead it is: {}",
            e
        ))?,
        None => Err(format!(
            "Nemesis config must contain a :nemeses vector. It does not: {}",
            config_str
        ))?,
    };

    let mut cfgs: Vec<SingleNemesis> = Vec::new();
    let mut nem_id: u32 = 0;
    for nem in nemeses {
        match nem {
            Value::Map(nem) => {
                let enabled = match nem.get(&Value::from(Keyword::from_name("enabled"))) {
                    Some(Value::Boolean(enabled)) => enabled,
                    _ => &false,
                };

                if !enabled {
                    continue;
                }
                nem_id += 1;

                let enabled_op_types =
                    match nem.get(&Value::from(Keyword::from_name("enabled-op-types"))) {
                        Some(Value::Vector(ops) | Value::List(ops)) => ops,
                        _ => Err("Nemesis config :enabled-op-types must be a vector")?,
                    };

                let reset_ops = match nem.get(&Value::from(Keyword::from_name("reset-ops"))) {
                    Some(Value::Vector(reset_ops) | Value::List(reset_ops)) => reset_ops,
                    _ => Err("Nemesis config :reset-ops must be a vector")?,
                };

                // NOTE: [ACTIONS]
                // We currently derive actions from the "enabled-op-types" field, but
                // we eventually want to look at "enabled-ops", which looks like:
                // ({:f :start-partition, :values [:primaries]} {:f :stop-partition, :values [nil]})
                let mut actions: Vec<NemesisAction> = Vec::new();
                for op in enabled_op_types {
                    match op {
                        Value::Keyword(op) => {
                            let action = NemesisAction::from_keyword(op);
                            actions.push(action);
                        }
                        _ => Err("Nemesis config :enabled-op-types must be a vector of keywords")?,
                    }
                }

                let cfg = SingleNemesis {
                    id: nem_id,
                    enabled_op_types: enabled_op_types
                        .iter()
                        .map(|op| match op {
                            Value::Keyword(kw) => Ok(kw.clone()),
                            _ => {
                                Err("Nemesis config :enabled-op-types must be a vector of keywords")
                            }
                        })
                        .collect::<Result<Vec<Keyword>, &str>>()?,
                    reset_ops: reset_ops.to_vec(),
                    actions,
                };
                cfgs.push(cfg)
            }

            e => Err(format!(
                ":nemeses must be a vector of maps, but it contains: {}",
                e
            ))?,
        }
    }

    Ok(cfgs)
}

fn parse_event(unparsed_event: &str) -> Result<(Value, BTreeMap<Value, Value>), Box<dyn Error>> {
    let event = match edn_format::parse_str(unparsed_event) {
        Ok(event) => event,
        Err(err) => Err(format!(
            "parse_event(): could not parse event {} due to error: {}",
            unparsed_event, err
        ))?,
    };

    // Check (heuristic) if the operation completed sucessfully or failed.
    let ev_map = match event {
        Value::Map(ref m) => m,
        _ => Err(format!(
            "parse_event(): event must be a map. Instead it is: {}",
            event
        ))?,
    }
    .clone();

    Ok((event, ev_map))
}

#[derive(Debug)]
struct SingleNemesis {
    id: u32,
    enabled_op_types: Vec<Keyword>,
    reset_ops: Vec<Value>,

    // TODO: parse the more specific enabled-ops rather than relying
    // only on the types to derive `actions`. See NOTE: [ACTIONS].
    actions: Vec<NemesisAction>,
}

#[derive(Debug)]
struct NemesisConfig {
    nemeses: Vec<SingleNemesis>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NemesisAction {
    action_type: Keyword,
    action_value: Option<Value>,
}

impl NemesisAction {
    pub fn noop() -> Self {
        Self {
            action_type: Keyword::from_name("pending"),
            action_value: None,
        }
    }

    fn new(action_type: Keyword, action_value: Option<Value>) -> Self {
        Self {
            action_type,
            action_value,
        }
    }

    fn from_keyword(kw: &Keyword) -> Self {
        Self {
            action_type: kw.clone(),
            action_value: None,
        }
    }

    pub fn as_edn(&self) -> Value {
        // We special-case :pending, since it looks different from other actions,
        // i.e. it is simply :pending rather than {:f :pending}.
        if self.action_type == Keyword::from_name("pending") {
            return Value::from(self.action_type.clone());
        }

        let mut map: BTreeMap<Value, Value> = BTreeMap::new();

        // We specially identify NemesisActions, so the Jepsen wrapper knows
        // that it needs to dispatch them at runtime. (As opposed to ResetOps,
        // which are directly executed by the Jepsen wrapper.)
        map.insert(
            Keyword::from_name("must-dispatch").into(),
            Value::from(true),
        );

        map.insert(
            Value::Keyword(Keyword::from_name("f")),
            Value::Keyword(self.action_type.clone()),
        );
        match &self.action_value {
            Some(v) => {
                map.insert(Value::Keyword(Keyword::from_name("value")), v.clone());
            }
            None => {}
        }
        Value::Map(map)
    }
}

impl fmt::Display for NemesisAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_edn())
    }
}

impl Into<Value> for NemesisAction {
    fn into(self) -> Value {
        self.as_edn()
    }
}

impl Into<String> for NemesisAction {
    fn into(self) -> String {
        self.as_edn().to_string()
    }
}
