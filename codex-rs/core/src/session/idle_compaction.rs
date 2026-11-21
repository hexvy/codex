//! Native, best-effort idle compaction for a live thread.
//!
//! The timer is polled by the ordered submission loop. Admission reserves the
//! active turn and checks for history replacement before doing any model work.
//! Wall time also bounds requests after a suspend or clock jump; on Linux a
//! suspend-aware boot clock supplies an additional independent bound.

use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use codex_extension_api::ThreadIdleCause;
use codex_features::Feature;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use tokio::time::Instant;

use super::session::Session;
use super::step_settings::StepSettingsUpdate;
use super::turn_context::TurnContext;
use crate::state::ActiveTurn;

const CLOCK_RECHECK_INTERVAL: Duration = Duration::from_secs(60);

/// The final response already installed a compacted context window.
pub(super) struct PostTurnCompacted;

/// A maintenance turn must not change a worker's result or report one to its parent.
pub(crate) struct WorkerIdleCompaction;

#[derive(Clone, Copy)]
pub(crate) struct IdleCompactionStart {
    monotonic: Instant,
    wall: SystemTime,
    boot: Option<Duration>,
}

impl IdleCompactionStart {
    pub(crate) fn now() -> Self {
        Self {
            monotonic: Instant::now(),
            wall: SystemTime::now(),
            boot: boot_time(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IdleCompactionSchedule {
    completed_turn_id: String,
    model: String,
    history_version: u64,
    ready: bool,
    started_at_wall: SystemTime,
    started_at_boot: Option<Duration>,
    next_attempt: Instant,
    next_attempt_wall: SystemTime,
    next_attempt_boot: Option<Duration>,
    expires_at: Instant,
    cache_window: Duration,
    retry: Duration,
}

impl IdleCompactionSchedule {
    fn matches_state(&self, state: &crate::state::SessionState) -> bool {
        !state.shutting_down && state.history.history_version() == self.history_version
    }

    fn expired(&self) -> bool {
        let boot_expired = self.started_at_boot.is_some_and(|started| {
            boot_time()
                .and_then(|now| now.checked_sub(started))
                .is_none_or(|elapsed| elapsed >= self.cache_window)
        });
        boot_expired
            || Instant::now() >= self.expires_at
            || SystemTime::now()
                .duration_since(self.started_at_wall)
                .map_or(true, |elapsed| elapsed >= self.cache_window)
    }

    fn due(&self) -> bool {
        Instant::now() >= self.next_attempt
            || SystemTime::now() >= self.next_attempt_wall
            || self
                .next_attempt_boot
                .zip(boot_time())
                .is_some_and(|(due, now)| now >= due)
    }

    fn next_check(&self) -> Instant {
        let deadline = if self.ready {
            self.next_attempt
        } else {
            self.expires_at
        };
        deadline.min(Instant::now() + CLOCK_RECHECK_INTERVAL)
    }
}

#[cfg(target_os = "linux")]
fn boot_time() -> Option<Duration> {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: CLOCK_BOOTTIME writes one timespec to this valid mutable pointer.
    if unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut time) } != 0 {
        return None;
    }
    Some(Duration::new(
        time.tv_sec.try_into().ok()?,
        time.tv_nsec.try_into().ok()?,
    ))
}

#[cfg(not(target_os = "linux"))]
fn boot_time() -> Option<Duration> {
    None
}

impl Session {
    pub(crate) fn cancel_idle_compaction(&self) {
        self.idle_compaction
            .send_if_modified(|current| current.take().is_some());
    }

    /// The caller holds the active-turn lock until after the schedule is published.
    pub(crate) async fn prepare_idle_compaction(
        &self,
        turn: &TurnContext,
        cause: ThreadIdleCause,
        last_model: Option<&str>,
        started: IdleCompactionStart,
    ) -> Option<IdleCompactionSchedule> {
        if !turn.config.features.enabled(Feature::IdleCompact)
            || cause == ThreadIdleCause::Interrupted
            || (turn.session_source.is_non_root_agent()
                && !matches!(
                    &turn.session_source,
                    SessionSource::SubAgent(SubAgentSource::ThreadSpawn { .. })
                ))
            || turn.config.features.enabled(Feature::TokenBudget)
            || turn.extension_data.get::<PostTurnCompacted>().is_some()
            || self
                .services
                .thread_extension_data
                .get::<crate::compact::AutoCompactionDisabled>()
                .is_some()
        {
            return None;
        }
        let current_model = turn.capture_current_model_info();
        let model = last_model.unwrap_or(&current_model.slug);
        let Some(policy) = turn.config.idle_compaction.for_model(
            &turn.config.model_provider_id,
            turn.provider.info(),
            model,
        ) else {
            tracing::debug!(provider = %turn.config.model_provider_id, %model,
                "no usable idle-compaction cache policy for this model");
            return None;
        };
        let IdleCompactionStart {
            monotonic: now,
            wall,
            boot,
        } = started;
        #[cfg(target_os = "linux")]
        boot?;
        let (Some(next_attempt), Some(expires_at), Some(next_attempt_wall)) = (
            now.checked_add(policy.delay),
            now.checked_add(policy.cache_window),
            wall.checked_add(policy.delay),
        ) else {
            return None;
        };
        let state = self.state.lock().await;
        if self.is_interrupted() || state.shutting_down {
            return None;
        }
        let schedule = IdleCompactionSchedule {
            completed_turn_id: turn.sub_id.clone(),
            model: model.to_owned(),
            history_version: state.history.history_version(),
            ready: false,
            started_at_wall: wall,
            started_at_boot: boot,
            next_attempt,
            next_attempt_wall,
            next_attempt_boot: boot.and_then(|time| time.checked_add(policy.delay)),
            expires_at,
            cache_window: policy.cache_window,
            retry: policy.retry,
        };
        self.idle_compaction.send_replace(Some(schedule.clone()));
        Some(schedule)
    }

    /// Let higher-priority idle lifecycle contributors run before the timer can dispatch.
    pub(crate) fn arm_idle_compaction(&self, expected: &IdleCompactionSchedule) {
        self.idle_compaction.send_if_modified(|current| {
            if current.as_ref() != Some(expected) {
                return false;
            }
            if expected.expired() {
                *current = None;
            } else if let Some(schedule) = current.as_mut() {
                schedule.ready = true;
            }
            true
        });
    }

    pub(crate) fn clear_idle_compaction(&self, expected: &IdleCompactionSchedule) {
        self.idle_compaction.send_if_modified(|current| {
            if current.as_ref() == Some(expected) {
                *current = None;
                true
            } else {
                false
            }
        });
    }

    pub(crate) fn idle_compaction_is_current(&self, expected: &IdleCompactionSchedule) -> bool {
        !expected.expired() && self.idle_compaction.borrow().as_ref() == Some(expected)
    }

    /// Claim a still-current schedule while the caller holds the active-turn lock.
    pub(crate) async fn claim_idle_compaction(
        &self,
        expected: &IdleCompactionSchedule,
        turn_id: &str,
    ) -> bool {
        let mut state = self.state.lock().await;
        if !expected.matches_state(&state) || !self.idle_compaction_is_current(expected) {
            return false;
        }
        state.last_started_turn_id = Some(turn_id.to_owned());
        self.clear_idle_compaction(expected);
        true
    }

    fn retry_idle_compaction(&self, expected: &IdleCompactionSchedule) {
        self.idle_compaction.send_if_modified(|current| {
            if current.as_ref() != Some(expected) {
                return false;
            }
            let now = Instant::now();
            let wall = SystemTime::now();
            match (
                now.checked_add(expected.retry),
                wall.checked_add(expected.retry),
            ) {
                (Some(next_attempt), Some(next_attempt_wall))
                    if next_attempt < expected.expires_at =>
                {
                    let mut next = expected.clone();
                    next.next_attempt = next_attempt;
                    next.next_attempt_wall = next_attempt_wall;
                    next.next_attempt_boot =
                        boot_time().and_then(|time| time.checked_add(expected.retry));
                    *current = Some(next);
                }
                _ => *current = None,
            }
            true
        });
    }

    /// Deadline used by the app server to retain an unsubscribed runtime with pending work.
    pub(crate) fn idle_compaction_retain_until(&self) -> Option<std::time::Instant> {
        self.idle_compaction
            .borrow()
            .as_ref()
            .and_then(|schedule| (!schedule.expired()).then_some(schedule.expires_at.into_std()))
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "reserve the idle turn atomically with validating its history and predecessor"
    )]
    pub(super) async fn check_idle_compaction(self: &Arc<Self>) {
        let Some(schedule) = self.idle_compaction.borrow().clone() else {
            return;
        };
        if schedule.expired() {
            self.clear_idle_compaction(&schedule);
            return;
        }
        if !schedule.ready {
            return;
        }
        if !schedule.due() {
            return;
        }
        let Some(_admission) = self.services.extensions.admit_turn_start() else {
            self.clear_idle_compaction(&schedule);
            return;
        };
        let turn_state = {
            let mut active = self.active_turn.lock().await;
            let state = self.state.lock().await;
            if !schedule.matches_state(&state) {
                self.clear_idle_compaction(&schedule);
                return;
            }
            if active.is_some() {
                self.retry_idle_compaction(&schedule);
                return;
            }
            let reservation = active.get_or_insert_with(ActiveTurn::default);
            Arc::clone(&reservation.turn_state)
        };
        if self.has_runnable_mail().await {
            self.clear_reserved_idle_turn(&turn_state).await;
            self.retry_idle_compaction(&schedule);
            self.maybe_start_turn_for_pending_work().await;
            return;
        }
        // Next-turn settings may have changed without touching the cached model prefix.
        // Use the completed turn's model without reverting the user's selection.
        let mut config = self.state.lock().await.session_configuration.clone();
        if config.step_settings.collaboration_mode.model() != schedule.model {
            let settings = config.step_settings.apply(
                &StepSettingsUpdate {
                    model: Some(schedule.model.clone()),
                    effort: Some(None),
                    ..Default::default()
                },
                &config.step_settings_constraints(&config.environments),
            );
            match settings {
                Ok(settings) => config.step_settings = Arc::new(settings),
                Err(error) => {
                    tracing::debug!(model = %schedule.model, %error, "idle-compaction model no longer meets thread requirements");
                    self.clear_reserved_idle_turn(&turn_state).await;
                    self.clear_idle_compaction(&schedule);
                    self.maybe_start_turn_for_pending_work().await;
                    return;
                }
            }
        }
        let turn = self.new_idle_compaction_turn(config).await;
        let reservation_is_current = self
            .active_turn
            .lock()
            .await
            .as_ref()
            .is_some_and(|active| {
                active.task.is_none() && Arc::ptr_eq(&active.turn_state, &turn_state)
            });
        let history_is_current =
            self.state.lock().await.history.history_version() == schedule.history_version;
        let schedule_is_current = self.idle_compaction.borrow().as_ref() == Some(&schedule);
        if schedule.expired()
            || !reservation_is_current
            || !history_is_current
            || !schedule_is_current
            || turn.capture_current_model_info().slug != schedule.model
        {
            self.clear_reserved_idle_turn(&turn_state).await;
            self.clear_idle_compaction(&schedule);
            self.maybe_start_turn_for_pending_work().await;
            return;
        }
        if self.has_runnable_mail().await {
            self.clear_reserved_idle_turn(&turn_state).await;
            self.retry_idle_compaction(&schedule);
            self.maybe_start_turn_for_pending_work().await;
            return;
        }
        if self.host_turn_admission().is_none()
            && self
                .services
                .agent_control
                .check_turn_admission(turn.multi_agent_version, &turn.session_source)
                .is_err()
        {
            self.clear_reserved_idle_turn(&turn_state).await;
            self.retry_idle_compaction(&schedule);
            return;
        }
        if !self
            .start_reserved_idle_compaction(turn, &turn_state, &schedule)
            .await
        {
            self.clear_reserved_idle_turn(&turn_state).await;
            self.clear_idle_compaction(&schedule);
            self.maybe_start_turn_for_pending_work().await;
        }
    }
}

pub(super) async fn wait_for_check(schedule: Option<IdleCompactionSchedule>) {
    if let Some(schedule) = schedule {
        tokio::time::sleep_until(schedule.next_check()).await;
    } else {
        std::future::pending::<()>().await;
    }
}
