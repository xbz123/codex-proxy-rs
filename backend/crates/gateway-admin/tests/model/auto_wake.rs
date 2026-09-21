use chrono::{DateTime, Utc};
use gateway_admin::model::auto_wake::{AutoWakeConfig, AutoWakeState, AutoWakeTrigger};

fn at(value: &str) -> DateTime<Utc> {
    value.parse().unwrap()
}

#[test]
fn schedule_respects_timezone_and_never_replays_current_slot() {
    let config = AutoWakeConfig {
        cron: "0 8 * * *".to_owned(),
        ..Default::default()
    };
    assert_eq!(
        config.next_after(at("2026-09-22T00:00:00Z")).unwrap(),
        at("2026-09-23T00:00:00Z").timestamp()
    );
    let berlin = AutoWakeConfig {
        timezone: "Europe/Berlin".to_owned(),
        ..config
    };
    assert_eq!(
        berlin.next_after(at("2026-10-25T00:00:00Z")).unwrap(),
        at("2026-10-25T07:00:00Z").timestamp()
    );
}

#[test]
fn enabled_schedule_requires_a_model_and_valid_cron_timezone() {
    let now = Utc::now();
    let mut config = AutoWakeConfig {
        enabled: true,
        ..Default::default()
    };
    assert!(config.validate(now).is_err());
    config.model = "gpt-5.6-sol".to_owned();
    assert!(config.validate(now).is_ok());
    config.cron = "* * * * * *".to_owned();
    assert!(config.validate(now).is_err());
    config.cron = "0 8 * * *".to_owned();
    config.timezone = "not-a-zone".to_owned();
    assert!(config.validate(now).is_err());
}

#[test]
fn both_consumed_resets_stay_consumed_after_serialization_and_restart() {
    let mut state = AutoWakeState::default();
    state.reset_targets.insert("five-hour".to_owned(), 1000);
    state.reset_targets.insert("weekly".to_owned(), 1100);
    assert_eq!(state.due_reset(1119), None);
    assert_eq!(state.due_reset(1120), Some(("five-hour", 1000)));
    state.consumed_resets = state.reset_targets.clone();
    let restored: AutoWakeState =
        serde_json::from_str(&serde_json::to_string(&state).unwrap()).unwrap();
    assert_eq!(restored.due_reset(5000), None);
    state.reset_targets.insert("five-hour".to_owned(), 19000);
    assert_eq!(state.due_reset(19120), Some(("five-hour", 19000)));
}

#[test]
fn unknown_and_model_specific_windows_cannot_trigger_account_wakeup() {
    use gateway_admin::model::provider_credentials::{ProviderQuota, QuotaLocalUsageAttribution};
    let mut window = crate::quota_window("primary", Some(18000), Some(10.0));
    window.reset_at = Some(at("2026-09-22T01:00:00Z"));
    let mut quota = ProviderQuota {
        plan_type: None,
        observed_at: None,
        refresh_token_expires_at: None,
        windows: vec![window],
        limit_reached: false,
        provider_data: None,
    };
    let config = AutoWakeConfig {
        trigger: AutoWakeTrigger::EitherReset,
        ..Default::default()
    };
    assert_eq!(config.reset_targets(&quota).len(), 1);
    quota.windows[0].local_usage_attribution = QuotaLocalUsageAttribution::Unavailable;
    assert!(config.reset_targets(&quota).is_empty());
    quota.windows[0].local_usage_attribution = QuotaLocalUsageAttribution::AccountWide;
    quota.windows[0].reset_at = None;
    assert!(config.reset_targets(&quota).is_empty());
}

#[test]
fn uncertain_attempt_enforces_minimum_interval_even_after_clock_regression() {
    let state = AutoWakeState {
        last_attempt_at: Some(1000),
        ..Default::default()
    };
    assert!(!state.can_attempt(999));
    assert!(!state.can_attempt(1299));
    assert!(state.can_attempt(1300));
}
