use async_trait::async_trait;
use gateway_admin::{
    model::{
        MutationContext,
        auto_wake::{AutoWakeConfig, AutoWakeRecord, AutoWakeState},
    },
    ports::{auto_wake::AutoWakeRepository, store::AdminStoreResult},
};
use std::collections::BTreeMap;
use std::sync::Mutex;

#[derive(Default)]
pub(super) struct MemoryAutoWake(Mutex<BTreeMap<String, AutoWakeRecord>>);

#[async_trait]
impl AutoWakeRepository for MemoryAutoWake {
    async fn load(&self, id: &str) -> AdminStoreResult<Option<AutoWakeRecord>> {
        Ok(self.0.lock().unwrap().get(id).cloned())
    }
    async fn ready(&self, now: i64, limit: u32) -> AdminStoreResult<Vec<AutoWakeRecord>> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .values()
            .filter(|r| r.config.enabled && r.state.check_after <= now)
            .take(limit as usize)
            .cloned()
            .collect())
    }
    async fn configure(
        &self,
        id: &str,
        config: AutoWakeConfig,
        state: AutoWakeState,
        _: &MutationContext,
    ) -> AdminStoreResult<AutoWakeRecord> {
        let mut records = self.0.lock().unwrap();
        let record = AutoWakeRecord {
            account_id: id.to_owned(),
            generation: records.get(id).map_or(1, |r| r.generation + 1),
            config,
            state,
        };
        records.insert(id.to_owned(), record.clone());
        Ok(record)
    }
    async fn compare_exchange(
        &self,
        expected: &AutoWakeRecord,
        state: AutoWakeState,
    ) -> AdminStoreResult<bool> {
        let mut records = self.0.lock().unwrap();
        if records.get(&expected.account_id) != Some(expected) {
            return Ok(false);
        }
        records.insert(
            expected.account_id.clone(),
            AutoWakeRecord {
                state,
                ..expected.clone()
            },
        );
        Ok(true)
    }
}

use super::{
    AdminHarness,
    accounts::{FakeAccountStore, FakeProviderAdmin, account_record, events},
};
use chrono::{Duration, Utc};
use gateway_admin::model::{
    MutationActor, auto_wake::AutoWakeTrigger, provider_credentials::ProviderQuota,
};
use gateway_core::{
    engine::probe::{AccountProbe, AccountProbeError, AccountProbeRequest, AccountProbeResult},
    lifecycle::CancellationToken,
    task::{
        ScheduledTask, WorkerContribution, WorkerCycleContext, WorkerId, WorkerKind, WorkerRunnable,
    },
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct CountingProbe(AtomicUsize);
impl AccountProbe for CountingProbe {
    fn probe(
        &self,
        _: AccountProbeRequest,
    ) -> futures::future::BoxFuture<'_, Result<AccountProbeResult, AccountProbeError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Ok(AccountProbeResult {
                text: vec!["OK".to_owned()],
            })
        })
    }
}

fn context() -> MutationContext {
    MutationContext {
        actor: MutationActor::System,
        request_id: "wake-test".to_owned(),
    }
}

#[tokio::test]
async fn reading_and_disabling_a_plan_does_not_call_the_unavailable_provider() {
    let repository = Arc::new(MemoryAutoWake::default());
    let store = FakeAccountStore::new("openai", events());
    let provider = FakeProviderAdmin::new("openai", events());
    provider.fail_next_quota(gateway_admin::ports::provider::ProviderAdminErrorKind::Unavailable);
    repository
        .configure(
            "acct_test",
            AutoWakeConfig {
                enabled: true,
                model: "removed-model".to_owned(),
                ..Default::default()
            },
            AutoWakeState::default(),
            &context(),
        )
        .await
        .unwrap();
    let mut harness = AdminHarness::new()
        .accounts(store)
        .provider(provider.clone());
    harness.auto_wake = repository;
    let services = harness.build().await;
    let id = gateway_core::account::ProviderAccountId::new("acct_test").unwrap();
    let mut record = services.auto_wake().get(id.clone()).await.unwrap();
    record.config.enabled = false;
    let saved = services
        .auto_wake()
        .configure(id, record.config, &context())
        .await
        .unwrap();
    assert!(!saved.config.enabled);
    assert!(provider.quota_requests().is_empty());
}

async fn task(
    repository: Arc<MemoryAutoWake>,
    provider: Arc<FakeProviderAdmin>,
    enabled: bool,
    probe: Arc<CountingProbe>,
) -> Box<dyn ScheduledTask> {
    let store = FakeAccountStore::new("openai", events());
    let mut account = account_record("openai");
    account.enabled = enabled;
    store.set_accounts(vec![account]);
    let mut harness = AdminHarness::new()
        .accounts(store)
        .provider(provider)
        .probe(probe);
    harness.auto_wake = repository;
    let mut bundle = harness.build_bundle().await;
    for contribution in bundle.take_worker_contributions() {
        if let WorkerContribution::Registration(registration) = contribution
            && registration.id.kind() == WorkerKind::AccountAutoWake
            && let WorkerRunnable::Scheduled { task, .. } = registration.runnable
        {
            return task;
        }
    }
    panic!("auto wake worker missing");
}

async fn run(task: &dyn ScheduledTask) {
    task.run_cycle(WorkerCycleContext::new(
        WorkerId::try_new(WorkerKind::AccountAutoWake, "test").unwrap(),
        None,
        CancellationToken::new(),
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn scheduled_wake_is_persisted_and_restart_does_not_replay() {
    let repository = Arc::new(MemoryAutoWake::default());
    let provider = FakeProviderAdmin::new("openai", events());
    let probe = Arc::new(CountingProbe::default());
    repository
        .configure(
            "acct_test",
            AutoWakeConfig {
                enabled: true,
                model: "test-model".to_owned(),
                ..Default::default()
            },
            AutoWakeState {
                next_run_at: Some(Utc::now().timestamp() - 1),
                ..Default::default()
            },
            &context(),
        )
        .await
        .unwrap();
    run(
        task(repository.clone(), provider.clone(), true, probe.clone())
            .await
            .as_ref(),
    )
    .await;
    assert_eq!(probe.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        repository
            .load("acct_test")
            .await
            .unwrap()
            .unwrap()
            .state
            .last_status
            .as_deref(),
        Some("succeeded")
    );
    run(task(repository, provider, true, probe.clone())
        .await
        .as_ref())
    .await;
    assert_eq!(probe.0.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn disabled_account_skips_scheduled_wake() {
    let repository = Arc::new(MemoryAutoWake::default());
    let probe = Arc::new(CountingProbe::default());
    repository
        .configure(
            "acct_test",
            AutoWakeConfig {
                enabled: true,
                model: "test-model".to_owned(),
                ..Default::default()
            },
            AutoWakeState {
                next_run_at: Some(1),
                ..Default::default()
            },
            &context(),
        )
        .await
        .unwrap();
    run(task(
        repository.clone(),
        FakeProviderAdmin::new("openai", events()),
        false,
        probe.clone(),
    )
    .await
    .as_ref())
    .await;
    assert_eq!(probe.0.load(Ordering::SeqCst), 0);
    assert_eq!(
        repository
            .load("acct_test")
            .await
            .unwrap()
            .unwrap()
            .state
            .last_status
            .as_deref(),
        Some("skipped")
    );
}

#[tokio::test]
async fn reset_requires_fresh_quota_and_is_consumed_once() {
    let repository = Arc::new(MemoryAutoWake::default());
    let provider = FakeProviderAdmin::new("openai", events());
    let probe = Arc::new(CountingProbe::default());
    let now = Utc::now();
    let reset = now.timestamp() - 180;
    let config = AutoWakeConfig {
        enabled: true,
        model: "test-model".to_owned(),
        trigger: AutoWakeTrigger::EitherReset,
        ..Default::default()
    };
    let state = AutoWakeState {
        reset_targets: BTreeMap::from([("primary".to_owned(), reset)]),
        ..Default::default()
    };
    repository
        .configure("acct_test", config, state, &context())
        .await
        .unwrap();
    let mut window = crate::quota_window("primary", Some(18000), Some(0.0));
    window.reset_at = Some(now + Duration::hours(5));
    let mut quota = ProviderQuota {
        plan_type: None,
        observed_at: Some(now - Duration::minutes(10)),
        refresh_token_expires_at: None,
        windows: vec![window],
        limit_reached: false,
        provider_data: None,
    };
    provider.set_quota(quota.clone());
    let worker = task(repository.clone(), provider.clone(), true, probe.clone()).await;
    run(worker.as_ref()).await;
    assert_eq!(probe.0.load(Ordering::SeqCst), 0);
    provider.fail_next_quota(gateway_admin::ports::provider::ProviderAdminErrorKind::Unavailable);
    repository
        .0
        .lock()
        .unwrap()
        .get_mut("acct_test")
        .unwrap()
        .state
        .check_after = 0;
    run(worker.as_ref()).await;
    assert_eq!(probe.0.load(Ordering::SeqCst), 0);
    quota.observed_at = Some(Utc::now());
    provider.set_quota(quota);
    repository
        .0
        .lock()
        .unwrap()
        .get_mut("acct_test")
        .unwrap()
        .state
        .check_after = 0;
    run(worker.as_ref()).await;
    assert_eq!(probe.0.load(Ordering::SeqCst), 1);
    let saved = repository.load("acct_test").await.unwrap().unwrap();
    assert_eq!(saved.state.consumed_resets.get("primary"), Some(&reset));
    repository
        .0
        .lock()
        .unwrap()
        .get_mut("acct_test")
        .unwrap()
        .state
        .check_after = 0;
    run(worker.as_ref()).await;
    assert_eq!(probe.0.load(Ordering::SeqCst), 1);
}
