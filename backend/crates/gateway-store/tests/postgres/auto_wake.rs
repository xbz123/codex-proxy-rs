use super::{TestDatabase, provider_accounts::account};
use gateway_admin::{
    model::{
        MutationActor, MutationContext,
        auto_wake::{AutoWakeConfig, AutoWakeState},
    },
    ports::auto_wake::AutoWakeRepository,
};
use gateway_store::postgres::{
    PgAutoWakeRepository, PgProviderAccountRepository, ProviderAccountRepository,
};

#[tokio::test]
async fn claims_are_atomic_configuration_fences_old_work_and_delete_cascades() {
    let Some(database) = TestDatabase::create("auto_wake").await else {
        return;
    };
    PgProviderAccountRepository::new(database.pool.clone())
        .insert_provider_account(account("acct_wake", "wake-test"))
        .await
        .unwrap();
    let repository = PgAutoWakeRepository::new(database.pool.clone());
    let context = MutationContext {
        actor: MutationActor::System,
        request_id: "wake-test".to_owned(),
    };
    let config = AutoWakeConfig {
        enabled: true,
        model: "test".to_owned(),
        ..Default::default()
    };
    let record = repository
        .configure(
            "acct_wake",
            config.clone(),
            AutoWakeState::default(),
            &context,
        )
        .await
        .unwrap();
    let claimed = AutoWakeState {
        last_attempt_at: Some(100),
        check_after: 400,
        ..Default::default()
    };
    let (first, second) = tokio::join!(
        repository.compare_exchange(&record, claimed.clone()),
        repository.compare_exchange(&record, claimed.clone())
    );
    assert_ne!(first.unwrap(), second.unwrap());
    assert!(repository.ready(399, 20).await.unwrap().is_empty());
    let old = repository.load("acct_wake").await.unwrap().unwrap();
    assert_eq!(repository.ready(400, 20).await.unwrap().len(), 1);
    let disabled = repository
        .configure(
            "acct_wake",
            AutoWakeConfig {
                enabled: false,
                ..config
            },
            AutoWakeState::default(),
            &context,
        )
        .await
        .unwrap();
    assert_eq!(disabled.generation, old.generation + 1);
    assert_eq!(disabled.state.last_attempt_at, Some(100));
    assert!(!repository.compare_exchange(&old, claimed).await.unwrap());
    assert!(repository.ready(1000, 20).await.unwrap().is_empty());
    let count: i64 = sqlx::query_scalar(
        "select count(*) from admin_audit_events where action='account.auto_wake_updated'",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(count, 2);
    sqlx::query("delete from provider_accounts where id='acct_wake'")
        .execute(&database.pool)
        .await
        .unwrap();
    assert!(repository.load("acct_wake").await.unwrap().is_none());
    database.close().await;
}
