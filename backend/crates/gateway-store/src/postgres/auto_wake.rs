//! 自动唤醒持久化与原子领取；配置写入同时记录安全审计。

use async_trait::async_trait;
use gateway_admin::{
    model::{
        MutationContext,
        auto_wake::{AutoWakeConfig, AutoWakeRecord, AutoWakeState},
    },
    ports::{
        auto_wake::AutoWakeRepository,
        store::{AdminStoreError, AdminStoreErrorKind, AdminStoreResult},
    },
};
use sqlx::{PgPool, Row as _, postgres::PgRow};

pub struct PgAutoWakeRepository {
    pool: PgPool,
}

impl PgAutoWakeRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn unavailable() -> AdminStoreError {
    AdminStoreError::new(
        AdminStoreErrorKind::Unavailable,
        "auto wake",
        "自动唤醒存储不可用",
    )
}

fn decode(row: PgRow) -> AdminStoreResult<AutoWakeRecord> {
    Ok(AutoWakeRecord {
        account_id: row.try_get("account_id").map_err(|_| unavailable())?,
        generation: row.try_get("generation").map_err(|_| unavailable())?,
        config: serde_json::from_value(row.try_get("config").map_err(|_| unavailable())?)
            .map_err(|_| unavailable())?,
        state: serde_json::from_value(row.try_get("state").map_err(|_| unavailable())?)
            .map_err(|_| unavailable())?,
    })
}

#[async_trait]
impl AutoWakeRepository for PgAutoWakeRepository {
    async fn load(&self, account_id: &str) -> AdminStoreResult<Option<AutoWakeRecord>> {
        sqlx::query("select account_id, generation, config, state from account_auto_wake where account_id=$1")
            .bind(account_id).fetch_optional(&self.pool).await.map_err(|_| unavailable())?.map(decode).transpose()
    }

    async fn ready(&self, now: i64, limit: u32) -> AdminStoreResult<Vec<AutoWakeRecord>> {
        sqlx::query("select account_id, generation, config, state from account_auto_wake where config->>'enabled'='true' and (state->>'checkAfter')::bigint <= $1 order by (state->>'checkAfter')::bigint, account_id limit $2")
            .bind(now).bind(i64::from(limit.min(100))).fetch_all(&self.pool).await.map_err(|_| unavailable())?.into_iter().map(decode).collect()
    }

    async fn configure(
        &self,
        account_id: &str,
        config: AutoWakeConfig,
        state: AutoWakeState,
        context: &MutationContext,
    ) -> AdminStoreResult<AutoWakeRecord> {
        let mut tx = self.pool.begin().await.map_err(|_| unavailable())?;
        let row = sqlx::query("insert into account_auto_wake (account_id, config, state) values ($1,$2,$3) on conflict (account_id) do update set config=excluded.config, state=excluded.state || jsonb_build_object('lastAttemptAt', account_auto_wake.state->'lastAttemptAt', 'lastTrigger', account_auto_wake.state->'lastTrigger', 'lastStatus', account_auto_wake.state->'lastStatus', 'lastMessage', account_auto_wake.state->'lastMessage', 'consumedResets', account_auto_wake.state->'consumedResets'), generation=account_auto_wake.generation+1, updated_at=now() returning account_id,generation,config,state")
            .bind(account_id).bind(serde_json::to_value(config).map_err(|_| unavailable())?).bind(serde_json::to_value(state).map_err(|_| unavailable())?)
            .fetch_one(&mut *tx).await.map_err(|_| unavailable())?;
        let event = crate::mutation_audit(
            context,
            "account.auto_wake_updated",
            "provider_account",
            account_id,
            vec!["auto_wake".to_owned()],
        );
        super::admin_security_audit::append_admin_audit_event_in_transaction(&mut tx, event, None)
            .await
            .map_err(|_| unavailable())?;
        let record = decode(row)?;
        tx.commit().await.map_err(|_| unavailable())?;
        Ok(record)
    }

    async fn compare_exchange(
        &self,
        expected: &AutoWakeRecord,
        state: AutoWakeState,
    ) -> AdminStoreResult<bool> {
        let result = sqlx::query("update account_auto_wake set state=$1, updated_at=now() where account_id=$2 and generation=$3 and state=$4")
            .bind(serde_json::to_value(state).map_err(|_| unavailable())?).bind(&expected.account_id).bind(expected.generation)
            .bind(serde_json::to_value(&expected.state).map_err(|_| unavailable())?).execute(&self.pool).await.map_err(|_| unavailable())?;
        Ok(result.rows_affected() == 1)
    }
}
