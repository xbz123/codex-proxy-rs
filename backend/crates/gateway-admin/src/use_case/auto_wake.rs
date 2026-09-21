//! 自动唤醒策略与后台编排；所有推理复用账号连接测试。

use super::{accounts::AccountsService, map_store_error};
use crate::{
    model::{
        AdminError, MutationContext,
        accounts::{AccountConnectionTestEvent, AccountRecord, AccountRuntimeSnapshot},
        auto_wake::{AutoWakeConfig, AutoWakeRecord, AutoWakeState, AutoWakeTrigger},
    },
    ports::{auto_wake::AutoWakeRepository, store::AccountStore},
};
use async_trait::async_trait;
use chrono::Utc;
use futures::StreamExt as _;
use gateway_core::{
    account::{AccountStatus, ProviderAccountId},
    routing::UpstreamModelId,
    task::{ScheduledTask, WorkerCycleContext, WorkerTaskError},
};
use std::{sync::Arc, time::Duration};

#[async_trait]
pub trait AutoWakeService: Send + Sync {
    async fn get(&self, account_id: ProviderAccountId) -> Result<AutoWakeRecord, AdminError>;
    async fn configure(
        &self,
        account_id: ProviderAccountId,
        config: AutoWakeConfig,
        context: &MutationContext,
    ) -> Result<AutoWakeRecord, AdminError>;
}

pub(crate) struct DefaultAutoWakeService {
    repository: Arc<dyn AutoWakeRepository>,
    accounts: Arc<dyn AccountsService>,
    store: Arc<dyn AccountStore>,
}

impl DefaultAutoWakeService {
    pub(crate) fn new(
        repository: Arc<dyn AutoWakeRepository>,
        accounts: Arc<dyn AccountsService>,
        store: Arc<dyn AccountStore>,
    ) -> Self {
        Self {
            repository,
            accounts,
            store,
        }
    }

    async fn load(&self, id: &ProviderAccountId) -> Result<AutoWakeRecord, AdminError> {
        Ok(self
            .repository
            .load(id.as_str())
            .await
            .map_err(|e| map_store_error(e, "auto wake"))?
            .unwrap_or_else(|| AutoWakeRecord {
                account_id: id.as_str().to_owned(),
                generation: 0,
                config: AutoWakeConfig::default(),
                state: AutoWakeState::default(),
            }))
    }

    fn supported(account: &AccountRecord) -> Result<(), AdminError> {
        if account.provider_kind.as_str() != "openai" || account.authentication_kind != "oauth" {
            return Err(AdminError::invalid("自动唤醒仅支持 OpenAI OAuth 账号"));
        }
        Ok(())
    }

    /// 查询和关闭配置不依赖上游，账号或模型服务故障时仍允许停止计划。
    async fn require_account(&self, id: &ProviderAccountId) -> Result<(), AdminError> {
        let item = self
            .store
            .load_account(id.as_str(), AccountRuntimeSnapshot::default())
            .await
            .map_err(|error| map_store_error(error, "auto wake account"))?
            .ok_or_else(|| AdminError::not_found("Provider 账号不存在"))?;
        Self::supported(&item.account)
    }

    async fn save_state(
        &self,
        record: &AutoWakeRecord,
        state: AutoWakeState,
    ) -> Result<bool, AdminError> {
        self.repository
            .compare_exchange(record, state)
            .await
            .map_err(|e| map_store_error(e, "auto wake"))
    }

    /// 周期检查只在到期后申请执行权；申请成功后不重试不确定的推理结果。
    async fn process(&self, record: AutoWakeRecord) -> Result<(), AdminError> {
        let now = Utc::now();
        let mut state = record.state.clone();
        state.check_after = now.timestamp() + 300;
        let scheduled = record.config.trigger == AutoWakeTrigger::Scheduled;
        let due = if scheduled {
            state.next_run_at.is_some_and(|at| at <= now.timestamp())
        } else {
            state.due_reset(now.timestamp()).is_some()
        };
        if scheduled && !due {
            state.check_after = state.next_run_at.unwrap_or(state.check_after);
            self.save_state(&record, state).await?;
            return Ok(());
        }
        let id = ProviderAccountId::new(record.account_id.clone())
            .map_err(|_| AdminError::invalid("账号 ID 不合法"))?;
        // 重置计划刷新额度；请求失败不能凭本地时钟推断额度已恢复。
        let item = match self.accounts.quota(&id, !scheduled).await {
            Ok(item) => item,
            Err(_) => {
                state.last_status = Some("waiting".to_owned());
                state.last_message = Some("账号或额度查询失败，5 分钟后复核".to_owned());
                self.save_state(&record, state).await?;
                return Ok(());
            }
        };
        if Self::supported(&item.account).is_err() {
            state.last_status = Some("skipped".to_owned());
            state.last_message = Some("账号类型已改变，请关闭唤醒计划".to_owned());
            self.save_state(&record, state).await?;
            return Ok(());
        }
        let mut ready = due;
        if !scheduled {
            let targets = record.config.reset_targets(&item.quota);
            if state.reset_targets.is_empty() {
                state.reset_targets = targets;
                ready = false;
            } else if due {
                // 必须收到重置之后的新观测，且目标窗口明确还有剩余额度。
                ready = item
                    .quota
                    .observed_at
                    .is_some_and(|at| at.timestamp() >= now.timestamp() - 60)
                    && state
                        .due_reset(now.timestamp())
                        .is_some_and(|(key, reset)| {
                            item.quota
                                .observed_at
                                .is_some_and(|at| at.timestamp() >= reset)
                                && item.quota.windows.iter().any(|w| {
                                    w.key == key
                                        && !w.limit_reached
                                        && w.used_percent.is_some_and(|used| {
                                            used.is_finite() && (0.0..100.0).contains(&used)
                                        })
                                })
                        });
            } else {
                // 上游可能在宽限期内先滚动窗口，保留尚未消费的旧重置时间。
                for (key, reset) in targets {
                    let consumed = state.reset_targets.get(&key).is_none_or(|old| {
                        state
                            .consumed_resets
                            .get(&key)
                            .is_some_and(|value| value >= old)
                    });
                    if consumed {
                        state.reset_targets.insert(key, reset);
                    }
                }
            }
        }
        if !ready {
            self.save_state(&record, state).await?;
            return Ok(());
        }
        if scheduled {
            state.last_trigger = state.next_run_at.map(|at| format!("schedule:{at}"));
            state.next_run_at = Some(record.config.next_after(now)?);
            state.check_after = state.next_run_at.unwrap_or(state.check_after);
        }
        if !item.account.enabled
            || item.projection.status != AccountStatus::Normal
            || !state.can_attempt(now.timestamp())
        {
            state.last_status = Some("skipped".to_owned());
            state.last_message = Some("账号暂不可用或距离上次执行不足 5 分钟".to_owned());
            self.save_state(&record, state).await?;
            return Ok(());
        }
        if !scheduled {
            // 同时到期的五小时与周窗口合并成一次请求，分别记录消费游标。
            for (key, reset) in &state.reset_targets {
                if reset.saturating_add(120) <= now.timestamp() {
                    state.consumed_resets.insert(key.clone(), *reset);
                }
            }
            state.last_trigger = Some(format!("reset:{}", now.timestamp()));
            state.reset_targets = record.config.reset_targets(&item.quota);
        }
        state.last_attempt_at = Some(now.timestamp());
        state.last_status = Some("claimed".to_owned());
        state.last_message = Some("已领取执行；中断时不会补发此请求".to_owned());
        if !self.save_state(&record, state.clone()).await? {
            return Ok(());
        }
        let claimed = AutoWakeRecord {
            state: state.clone(),
            ..record
        };
        let model = UpstreamModelId::new(claimed.config.model.clone())
            .map_err(|_| AdminError::invalid("模型不合法"))?;
        let run = async {
            let mut events = self.accounts.test_connection(id, model).await?;
            while let Some(event) = events.next().await {
                match event {
                    AccountConnectionTestEvent::Completed => return Ok(true),
                    AccountConnectionTestEvent::Failed { .. } => return Ok(false),
                    _ => {}
                }
            }
            Ok::<bool, AdminError>(false)
        };
        let succeeded = matches!(
            tokio::time::timeout(Duration::from_secs(90), run).await,
            Ok(Ok(true))
        );
        state.last_status = Some(if succeeded { "succeeded" } else { "failed" }.to_owned());
        state.last_message = Some(
            if succeeded {
                "最小推理请求完成"
            } else {
                "请求失败或结果不确定，本次不重试"
            }
            .to_owned(),
        );
        self.save_state(&claimed, state).await?;
        Ok(())
    }
}

#[async_trait]
impl AutoWakeService for DefaultAutoWakeService {
    async fn get(&self, id: ProviderAccountId) -> Result<AutoWakeRecord, AdminError> {
        self.require_account(&id).await?;
        self.load(&id).await
    }

    async fn configure(
        &self,
        id: ProviderAccountId,
        config: AutoWakeConfig,
        context: &MutationContext,
    ) -> Result<AutoWakeRecord, AdminError> {
        let now = Utc::now();
        config.validate(now)?;
        self.require_account(&id).await?;
        if config.enabled {
            let models = self.accounts.models(&id, false).await?;
            if !models
                .models
                .iter()
                .any(|model| model.id.as_str() == config.model)
            {
                return Err(AdminError::invalid("所选模型不在账号模型目录中"));
            }
        }
        let old = self.load(&id).await?;
        let mut state = old.state;
        state.next_run_at = if config.enabled && config.trigger == AutoWakeTrigger::Scheduled {
            Some(config.next_after(now)?)
        } else {
            None
        };
        state.reset_targets.clear();
        state.check_after = state.next_run_at.unwrap_or(now.timestamp());
        self.repository
            .configure(id.as_str(), config, state, context)
            .await
            .map_err(|e| map_store_error(e, "auto wake"))
    }
}

pub(crate) struct AutoWakeTask(pub Arc<DefaultAutoWakeService>);

impl ScheduledTask for AutoWakeTask {
    fn run_cycle(
        &self,
        context: WorkerCycleContext,
    ) -> futures::future::BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move {
            let records = self.0.repository.ready(Utc::now().timestamp(), 20).await;
            match records {
                Ok(records) => {
                    for record in records {
                        if context.cancellation().is_cancelled() {
                            break;
                        }
                        if let Err(error) = self.0.process(record).await {
                            tracing::warn!(error = %error, "自动唤醒检查失败");
                        }
                    }
                }
                Err(error) => tracing::warn!(error = %error, "读取自动唤醒计划失败"),
            }
            Ok(())
        })
    }
}
