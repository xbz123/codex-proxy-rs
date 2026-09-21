//! 自动唤醒的配置与持久化游标；额度事实仍由 Provider 拥有。

use std::{collections::BTreeMap, str::FromStr as _};

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use cron::Schedule;
use gateway_core::routing::UpstreamModelId;
use serde::{Deserialize, Serialize};

use super::{AdminError, provider_credentials::ProviderQuota};

/// 单次唤醒发送固定的最小连接测试消息。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AutoWakeConfig {
    pub enabled: bool,
    pub trigger: AutoWakeTrigger,
    pub model: String,
    pub cron: String,
    pub timezone: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AutoWakeTrigger {
    Scheduled,
    FiveHourReset,
    WeeklyReset,
    EitherReset,
}

/// Unix 秒用于持久化与接口，不依赖服务器本地时区。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AutoWakeState {
    pub next_run_at: Option<i64>,
    pub reset_targets: BTreeMap<String, i64>,
    pub consumed_resets: BTreeMap<String, i64>,
    pub last_trigger: Option<String>,
    pub last_attempt_at: Option<i64>,
    pub last_status: Option<String>,
    pub last_message: Option<String>,
    pub check_after: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutoWakeRecord {
    pub account_id: String,
    pub generation: i64,
    pub config: AutoWakeConfig,
    pub state: AutoWakeState,
}

impl Default for AutoWakeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            trigger: AutoWakeTrigger::Scheduled,
            model: String::new(),
            cron: "0 8,13,18 * * *".to_owned(),
            timezone: "Asia/Shanghai".to_owned(),
        }
    }
}

impl AutoWakeConfig {
    /// 启用前验证模型、五字段 Cron 与 IANA 时区。
    pub fn validate(&self, now: DateTime<Utc>) -> Result<(), AdminError> {
        if self.model.len() > 128 || self.model.chars().any(char::is_whitespace) {
            return Err(AdminError::invalid("唤醒模型不合法"));
        }
        if self.enabled {
            UpstreamModelId::new(self.model.clone())
                .map_err(|_| AdminError::invalid("请选择唤醒模型"))?;
        }
        if self.trigger == AutoWakeTrigger::Scheduled {
            self.next_after(now)?;
        }
        Ok(())
    }

    /// 严格晚于给定时刻，避免重启补发已错过的多个计划。
    pub fn next_after(&self, now: DateTime<Utc>) -> Result<i64, AdminError> {
        if self.cron.len() > 128 || self.cron.split_whitespace().count() != 5 {
            return Err(AdminError::invalid("Cron 需要五个字段：分 时 日 月 周"));
        }
        let timezone = Tz::from_str(&self.timezone)
            .map_err(|_| AdminError::invalid("请输入有效的 IANA 时区"))?;
        let schedule = Schedule::from_str(&format!("0 {}", self.cron))
            .map_err(|_| AdminError::invalid("Cron 表达式不合法"))?;
        schedule
            .after(&now.with_timezone(&timezone))
            .next()
            .map(|next| next.timestamp())
            .ok_or_else(|| AdminError::invalid("Cron 没有下一次执行时间"))
    }

    /// 只记录符合所选周期的明确重置时间，不推测缺失窗口。
    #[must_use]
    pub fn reset_targets(&self, quota: &ProviderQuota) -> BTreeMap<String, i64> {
        quota
            .windows
            .iter()
            .filter_map(|window| {
                let selected = matches!(
                    (self.trigger, window.window_seconds),
                    (
                        AutoWakeTrigger::FiveHourReset | AutoWakeTrigger::EitherReset,
                        Some(18_000),
                    ) | (
                        AutoWakeTrigger::WeeklyReset | AutoWakeTrigger::EitherReset,
                        Some(604_800),
                    )
                );
                (selected
                    && window.local_usage_attribution
                        == super::provider_credentials::QuotaLocalUsageAttribution::AccountWide)
                    .then_some(window.reset_at)
                    .flatten()
                    .map(|at| (window.key.clone(), at.timestamp()))
            })
            .collect()
    }
}

impl AutoWakeState {
    /// 到期只表示可以开始复核；真正发送还必须取得上游可用额度的证据。
    #[must_use]
    pub fn due_reset(&self, now: i64) -> Option<(&str, i64)> {
        self.reset_targets
            .iter()
            .filter(|(_, reset)| reset.saturating_add(120) <= now)
            .filter(|(key, reset)| {
                self.consumed_resets
                    .get(*key)
                    .is_none_or(|consumed| consumed < *reset)
            })
            .min_by_key(|(_, reset)| *reset)
            .map(|(key, reset)| (key.as_str(), *reset))
    }

    /// 请求发送结果不明确时也保留领取记录，避免进程重启重复消耗额度。
    #[must_use]
    pub fn can_attempt(&self, now: i64) -> bool {
        self.last_attempt_at
            .is_none_or(|last| now.saturating_sub(last) >= 300)
    }
}
