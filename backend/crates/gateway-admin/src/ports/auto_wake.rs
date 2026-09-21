//! 账号唤醒计划及运行游标的持久化端口。

use async_trait::async_trait;

use super::store::AdminStoreResult;
use crate::model::{
    MutationContext,
    auto_wake::{AutoWakeConfig, AutoWakeRecord, AutoWakeState},
};

#[async_trait]
pub trait AutoWakeRepository: Send + Sync {
    async fn load(&self, account_id: &str) -> AdminStoreResult<Option<AutoWakeRecord>>;

    /// 按检查时间排序分页，避免错误账号占用全部循环。
    async fn ready(&self, now: i64, limit: u32) -> AdminStoreResult<Vec<AutoWakeRecord>>;

    /// 配置、代次、初始游标和脱敏审计在同一事务提交。
    async fn configure(
        &self,
        account_id: &str,
        config: AutoWakeConfig,
        state: AutoWakeState,
        context: &MutationContext,
    ) -> AdminStoreResult<AutoWakeRecord>;

    /// 同时比较配置代次及旧游标；陈旧工作不能覆盖新设置或重复领取。
    async fn compare_exchange(
        &self,
        expected: &AutoWakeRecord,
        state: AutoWakeState,
    ) -> AdminStoreResult<bool>;
}
