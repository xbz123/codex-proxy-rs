//! 自动唤醒配置使用管理员会话及现有账号 ID 校验。

use super::*;
use crate::auth::SessionState;
use gateway_admin::model::auto_wake::AutoWakeConfig;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WakeQuery {
    account_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WakeRequest {
    account_id: String,
    config: AutoWakeConfig,
}

pub(super) fn router<S: SessionState + Clone + Send + Sync + 'static>() -> Router<S> {
    Router::new().route(
        "/api/admin/accounts/auto-wake",
        get(load::<S>).post(configure::<S>),
    )
}

async fn load<S: SessionState + Send + Sync>(
    _auth: AdminAuth,
    State(state): State<S>,
    AdminQuery(query): AdminQuery<WakeQuery>,
) -> Result<impl IntoResponse, AdminError> {
    require_account_id(&query.account_id, "accountId").map_err(map_wire_error)?;
    let id = ProviderAccountId::new(query.account_id)
        .map_err(|_| map_wire_error(WireValidationError::new("accountId")))?;
    let result = state
        .admin_services()
        .auto_wake()
        .get(id)
        .await
        .map_err(map_service_error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(result),
    ))
}

async fn configure<S: SessionState + Send + Sync>(
    auth: AdminAuth,
    State(state): State<S>,
    AdminJson(request): AdminJson<WakeRequest>,
) -> Result<impl IntoResponse, AdminError> {
    require_account_id(&request.account_id, "accountId").map_err(map_wire_error)?;
    let id = ProviderAccountId::new(request.account_id)
        .map_err(|_| map_wire_error(WireValidationError::new("accountId")))?;
    let result = state
        .admin_services()
        .auto_wake()
        .configure(id, request.config, &auth.context().mutation_context())
        .await
        .map_err(map_service_error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(result),
    ))
}
