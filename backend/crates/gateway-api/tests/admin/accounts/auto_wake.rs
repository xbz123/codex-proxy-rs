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
pub(crate) struct MemoryAutoWake(Mutex<BTreeMap<String, AutoWakeRecord>>);

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

#[tokio::test]
async fn wake_routes_require_admin_and_reject_invalid_input() {
    use super::super::{AdminTestFixture, AdminTestState};
    use axum::{
        body::Body,
        http::{Request, StatusCode, header},
    };
    use tower::ServiceExt as _;
    let fixture = AdminTestFixture::new().await;
    fixture.auth.insert_session("valid-session");
    for (method, uri, body, authenticated, expected) in [
        (
            "GET",
            "/api/admin/accounts/auto-wake?accountId=acct_test",
            "",
            false,
            StatusCode::UNAUTHORIZED,
        ),
        (
            "POST",
            "/api/admin/accounts/auto-wake",
            "{}",
            false,
            StatusCode::UNAUTHORIZED,
        ),
        (
            "GET",
            "/api/admin/accounts/auto-wake?accountId=bad",
            "",
            true,
            StatusCode::BAD_REQUEST,
        ),
        (
            "GET",
            "/api/admin/accounts/auto-wake?accountId=acct_test&extra=1",
            "",
            true,
            StatusCode::BAD_REQUEST,
        ),
        (
            "POST",
            "/api/admin/accounts/auto-wake",
            r#"{"accountId":"acct_test","config":{"enabled":true,"trigger":"scheduled","model":"test","cron":"invalid","timezone":"UTC"}}"#,
            true,
            StatusCode::BAD_REQUEST,
        ),
        (
            "POST",
            "/api/admin/accounts/auto-wake",
            r#"{"accountId":"acct_test","config":{"enabled":true,"trigger":"scheduled","model":"test","cron":"* * * * *","timezone":"UTC","extra":1}}"#,
            true,
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
    ] {
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .header("x-request-id", "req_wake");
        if authenticated {
            request = request.header(header::COOKIE, "cpr_session=valid-session");
        }
        let response = gateway_api::admin::router::<AdminTestState>()
            .with_state(fixture.state())
            .oneshot(request.body(Body::from(body)).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), expected, "{method} {uri}");
    }
}
