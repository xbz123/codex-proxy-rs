-- 唤醒计划独立于凭据与运行快照，删除账号时一并清理。
create table account_auto_wake (
  account_id text primary key references provider_accounts(id) on delete cascade,
  generation bigint not null default 1 check (generation > 0),
  config jsonb not null,
  state jsonb not null,
  updated_at timestamptz not null default now()
);
create index account_auto_wake_ready_idx
  on account_auto_wake (((state->>'checkAfter')::bigint))
  where config->>'enabled' = 'true';
