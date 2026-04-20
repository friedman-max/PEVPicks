-- 1. Add user_id column
alter table slips add column user_id uuid references auth.users(id) on delete cascade;
alter table legs  add column user_id uuid references auth.users(id) on delete cascade;

-- 2. New table: per-user runtime config
create table user_config (
  user_id         uuid primary key references auth.users(id) on delete cascade,
  min_ev_pct      numeric default 0.01,
  active_leagues  jsonb   default '{"NBA":true,"MLB":true,"NHL":true,"NCAAB":true}'::jsonb,
  refresh_interval_min int default 15,
  auto_backtest   boolean default false,
  created_at      timestamptz default now(),
  updated_at      timestamptz default now()
);

-- 3. Indexes — every per-user query filters on user_id
create index idx_slips_user  on slips(user_id, timestamp desc);
create index idx_legs_user   on legs(user_id, slip_id);

-- 4. Enable Row-Level Security
alter table slips enable row level security;
alter table legs  enable row level security;
alter table user_config enable row level security;

-- 5. RLS Policies
create policy "slips_owner" on slips
  for all using (user_id = auth.uid()) with check (user_id = auth.uid());

create policy "legs_owner" on legs
  for all using (user_id = auth.uid()) with check (user_id = auth.uid());

create policy "user_config_owner" on user_config
  for all using (user_id = auth.uid()) with check (user_id = auth.uid());

-- 6. IMPORTANT: After verifying the user_id columns and backfilling data,
-- you should make them NOT NULL by running:
-- alter table slips alter column user_id set not null;
-- alter table legs alter column user_id set not null;
