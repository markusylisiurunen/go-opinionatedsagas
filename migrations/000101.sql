-- a table for making saga step handlers idempotent
create table :SCHEMA.idempotency_keys (
  id bigserial primary key,
  key text not null unique,
  created_at timestamptz not null,
  locked_at timestamptz,
  is_processed boolean not null
);
