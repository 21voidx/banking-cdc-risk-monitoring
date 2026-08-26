CREATE TABLE cdc_events (
    event_id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    table_name TEXT NOT NULL,
    operation CHAR(1) NOT NULL,
    source_ts TIMESTAMPTZ,
    before_data JSONB,
    after_data JSONB,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(topic, partition_id, kafka_offset)
);

CREATE TABLE customers_current (
    customer_id UUID PRIMARY KEY,
    full_name TEXT NOT NULL,
    kyc_status TEXT NOT NULL,
    risk_level TEXT NOT NULL,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    cdc_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE accounts_current (
    account_id UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    account_type TEXT NOT NULL,
    balance NUMERIC(18,2) NOT NULL,
    currency CHAR(3) NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    cdc_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE beneficiaries_current (
    beneficiary_id UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    destination_account_id UUID NOT NULL,
    nickname TEXT,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ,
    cdc_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE transactions_current (
    transaction_id UUID PRIMARY KEY,
    source_account_id UUID NOT NULL,
    destination_account_id UUID NOT NULL,
    transaction_type TEXT NOT NULL,
    amount NUMERIC(18,2) NOT NULL,
    currency CHAR(3) NOT NULL,
    channel TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    cdc_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE risk_alerts (
    alert_id BIGSERIAL PRIMARY KEY,
    transaction_id UUID NOT NULL,
    source_account_id UUID NOT NULL,
    rule_code TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('MEDIUM', 'HIGH', 'CRITICAL')),
    reason TEXT NOT NULL,
    amount NUMERIC(18,2) NOT NULL,
    alert_created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(transaction_id, rule_code)
);

CREATE INDEX idx_transactions_current_created_at ON transactions_current(created_at DESC);
CREATE INDEX idx_risk_alerts_created_at ON risk_alerts(alert_created_at DESC);
CREATE INDEX idx_risk_alerts_rule_code ON risk_alerts(rule_code);
CREATE INDEX idx_beneficiaries_current_lookup ON beneficiaries_current(customer_id, destination_account_id, created_at DESC);

-- Read-only Grafana role. Dashboard queries should not use the writer account.
CREATE ROLE grafana_reader WITH LOGIN PASSWORD 'grafana_reader';
GRANT CONNECT ON DATABASE banking_analytics TO grafana_reader;
GRANT USAGE ON SCHEMA public TO grafana_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_reader;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO grafana_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO grafana_reader;
