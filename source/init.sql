-- Local/demo-only source database initialization.
-- PostgreSQL is started with wal_level=logical from docker-compose.yml.

CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';
GRANT CONNECT ON DATABASE core_banking TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;

CREATE TABLE customers (
    customer_id UUID PRIMARY KEY,
    full_name TEXT NOT NULL,
    kyc_status TEXT NOT NULL CHECK (kyc_status IN ('PENDING', 'VERIFIED', 'REJECTED')),
    risk_level TEXT NOT NULL CHECK (risk_level IN ('LOW', 'MEDIUM', 'HIGH')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE accounts (
    account_id UUID PRIMARY KEY,
    customer_id UUID NOT NULL REFERENCES customers(customer_id),
    account_type TEXT NOT NULL CHECK (account_type IN ('SAVINGS', 'CURRENT')),
    balance NUMERIC(18,2) NOT NULL CHECK (balance >= 0),
    currency CHAR(3) NOT NULL DEFAULT 'IDR',
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'FROZEN', 'CLOSED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE beneficiaries (
    beneficiary_id UUID PRIMARY KEY,
    customer_id UUID NOT NULL REFERENCES customers(customer_id),
    destination_account_id UUID NOT NULL REFERENCES accounts(account_id),
    nickname TEXT,
    status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'INACTIVE')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE transactions (
    transaction_id UUID PRIMARY KEY,
    source_account_id UUID NOT NULL REFERENCES accounts(account_id),
    destination_account_id UUID NOT NULL REFERENCES accounts(account_id),
    transaction_type TEXT NOT NULL CHECK (transaction_type IN ('TRANSFER', 'PAYMENT')),
    amount NUMERIC(18,2) NOT NULL CHECK (amount > 0),
    currency CHAR(3) NOT NULL DEFAULT 'IDR',
    channel TEXT NOT NULL CHECK (channel IN ('MOBILE', 'ATM', 'INTERNET_BANKING', 'TELLER')),
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'PROCESSING', 'SUCCESS', 'FAILED', 'REVERSED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_accounts_customer_id ON accounts(customer_id);
CREATE INDEX idx_beneficiaries_customer_destination ON beneficiaries(customer_id, destination_account_id);
CREATE INDEX idx_transactions_created_at ON transactions(created_at);
CREATE INDEX idx_transactions_source_account ON transactions(source_account_id);

-- Full before-images make the demo easier to inspect. This increases WAL volume,
-- so it should not be enabled indiscriminately in production.
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE accounts REPLICA IDENTITY FULL;
ALTER TABLE beneficiaries REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

-- Create publication explicitly so the Debezium role does not need table ownership
-- or broad DDL privileges just to auto-create it.
CREATE PUBLICATION banking_publication FOR TABLE
    customers,
    accounts,
    beneficiaries,
    transactions;

-- Deterministic dummy customers/accounts. There are deliberately no initial
-- transactions, so live transaction alerts are easy to observe after startup.
INSERT INTO customers (customer_id, full_name, kyc_status, risk_level) VALUES
('00000000-0000-0000-0000-000000000001', 'Andi Wijaya', 'VERIFIED', 'LOW'),
('00000000-0000-0000-0000-000000000002', 'Budi Santoso', 'VERIFIED', 'LOW'),
('00000000-0000-0000-0000-000000000003', 'Citra Lestari', 'VERIFIED', 'MEDIUM'),
('00000000-0000-0000-0000-000000000004', 'Dewi Anggraini', 'VERIFIED', 'LOW'),
('00000000-0000-0000-0000-000000000005', 'Eko Prasetyo', 'VERIFIED', 'HIGH'),
('00000000-0000-0000-0000-000000000006', 'Farah Putri', 'VERIFIED', 'LOW');

INSERT INTO accounts (account_id, customer_id, account_type, balance, status) VALUES
('10000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000001', 'SAVINGS', 250000000.00, 'ACTIVE'),
('10000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000002', 'SAVINGS', 180000000.00, 'ACTIVE'),
('10000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000003', 'CURRENT', 300000000.00, 'ACTIVE'),
('10000000-0000-0000-0000-000000000004', '00000000-0000-0000-0000-000000000004', 'SAVINGS', 150000000.00, 'ACTIVE'),
('10000000-0000-0000-0000-000000000005', '00000000-0000-0000-0000-000000000005', 'CURRENT', 400000000.00, 'ACTIVE'),
('10000000-0000-0000-0000-000000000006', '00000000-0000-0000-0000-000000000006', 'SAVINGS', 220000000.00, 'ACTIVE');
