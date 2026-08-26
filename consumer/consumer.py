import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal

import psycopg
from confluent_kafka import Consumer, KafkaError, KafkaException

from rules import evaluate_frozen_account, evaluate_high_value, evaluate_new_beneficiary


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
LOG = logging.getLogger("banking-risk-consumer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "banking-risk-consumer-v1")
ANALYTICS_DSN = os.getenv(
    "ANALYTICS_DSN",
    "postgresql://analytics_app:analytics_dev@analytics-postgres:5432/banking_analytics",
)
HIGH_VALUE_THRESHOLD = Decimal(os.getenv("HIGH_VALUE_THRESHOLD", "50000000"))
NEW_BENEFICIARY_THRESHOLD = Decimal(os.getenv("NEW_BENEFICIARY_THRESHOLD", "20000000"))
NEW_BENEFICIARY_WINDOW_MINUTES = int(os.getenv("NEW_BENEFICIARY_WINDOW_MINUTES", "10"))

TOPICS = [
    "banking.public.customers",
    "banking.public.accounts",
    "banking.public.beneficiaries",
    "banking.public.transactions",
]

RUNNING = True


def stop_handler(signum, frame):
    global RUNNING
    LOG.info("Received signal %s; stopping after current message", signum)
    RUNNING = False


def wait_for_postgres():
    while RUNNING:
        try:
            conn = psycopg.connect(ANALYTICS_DSN)
            conn.close()
            return
        except Exception as exc:
            LOG.warning("Analytics PostgreSQL not ready: %s", exc)
            time.sleep(2)
    raise RuntimeError("Stopped before PostgreSQL became ready")


def to_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric > 1e14:  # microseconds
            return datetime.fromtimestamp(numeric / 1_000_000, tz=timezone.utc)
        if numeric > 1e11:  # milliseconds
            return datetime.fromtimestamp(numeric / 1_000, tz=timezone.utc)
        return datetime.fromtimestamp(numeric, tz=timezone.utc)
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    raise ValueError(f"Unsupported timestamp value: {value!r}")


def record_cdc_event(cur, msg, payload, table_name):
    event_id = f"{msg.topic()}:{msg.partition()}:{msg.offset()}"
    before = payload.get("before")
    after = payload.get("after")
    source = payload.get("source") or {}
    source_ts = to_datetime(source.get("ts_ms"))

    cur.execute(
        """
        INSERT INTO cdc_events (
            event_id, topic, partition_id, kafka_offset, table_name,
            operation, source_ts, before_data, after_data
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
        ON CONFLICT (event_id) DO NOTHING
        RETURNING event_id
        """,
        (
            event_id,
            msg.topic(),
            msg.partition(),
            msg.offset(),
            table_name,
            payload.get("op"),
            source_ts,
            json.dumps(before) if before is not None else None,
            json.dumps(after) if after is not None else None,
        ),
    )
    return cur.fetchone() is not None


def upsert_customer(cur, data):
    cur.execute(
        """
        INSERT INTO customers_current (
            customer_id, full_name, kyc_status, risk_level, created_at, updated_at, cdc_updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (customer_id) DO UPDATE SET
            full_name = EXCLUDED.full_name,
            kyc_status = EXCLUDED.kyc_status,
            risk_level = EXCLUDED.risk_level,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at,
            cdc_updated_at = NOW()
        """,
        (
            data["customer_id"], data["full_name"], data["kyc_status"], data["risk_level"],
            to_datetime(data.get("created_at")), to_datetime(data.get("updated_at")),
        ),
    )


def upsert_account(cur, data):
    cur.execute(
        """
        INSERT INTO accounts_current (
            account_id, customer_id, account_type, balance, currency, status,
            created_at, updated_at, cdc_updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (account_id) DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            account_type = EXCLUDED.account_type,
            balance = EXCLUDED.balance,
            currency = EXCLUDED.currency,
            status = EXCLUDED.status,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at,
            cdc_updated_at = NOW()
        """,
        (
            data["account_id"], data["customer_id"], data["account_type"], Decimal(str(data["balance"])),
            data["currency"], data["status"], to_datetime(data.get("created_at")),
            to_datetime(data.get("updated_at")),
        ),
    )


def upsert_beneficiary(cur, data):
    cur.execute(
        """
        INSERT INTO beneficiaries_current (
            beneficiary_id, customer_id, destination_account_id, nickname,
            status, created_at, cdc_updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (beneficiary_id) DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            destination_account_id = EXCLUDED.destination_account_id,
            nickname = EXCLUDED.nickname,
            status = EXCLUDED.status,
            created_at = EXCLUDED.created_at,
            cdc_updated_at = NOW()
        """,
        (
            data["beneficiary_id"], data["customer_id"], data["destination_account_id"],
            data.get("nickname"), data["status"], to_datetime(data.get("created_at")),
        ),
    )


def upsert_transaction(cur, data):
    cur.execute(
        """
        INSERT INTO transactions_current (
            transaction_id, source_account_id, destination_account_id,
            transaction_type, amount, currency, channel, status,
            created_at, updated_at, cdc_updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (transaction_id) DO UPDATE SET
            source_account_id = EXCLUDED.source_account_id,
            destination_account_id = EXCLUDED.destination_account_id,
            transaction_type = EXCLUDED.transaction_type,
            amount = EXCLUDED.amount,
            currency = EXCLUDED.currency,
            channel = EXCLUDED.channel,
            status = EXCLUDED.status,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at,
            cdc_updated_at = NOW()
        """,
        (
            data["transaction_id"], data["source_account_id"], data["destination_account_id"],
            data["transaction_type"], Decimal(str(data["amount"])), data["currency"],
            data["channel"], data["status"], to_datetime(data.get("created_at")),
            to_datetime(data.get("updated_at")),
        ),
    )


def delete_current_row(cur, table_name, before):
    if not before:
        return
    pk_by_table = {
        "customers": ("customer_id", "customers_current"),
        "accounts": ("account_id", "accounts_current"),
        "beneficiaries": ("beneficiary_id", "beneficiaries_current"),
        "transactions": ("transaction_id", "transactions_current"),
    }
    pk_field, target_table = pk_by_table[table_name]
    cur.execute(f"DELETE FROM {target_table} WHERE {pk_field} = %s", (before[pk_field],))


def evaluate_transaction_risk(cur, tx):
    amount = Decimal(str(tx["amount"]))
    source_account_id = tx["source_account_id"]
    destination_account_id = tx["destination_account_id"]

    cur.execute(
        "SELECT customer_id, status FROM accounts_current WHERE account_id = %s",
        (source_account_id,),
    )
    account = cur.fetchone()
    customer_id = account[0] if account else None
    account_status = account[1] if account else None

    beneficiary_created_at = None
    if customer_id:
        cur.execute(
            """
            SELECT created_at
            FROM beneficiaries_current
            WHERE customer_id = %s
              AND destination_account_id = %s
              AND status = 'ACTIVE'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (customer_id, destination_account_id),
        )
        row = cur.fetchone()
        beneficiary_created_at = row[0] if row else None

    alerts = [
        evaluate_high_value(amount, HIGH_VALUE_THRESHOLD),
        evaluate_new_beneficiary(
            amount,
            NEW_BENEFICIARY_THRESHOLD,
            beneficiary_created_at,
            NEW_BENEFICIARY_WINDOW_MINUTES,
        ),
        evaluate_frozen_account(account_status),
    ]

    for alert in (a for a in alerts if a is not None):
        cur.execute(
            """
            INSERT INTO risk_alerts (
                transaction_id, source_account_id, rule_code, severity, reason, amount
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id, rule_code) DO NOTHING
            """,
            (
                tx["transaction_id"], source_account_id, alert.rule_code,
                alert.severity, alert.reason, amount,
            ),
        )
        if cur.rowcount:
            LOG.warning(
                "RISK ALERT transaction=%s rule=%s severity=%s amount=%s",
                tx["transaction_id"], alert.rule_code, alert.severity, amount,
            )


def apply_event(cur, table_name, payload):
    op = payload.get("op")
    before = payload.get("before")
    after = payload.get("after")

    if op == "d":
        delete_current_row(cur, table_name, before)
        return

    if after is None:
        return

    handlers = {
        "customers": upsert_customer,
        "accounts": upsert_account,
        "beneficiaries": upsert_beneficiary,
        "transactions": upsert_transaction,
    }
    handlers[table_name](cur, after)

    # Alert only on newly-created live transactions. Snapshot reads (op='r')
    # populate current state but do not produce historical alerts.
    if table_name == "transactions" and op == "c":
        evaluate_transaction_risk(cur, after)


def main():
    signal.signal(signal.SIGTERM, stop_handler)
    signal.signal(signal.SIGINT, stop_handler)

    wait_for_postgres()
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe(TOPICS)
    LOG.info("Subscribed to topics: %s", ", ".join(TOPICS))

    conn = psycopg.connect(ANALYTICS_DSN)
    try:
        while RUNNING:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            if msg.value() is None:
                consumer.commit(message=msg, asynchronous=False)
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                table_name = msg.topic().split(".")[-1]
                with conn.cursor() as cur:
                    is_new = record_cdc_event(cur, msg, payload, table_name)
                    if is_new:
                        apply_event(cur, table_name, payload)
                conn.commit()
                consumer.commit(message=msg, asynchronous=False)

                if is_new:
                    LOG.info(
                        "Processed %s op=%s partition=%s offset=%s",
                        table_name, payload.get("op"), msg.partition(), msg.offset(),
                    )
            except Exception:
                conn.rollback()
                LOG.exception(
                    "Failed topic=%s partition=%s offset=%s; Kafka offset not committed",
                    msg.topic(), msg.partition(), msg.offset(),
                )
                time.sleep(1)
    finally:
        conn.close()
        consumer.close()
        LOG.info("Consumer stopped")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
