import argparse
import os
import random
import time
import uuid
from decimal import Decimal

import psycopg


SOURCE_DSN = os.getenv(
    "SOURCE_DSN",
    "postgresql://postgres:postgres@source-postgres:5432/core_banking",
)
CHANNELS = ["MOBILE", "INTERNET_BANKING", "ATM", "TELLER"]


def connect_with_retry():
    while True:
        try:
            return psycopg.connect(SOURCE_DSN)
        except Exception as exc:
            print(f"source database not ready: {exc}")
            time.sleep(2)


def list_accounts(conn, only_active=True):
    with conn.cursor() as cur:
        if only_active:
            cur.execute(
                "SELECT account_id, customer_id, balance FROM accounts WHERE status = 'ACTIVE' ORDER BY account_id"
            )
        else:
            cur.execute("SELECT account_id, customer_id, balance FROM accounts ORDER BY account_id")
        return cur.fetchall()


def settle_transfer(conn, transaction_id, source_account_id, destination_account_id, amount):
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE transactions
                SET status = 'SUCCESS', updated_at = NOW()
                WHERE transaction_id = %s
                """,
                (transaction_id,),
            )
            cur.execute(
                "UPDATE accounts SET balance = balance - %s, updated_at = NOW() WHERE account_id = %s",
                (amount, source_account_id),
            )
            cur.execute(
                "UPDATE accounts SET balance = balance + %s, updated_at = NOW() WHERE account_id = %s",
                (amount, destination_account_id),
            )


def create_pending_transfer(conn, source_account_id, destination_account_id, amount, channel=None):
    transaction_id = str(uuid.uuid4())
    channel = channel or random.choice(CHANNELS)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO transactions (
                transaction_id, source_account_id, destination_account_id,
                transaction_type, amount, currency, channel, status
            ) VALUES (%s, %s, %s, 'TRANSFER', %s, 'IDR', %s, 'PENDING')
            """,
            (transaction_id, source_account_id, destination_account_id, amount, channel),
        )
    conn.commit()
    print(
        f"created PENDING transaction={transaction_id} source={source_account_id} "
        f"destination={destination_account_id} amount={amount} channel={channel}"
    )
    return transaction_id


def normal(conn):
    accounts = list_accounts(conn)
    if len(accounts) < 2:
        raise RuntimeError("Need at least two ACTIVE accounts")
    source_candidates = [row for row in accounts if Decimal(row[2]) >= Decimal("1000000")]
    if not source_candidates:
        raise RuntimeError("No ACTIVE account has enough balance for a normal demo transfer")
    source = random.choice(source_candidates)
    destination = random.choice([row for row in accounts if row[0] != source[0]])
    max_safe = min(Decimal("5000000"), Decimal(source[2]) / 4)
    amount = Decimal(random.randint(200_000, int(max_safe)))
    txid = create_pending_transfer(conn, source[0], destination[0], amount)
    time.sleep(0.8)
    settle_transfer(conn, txid, source[0], destination[0], amount)
    print(f"settled SUCCESS transaction={txid}")


def suspicious(conn):
    accounts = list_accounts(conn)
    accounts_by_balance = sorted(accounts, key=lambda row: Decimal(row[2]), reverse=True)
    source = accounts_by_balance[0]
    destination = next(row for row in accounts_by_balance[1:] if row[0] != source[0])
    beneficiary_id = str(uuid.uuid4())

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO beneficiaries (
                beneficiary_id, customer_id, destination_account_id, nickname, status
            ) VALUES (%s, %s, %s, 'New beneficiary demo', 'ACTIVE')
            """,
            (beneficiary_id, source[1], destination[0]),
        )
    conn.commit()
    print(
        f"created NEW BENEFICIARY beneficiary={beneficiary_id} "
        f"customer={source[1]} destination={destination[0]}"
    )
    print("waiting 2 seconds so the beneficiary CDC event reaches downstream state...")
    time.sleep(2)

    amount = Decimal("75000000")
    txid = create_pending_transfer(conn, source[0], destination[0], amount, "MOBILE")
    time.sleep(1)
    settle_transfer(conn, txid, source[0], destination[0], amount)
    print(f"settled SUCCESS suspicious transaction={txid}")
    print("expected alerts: HIGH_VALUE_TRANSFER + NEW_BENEFICIARY_LARGE_TRANSFER")


def frozen(conn):
    accounts = list_accounts(conn)
    source, destination = accounts[2], accounts[3]

    with conn.cursor() as cur:
        cur.execute(
            "UPDATE accounts SET status = 'FROZEN', updated_at = NOW() WHERE account_id = %s",
            (source[0],),
        )
    conn.commit()
    print(f"froze account={source[0]}")
    print("waiting 2 seconds so the account CDC update reaches downstream state...")
    time.sleep(2)

    amount = Decimal("1500000")
    txid = create_pending_transfer(conn, source[0], destination[0], amount, "INTERNET_BANKING")
    time.sleep(0.8)
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE transactions SET status = 'FAILED', updated_at = NOW() WHERE transaction_id = %s",
            (txid,),
        )
    conn.commit()
    print(f"marked FAILED transaction={txid}")
    print("expected alert: FROZEN_ACCOUNT_ACTIVITY")

    # Restore source account so later demos can continue.
    time.sleep(1)
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE accounts SET status = 'ACTIVE', updated_at = NOW() WHERE account_id = %s",
            (source[0],),
        )
    conn.commit()
    print(f"restored account={source[0]} to ACTIVE")


def stream(conn, interval, risk_every):
    counter = 0
    print("continuous generator started; press Ctrl+C to stop")
    while True:
        counter += 1
        try:
            if risk_every > 0 and counter % risk_every == 0:
                suspicious(conn)
            else:
                normal(conn)
            time.sleep(interval)
        except psycopg.errors.CheckViolation as exc:
            conn.rollback()
            print(f"transaction skipped due to constraint: {exc}")
        except psycopg.errors.ForeignKeyViolation as exc:
            conn.rollback()
            print(f"transaction skipped due to FK: {exc}")


def main():
    parser = argparse.ArgumentParser(description="Dummy banking source activity generator")
    sub = parser.add_subparsers(dest="scenario", required=True)
    sub.add_parser("normal", help="Create one ordinary transfer")
    sub.add_parser("suspicious", help="Create a new beneficiary followed by a Rp75M transfer")
    sub.add_parser("frozen", help="Attempt a transfer from a temporarily frozen account")
    stream_parser = sub.add_parser("stream", help="Continuously create dummy transactions")
    stream_parser.add_argument("--interval", type=float, default=2.0, help="Seconds between scenarios")
    stream_parser.add_argument(
        "--risk-every",
        type=int,
        default=0,
        help="Run suspicious scenario every N transactions; 0 means normal-only",
    )
    args = parser.parse_args()

    conn = connect_with_retry()
    try:
        if args.scenario == "normal":
            normal(conn)
        elif args.scenario == "suspicious":
            suspicious(conn)
        elif args.scenario == "frozen":
            frozen(conn)
        elif args.scenario == "stream":
            stream(conn, args.interval, args.risk_every)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
