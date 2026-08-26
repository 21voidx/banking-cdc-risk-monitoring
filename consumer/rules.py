from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional


@dataclass(frozen=True)
class RiskAlert:
    rule_code: str
    severity: str
    reason: str


def evaluate_high_value(amount: Decimal, threshold: Decimal) -> Optional[RiskAlert]:
    if amount >= threshold:
        return RiskAlert(
            rule_code="HIGH_VALUE_TRANSFER",
            severity="HIGH",
            reason=f"Transaction amount {amount} is at or above threshold {threshold}",
        )
    return None


def evaluate_new_beneficiary(
    amount: Decimal,
    threshold: Decimal,
    beneficiary_created_at: Optional[datetime],
    window_minutes: int,
    now: Optional[datetime] = None,
) -> Optional[RiskAlert]:
    if beneficiary_created_at is None or amount < threshold:
        return None

    now = now or datetime.now(timezone.utc)
    if beneficiary_created_at.tzinfo is None:
        beneficiary_created_at = beneficiary_created_at.replace(tzinfo=timezone.utc)

    if now - beneficiary_created_at <= timedelta(minutes=window_minutes):
        return RiskAlert(
            rule_code="NEW_BENEFICIARY_LARGE_TRANSFER",
            severity="CRITICAL",
            reason=(
                f"Large transfer {amount} sent to beneficiary created within "
                f"the last {window_minutes} minutes"
            ),
        )
    return None


def evaluate_frozen_account(account_status: Optional[str]) -> Optional[RiskAlert]:
    if account_status == "FROZEN":
        return RiskAlert(
            rule_code="FROZEN_ACCOUNT_ACTIVITY",
            severity="CRITICAL",
            reason="A new transaction was attempted from an account currently marked FROZEN",
        )
    return None
