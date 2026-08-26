import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from rules import evaluate_frozen_account, evaluate_high_value, evaluate_new_beneficiary


class RiskRulesTest(unittest.TestCase):
    def test_high_value_threshold(self):
        alert = evaluate_high_value(Decimal("50000000"), Decimal("50000000"))
        self.assertEqual(alert.rule_code, "HIGH_VALUE_TRANSFER")

    def test_new_beneficiary_window(self):
        now = datetime.now(timezone.utc)
        alert = evaluate_new_beneficiary(
            Decimal("25000000"), Decimal("20000000"), now - timedelta(minutes=2), 10, now
        )
        self.assertEqual(alert.rule_code, "NEW_BENEFICIARY_LARGE_TRANSFER")

    def test_frozen_account(self):
        alert = evaluate_frozen_account("FROZEN")
        self.assertEqual(alert.rule_code, "FROZEN_ACCOUNT_ACTIVITY")


if __name__ == "__main__":
    unittest.main()
