#!/usr/bin/env python3
"""
test_pipeline.py

Comprehensive end-to-end test suite for the SignalStack Paper Trading Agent:
1. Health and liveness verification
2. Account status and zero-position bootstrap initial state verification
3. Auth & validation enforcement (401, 400)
4. Hard safety limit enforcement (422)
5. Bootstrap unheld sell filter verification (graceful skip of pre-bootstrap positions)
6. New buy order execution & paper ledger verification
7. Strict idempotency duplicate protection (409)
8. Mark price update & unrealized P/L calculation
9. Exit sell order execution & realized P/L recording
10. Dashboard HTML rendering verification
"""

import sys
import time
import requests
from signal_client import SignalStackAgentClient

CLIENT = SignalStackAgentClient(base_url="http://127.0.0.1:8080", signal_secret="super-secret-trading-signal-token-2026")


def run_tests():
    print("=" * 60)
    print("🧪 Running SignalStack Agent End-to-End Test Suite")
    print("=" * 60)
    passed = 0
    total = 10

    # 1. Health check
    print("\n[Test 1] Health Check...")
    h = CLIENT.check_health()
    assert h["status"] == "healthy", f"Expected healthy, got {h}"
    assert h["broker"] == "signalstack_paper", f"Expected signalstack_paper, got {h}"
    assert h["bootstrap_mode"] is True, f"Expected bootstrap_mode True, got {h}"
    print(f"  ✅ PASS: Agent healthy (broker: {h['broker']}, bootstrap: {h['bootstrap_mode']})")
    passed += 1

    # 2. Status & Clean Bootstrap State Check
    print("\n[Test 2] Status & Starting Bootstrap State Check...")
    st = CLIENT.get_status()
    acct = st["account"]
    assert acct["status"] == "ACTIVE", f"Expected ACTIVE account, got {acct}"
    print(f"  Account: {acct['account_number']}, Cash: ${acct['cash']:,.2f}, Buying Power: ${acct['buying_power']:,.2f}")
    assert st["open_positions_count"] == 0, f"Expected 0 open positions at bootstrap start, got {st['open_positions_count']}"
    print(f"  ✅ PASS: Account active with 0 open positions (clean bootstrap starting state)")
    passed += 1

    # 3. Auth Check
    print("\n[Test 3] Authentication Check...")
    bad_resp = requests.post("http://127.0.0.1:8080/signal", json={"signal_id": "test"}, headers={"Authorization": "Bearer bad-secret"})
    assert bad_resp.status_code == 401, f"Expected 401, got {bad_resp.status_code}"
    print("  ✅ PASS: Unauthorized requests correctly blocked (HTTP 401)")
    passed += 1

    # 4. Validation Check
    print("\n[Test 4] Payload Validation...")
    res = CLIENT.send_signal(signal_id="sig-bad", symbol="SOXL", action="BUY", shares=-5, signal_price=25.0)
    assert res["status_code"] == 400, f"Expected 400, got {res['status_code']}"
    print(f"  ✅ PASS: Invalid shares blocked with HTTP 400: {res['data'].get('message')}")
    passed += 1

    # 5. Hard Safety Limit Check
    print("\n[Test 5] Hard Safety Limit Check (Excessive Buying Power)...")
    res = CLIENT.send_signal(signal_id="sig-excess", symbol="SOXL", action="BUY", shares=100000, signal_price=500.0)
    assert res["status_code"] == 422, f"Expected 422, got {res['status_code']}"
    print(f"  ✅ PASS: Safety violation blocked with HTTP 422: {res['data'].get('message')}")
    passed += 1

    # 6. Bootstrap Unheld Exit Filter Check
    print("\n[Test 6] Bootstrap Unheld Sell Filter (Graceful Skip)...")
    res = CLIENT.send_signal(
        signal_id=f"sig-exit-unheld-FAS-{int(time.time())}",
        symbol="FAS",
        action="SELL",
        shares=20,
        signal_price=110.0
    )
    assert res["status_code"] == 200, f"Expected 200, got {res['status_code']}"
    data = res["data"]
    assert data.get("status") == "skipped", f"Expected skipped, got {data}"
    assert data.get("reason") == "bootstrap_unheld_position", f"Expected bootstrap_unheld_position, got {data}"
    print(f"  ✅ PASS: Unheld pre-bootstrap exit skipped gracefully: {data.get('message')}")
    passed += 1

    # 7. New BUY Order Execution
    print("\n[Test 7] New BUY Order Execution...")
    sig_id = f"sig-buy-SOXL-{int(time.time())}"
    res = CLIENT.send_signal(
        signal_id=sig_id,
        symbol="SOXL",
        action="BUY",
        shares=10,
        signal_price=25.00
    )
    assert res["status_code"] == 200, f"Expected 200, got {res['status_code']}: {res['data']}"
    data = res["data"]
    assert data.get("order_status") == "filled", f"Expected filled, got {data}"
    assert data.get("filled_shares") == 10, f"Expected 10 shares, got {data}"
    print(f"  ✅ PASS: Order filled (Order ID: {data.get('order_id')}, Fill: 10 SOXL @ $25.00)")

    # Verify positions
    positions = CLIENT.get_positions()["positions"]
    soxl_pos = next((p for p in positions if p["symbol"] == "SOXL"), None)
    assert soxl_pos is not None, "SOXL position not found"
    assert soxl_pos["qty"] == 10, f"Expected 10 qty, got {soxl_pos['qty']}"
    assert soxl_pos["cost_basis"] == 250.0, f"Expected $250 cost basis, got {soxl_pos['cost_basis']}"
    print(f"  ✅ Position verified: 10 SOXL @ $25.00 (Cost basis: $250.00)")
    passed += 1

    # 8. Idempotency Check
    print("\n[Test 8] Idempotency Protection...")
    dup_res = CLIENT.send_signal(
        signal_id=sig_id,
        symbol="SOXL",
        action="BUY",
        shares=10,
        signal_price=25.00
    )
    assert dup_res["status_code"] == 409, f"Expected 409, got {dup_res['status_code']}"
    print(f"  ✅ PASS: Duplicate signal rejected with HTTP 409: {dup_res['data'].get('message')}")
    passed += 1

    # 9. Mark Price Update & Unrealized P/L
    print("\n[Test 9] Mark Price Update & Unrealized P/L Calculation...")
    up_res = CLIENT.update_price("SOXL", 28.50)
    assert up_res["status_code"] == 200, f"Expected 200, got {up_res}"
    positions = CLIENT.get_positions()["positions"]
    soxl_pos = next((p for p in positions if p["symbol"] == "SOXL"), None)
    assert soxl_pos["current_price"] == 28.50, f"Expected 28.50, got {soxl_pos['current_price']}"
    assert abs(soxl_pos["unrealized_pl"] - 35.0) < 0.01, f"Expected +$35.00 unrealized PL, got {soxl_pos['unrealized_pl']}"
    print(f"  ✅ PASS: Mark price updated to $28.50, Unrealized P/L: +${soxl_pos['unrealized_pl']:.2f} ({soxl_pos['unrealized_pl_pct']:+.1f}%)")
    passed += 1

    # 10. SELL Order Execution & Realized P/L
    print("\n[Test 10] SELL Order Execution & Realized P/L Check...")
    sell_sig = f"sig-exit-SOXL-{int(time.time())}"
    res = CLIENT.send_signal(
        signal_id=sell_sig,
        symbol="SOXL",
        action="SELL",
        shares=10,
        signal_price=28.50
    )
    assert res["status_code"] == 200, f"Expected 200, got {res['status_code']}: {res['data']}"
    data = res["data"]
    assert data.get("order_status") == "filled", f"Expected filled, got {data}"

    # Verify positions is now empty
    positions = CLIENT.get_positions()["positions"]
    soxl_pos = next((p for p in positions if p["symbol"] == "SOXL"), None)
    assert soxl_pos is None, "SOXL position should be closed"

    # Verify realized P/L in trades
    trades = CLIENT.get_trades()["trades"]
    soxl_trade = next((t for t in trades if t["symbol"] == "SOXL" and t["action"] == "SELL"), None)
    assert soxl_trade is not None, "Closed trade record not found"
    assert abs(soxl_trade["realized_pl"] - 35.0) < 0.01, f"Expected +$35.00 realized PL, got {soxl_trade['realized_pl']}"
    print(f"  ✅ PASS: SOXL closed completely. Realized P/L recorded: +${soxl_trade['realized_pl']:.2f}")

    # Verify Dashboard HTML
    html_resp = requests.get("http://127.0.0.1:8080/", headers={"Accept": "text/html"})
    assert html_resp.status_code == 200
    assert "SignalStack Paper Trading Agent" in html_resp.text
    assert "BOOTSTRAP ACTIVE" in html_resp.text
    print("  ✅ PASS: Dashboard HTML rendering verified")
    passed += 1

    print("\n" + "=" * 60)
    print(f"🎉 ALL TESTS PASSED ({passed}/{total + 1})")
    print("=" * 60)


if __name__ == "__main__":
    run_tests()
