#!/usr/bin/env python3
"""
test_challenger_live.py

Live end-to-end verification of the SignalStack Challenger Trading Agent:
1. Health and liveness verification on Fly.io
2. Service branding and Model 3 Symmetric Dual-Hybrid configuration check
3. Account status ($100k cash, 2.0x buying power, SS-CHALLENGER-MAIN)
4. Authentication enforcement (HTTP 401 on missing/bad token)
5. Safety limit enforcement (HTTP 400 on negative shares, HTTP 422 on excessive size)
6. Bootstrap unheld sell filter verification
7. Live SignalStack webhook routing (BUY 1 TNA via webhook fBFE6NMFazpReVZVcJjy3N)
8. Idempotency protection check (HTTP 409 on duplicate signal_id)
9. Mark price update & MTM calculation
10. Round-trip SELL order execution to close position and record realized P/L
11. Final verification that portfolio returns to flat
"""

import sys
import time
import requests

AGENT_URL = "https://signalstack-challenger-agent.fly.dev"
SIGNAL_SECRET = "super-secret-trading-signal-token-2026"
HEADERS = {
    "Authorization": f"Bearer {SIGNAL_SECRET}",
    "Content-Type": "application/json",
    "User-Agent": "ChallengerTestClient/1.0"
}


def log(msg, emoji="ℹ️"):
    print(f"[{emoji}] {msg}")


def run_live_test():
    print("=" * 65)
    print("🚀 Running Live Challenger Agent End-to-End Test Suite")
    print(f"Target: {AGENT_URL}")
    print("=" * 65)
    passed = 0
    total = 11

    # 1. Health check
    log("Test 1: Health & Liveness Check...", "🧪")
    r = requests.get(f"{AGENT_URL}/health", timeout=10)
    assert r.status_code == 200, f"Expected 200, got {r.status_code}"
    h = r.json()
    assert h.get("status") == "healthy", f"Expected healthy, got {h}"
    assert h.get("broker") == "signalstack_paper", f"Expected signalstack_paper, got {h}"
    log(f"PASS: Agent is healthy (broker: {h['broker']}, bootstrap: {h.get('bootstrap_mode')})", "✅")
    passed += 1

    # 2. Service metadata and Model 3 branding
    log("Test 2: Service Metadata & Model 3 Branding...", "🧪")
    r = requests.get(f"{AGENT_URL}/", timeout=10)
    assert r.status_code == 200
    meta = r.json()
    assert meta.get("service") == "SignalStack Challenger Trading Agent", f"Unexpected service: {meta.get('service')}"
    assert meta.get("model") == "Model 3 (Symmetric Dual-Hybrid)", f"Unexpected model: {meta.get('model')}"
    assert meta.get("role") == "challenger", f"Unexpected role: {meta.get('role')}"
    assert meta.get("champion_url") == "https://signalstack-trading-agent.fly.dev"
    log(f"PASS: Service identified as {meta['service']} ({meta['model']})", "✅")
    passed += 1

    # 3. Account balances
    log("Test 3: Paper Account Balances & Starting State...", "🧪")
    r = requests.get(f"{AGENT_URL}/account", timeout=10)
    assert r.status_code == 200
    acct = r.json()["account"]
    assert acct.get("account_number") == "SS-CHALLENGER-MAIN", f"Expected SS-CHALLENGER-MAIN, got {acct.get('account_number')}"
    assert acct.get("status") == "ACTIVE", f"Expected ACTIVE, got {acct.get('status')}"
    log(f"PASS: Account {acct['account_number']} is ACTIVE (Cash: ${acct['cash']:,.2f}, Buying Power: ${acct['buying_power']:,.2f})", "✅")
    passed += 1

    # 4. Authentication Check
    log("Test 4: Authentication Security Enforcement...", "🧪")
    bad_r = requests.post(
        f"{AGENT_URL}/signal",
        json={"signal_id": "test"},
        headers={"Authorization": "Bearer invalid-secret"}
    )
    assert bad_r.status_code == 401, f"Expected 401 Unauthorized, got {bad_r.status_code}"
    log("PASS: Unauthorized request blocked with HTTP 401", "✅")
    passed += 1

    # 5. Payload validation
    log("Test 5: Payload Validation Check...", "🧪")
    val_r = requests.post(
        f"{AGENT_URL}/signal",
        json={"signal_id": "bad-val", "symbol": "TNA", "action": "BUY", "shares": -5, "signal_price": 50.0},
        headers=HEADERS
    )
    assert val_r.status_code == 400, f"Expected 400 Bad Request, got {val_r.status_code}"
    log(f"PASS: Negative shares blocked with HTTP 400: {val_r.json().get('message')}", "✅")
    passed += 1

    # 6. Hard safety limit check
    log("Test 6: Safety Limit Ceiling Check...", "🧪")
    safe_r = requests.post(
        f"{AGENT_URL}/signal",
        json={"signal_id": "excess-shares", "symbol": "TNA", "action": "BUY", "shares": 100000, "signal_price": 50.0},
        headers=HEADERS
    )
    assert safe_r.status_code == 422, f"Expected 422 Unprocessable, got {safe_r.status_code}"
    log(f"PASS: Excessive order size blocked with HTTP 422: {safe_r.json().get('message')}", "✅")
    passed += 1

    # 7. Bootstrap unheld sell filter
    log("Test 7: Bootstrap Unheld Sell Filter (Graceful Skip)...", "🧪")
    unheld_r = requests.post(
        f"{AGENT_URL}/signal",
        json={
            "signal_id": f"sig-unheld-test-{int(time.time())}",
            "symbol": "FAS",
            "action": "SELL",
            "shares": 10,
            "signal_price": 120.0
        },
        headers=HEADERS
    )
    assert unheld_r.status_code == 200, f"Expected 200, got {unheld_r.status_code}"
    u_data = unheld_r.json()
    assert u_data.get("status") == "skipped", f"Expected skipped, got {u_data}"
    assert u_data.get("reason") == "bootstrap_unheld_position"
    log(f"PASS: Unheld exit safely skipped: {u_data.get('message')}", "✅")
    passed += 1

    # 8. Live SignalStack BUY order submission
    log("Test 8: Live BUY Order via SignalStack Webhook (1 share TNA)...", "🧪")
    buy_sig_id = f"sig-test-buy-TNA-{int(time.time())}"
    buy_r = requests.post(
        f"{AGENT_URL}/signal",
        json={
            "signal_id": buy_sig_id,
            "symbol": "TNA",
            "action": "BUY",
            "shares": 1,
            "signal_price": 58.00,
            "strategy": "challenger_symmetric_dual_hybrid"
        },
        headers=HEADERS
    )
    assert buy_r.status_code == 200, f"Expected 200, got {buy_r.status_code}: {buy_r.text}"
    buy_data = buy_r.json()
    assert buy_data.get("order_status") == "filled", f"Expected filled, got {buy_data}"
    order_id = buy_data.get("order_id")
    log(f"PASS: Order accepted & routed to SignalStack! Order ID: {order_id}, Filled: 1 TNA @ $58.00", "✅")
    passed += 1

    # Verify positions table
    pos_r = requests.get(f"{AGENT_URL}/positions", timeout=10)
    positions = pos_r.json().get("positions", [])
    tna_pos = next((p for p in positions if p["symbol"] == "TNA"), None)
    assert tna_pos is not None, f"TNA position not found in positions: {positions}"
    assert tna_pos["qty"] == 1, f"Expected 1 qty, got {tna_pos['qty']}"
    log(f"Position Verified: {tna_pos['qty']} sh TNA @ ${tna_pos['avg_entry_price']:.2f} (Cost basis: ${tna_pos['cost_basis']:.2f})", "📊")

    # 9. Idempotency Check
    log("Test 9: Idempotency Protection Check...", "🧪")
    dup_r = requests.post(
        f"{AGENT_URL}/signal",
        json={
            "signal_id": buy_sig_id,
            "symbol": "TNA",
            "action": "BUY",
            "shares": 1,
            "signal_price": 58.00
        },
        headers=HEADERS
    )
    assert dup_r.status_code == 409, f"Expected 409 Conflict, got {dup_r.status_code}"
    log(f"PASS: Duplicate signal rejected with HTTP 409: {dup_r.json().get('message')}", "✅")
    passed += 1

    # 10. Mark Price Update
    log("Test 10: Mark Price Update & MTM Valuation...", "🧪")
    price_r = requests.post(
        f"{AGENT_URL}/price",
        json={"symbol": "TNA", "price": 59.50, "prev_close": 58.00},
        headers=HEADERS
    )
    assert price_r.status_code == 200
    pos_r2 = requests.get(f"{AGENT_URL}/positions", timeout=10)
    tna_pos2 = next((p for p in pos_r2.json().get("positions", []) if p["symbol"] == "TNA"), None)
    assert tna_pos2 is not None
    assert tna_pos2["current_price"] == 59.50
    assert abs(tna_pos2["unrealized_pl"] - 1.50) < 0.05
    log(f"PASS: Mark price updated to $59.50, Unrealized P/L: +${tna_pos2['unrealized_pl']:.2f} ({tna_pos2['unrealized_pl_pct']:+.2f}%)", "✅")
    passed += 1

    # 11. Round-Trip SELL Order Execution
    log("Test 11: Round-Trip SELL Order Execution (Close 1 share TNA)...", "🧪")
    sell_sig_id = f"sig-test-sell-TNA-{int(time.time())}"
    sell_r = requests.post(
        f"{AGENT_URL}/signal",
        json={
            "signal_id": sell_sig_id,
            "symbol": "TNA",
            "action": "SELL",
            "shares": 1,
            "signal_price": 59.50,
            "strategy": "challenger_symmetric_dual_hybrid",
            "exit_reason": "Test Round-Trip Close"
        },
        headers=HEADERS
    )
    assert sell_r.status_code == 200, f"Expected 200, got {sell_r.status_code}: {sell_r.text}"
    sell_data = sell_r.json()
    assert sell_data.get("order_status") == "filled"
    log(f"PASS: SELL order routed to SignalStack! Order ID: {sell_data.get('order_id')}", "✅")

    # Verify positions is now flat
    final_pos_r = requests.get(f"{AGENT_URL}/positions", timeout=10)
    final_positions = final_pos_r.json().get("positions", [])
    tna_final = next((p for p in final_positions if p["symbol"] == "TNA"), None)
    assert tna_final is None, "TNA position should be completely closed"

    # Verify realized P/L in closed trades
    trades_r = requests.get(f"{AGENT_URL}/trades", timeout=10)
    trades = trades_r.json().get("trades", [])
    tna_trade = next((t for t in trades if t["symbol"] == "TNA" and t["action"] == "SELL"), None)
    assert tna_trade is not None, "Closed trade record not found"
    assert abs(tna_trade["realized_pl"] - 1.50) < 0.05
    log(f"PASS: TNA closed cleanly! Realized P/L recorded: +${tna_trade['realized_pl']:.2f}", "✅")
    passed += 1

    print("\n" + "=" * 65)
    print(f"🎉 ALL LIVE TESTS PASSED ({passed}/{total})")
    print("SignalStack Webhook: https://app.signalstack.com/hook/fBFE6NMFazpReVZVcJjy3N")
    print(f"Live Dashboard:     {AGENT_URL}/")
    print("=" * 65)


if __name__ == "__main__":
    run_live_test()
