"""
signal_client.py

Client library for communicating with the SignalStack Paper Trading Agent.
Enforces authentication and formats trade instructions to standard schema.
"""

import os
import datetime
import requests
from typing import Dict, Any, Optional

DEFAULT_AGENT_URL = os.getenv("SIGNALSTACK_AGENT_URL", "http://127.0.0.1:8080")
DEFAULT_SIGNAL_SECRET = os.getenv("SIGNAL_SECRET", "super-secret-trading-signal-token-2026")


class SignalStackAgentClient:
    """Client for communicating with the SignalStack Go Trading Agent."""

    def __init__(self, base_url: str = DEFAULT_AGENT_URL, signal_secret: str = DEFAULT_SIGNAL_SECRET):
        self.base_url = base_url.rstrip("/")
        self.signal_secret = signal_secret
        self.headers = {
            "Authorization": f"Bearer {self.signal_secret}",
            "Content-Type": "application/json",
            "User-Agent": "SignalStackClient/1.0"
        }

    def check_health(self) -> Dict[str, Any]:
        """Queries the agent's /health endpoint."""
        resp = requests.get(f"{self.base_url}/health", timeout=5)
        resp.raise_for_status()
        return resp.json()

    def get_status(self) -> Dict[str, Any]:
        """Queries /status to view paper account details, buying power, equity, and positions."""
        resp = requests.get(f"{self.base_url}/status", timeout=10)
        resp.raise_for_status()
        return resp.json()

    def get_positions(self) -> Dict[str, Any]:
        """Queries /positions to view active open holdings."""
        resp = requests.get(f"{self.base_url}/positions", timeout=10)
        resp.raise_for_status()
        return resp.json()

    def get_journal(self) -> Dict[str, Any]:
        """Queries /journal for recent execution records."""
        resp = requests.get(f"{self.base_url}/journal", timeout=10)
        resp.raise_for_status()
        return resp.json()

    def get_trades(self) -> Dict[str, Any]:
        """Queries /trades for closed trades and realized P/L."""
        resp = requests.get(f"{self.base_url}/trades", timeout=10)
        resp.raise_for_status()
        return resp.json()

    def update_price(self, symbol: str, price: float) -> Dict[str, Any]:
        """Updates the mark price for an open position."""
        payload = {"symbol": symbol.upper().strip(), "price": float(price)}
        resp = requests.post(f"{self.base_url}/price", json=payload, headers=self.headers, timeout=5)
        return {"status_code": resp.status_code, "data": resp.json() if resp.ok else resp.text}

    def send_signal(
        self,
        signal_id: str,
        symbol: str,
        action: str,
        shares: float,
        signal_price: float,
        timestamp: Optional[str] = None,
        strategy: str = "direxion_4h_zigzag"
    ) -> Dict[str, Any]:
        """
        Dispatches a broker-neutral trade instruction to the SignalStack Agent.

        Args:
            signal_id: Unique idempotency key (e.g. 'sig-TNA-20260928-110000')
            symbol: Ticker symbol (e.g. 'TNA')
            action: 'BUY' or 'SELL'
            shares: Number of shares calculated by trading model
            signal_price: Model trigger price
            timestamp: ISO8601 timestamp string
            strategy: Strategy name

        Returns:
            Dict containing response status and order execution details.
        """
        if not timestamp:
            timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

        payload = {
            "signal_id": signal_id,
            "symbol": symbol.upper().strip(),
            "action": action.upper().strip(),
            "shares": float(shares),
            "signal_price": float(signal_price),
            "timestamp": timestamp,
            "strategy": strategy
        }

        url = f"{self.base_url}/signal"
        resp = requests.post(url, json=payload, headers=self.headers, timeout=15)

        try:
            data = resp.json()
        except Exception:
            data = {"raw_text": resp.text}

        return {
            "status_code": resp.status_code,
            "ok": resp.ok,
            "data": data
        }
