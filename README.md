# SignalStack Paper Trading Agent

High-performance, secure Go execution agent designed for algorithmic trading with **SignalStack** (`https://app.signalstack.com`):

```
┌─────────┐      ┌─────────────────────────────┐      ┌────────────────────────────────┐      ┌──────────────────────────┐
│  EODHD  │ ───> │ Direxion Trading Model      │ ───> │ SignalStack Agent (Fly.io)     │ ───> │ SignalStack Webhook /    │
│  Data   │      │ (soup3: indicators, shares) │      │ (Safety, Bootstrap, Journal)  │      │ Connected Paper Broker   │
└─────────┘      └─────────────────────────────┘      └────────────────────────────────┘      └──────────────────────────┘
                                                                      │
                                              ┌───────────────────────┼────────────────────────┐
                                              ▼                       ▼                        ▼
                                      ┌───────────────┐       ┌───────────────┐        ┌───────────────┐
                                      │    SQLite     │       │  PocketBase   │        │     Neon      │
                                      │ (Paper Book)  │       │ (Collections) │        │ (PostgreSQL)  │
                                      └───────────────┘       └───────────────┘        └───────────────┘
```

---

## 1. Core Principles & Features

1. **Model-Driven Execution**: The Python strategy (`soup3`) computes indicators, timing, and exact position sizes (`shares`). The Go agent acts as the execution gateway and safety gatekeeper.
2. **Fresh Bootstrap Starting State**:
   - The paper trading portfolio starts **flat from scratch** (0 open positions, $100,000 cash).
   - Historical open positions from previous weeks are **NOT carried over**.
   - **Bootstrap Protection Filter**: Any `SELL` instruction received for a symbol that was *not* opened during the current bootstrap cycle is gracefully skipped with `skipped_bootstrap_unheld`, preventing broker rejections (`asset cannot be sold short`) or synthetic short positions.
   - New `BUY` orders automatically open positions in the local paper ledger and execute via SignalStack.
3. **SignalStack Webhook Routing**:
   - Formats orders directly for SignalStack's alert webhook schema:
     ```json
     {
       "symbol": "TNA",
       "action": "buy",
       "quantity": 38,
       "quantity_type": "fixed"
     }
     ```
   - Supports dry-run simulation mode when `SIGNALSTACK_WEBHOOK_URL` is omitted.
4. **Hard Safety Limits**:
   - Hard order value ceiling (`MAX_ORDER_VALUE`, default $50,000).
   - Hard max shares per order (`MAX_SHARES_PER_ORDER`, default 5,000).
   - Available buying power validation before order routing.
5. **Strict Idempotency Protection**:
   - Rejects duplicate `signal_id` payloads with HTTP `409 Conflict`.
6. **Multi-Store Journaling**:
   - Primary: SQLite (`paper_account`, `paper_positions`, `paper_trades`, `execution_journal`) with WAL mode.
   - Optional sync to PocketBase and Neon PostgreSQL.

---

## 2. API Endpoints

| Method | Path | Auth Required | Description |
|---|---|---|---|
| `GET` | `/` | No | Web Dashboard (HTML) or Service Summary (JSON) |
| `GET` | `/health` | No | Liveness and broker connection status |
| `GET` | `/status` | No | Paper account balances, buying power, equity, and open positions |
| `GET` | `/positions` | No | List of all open positions in paper portfolio |
| `GET` | `/journal` | No | Execution journal linking signal IDs to orders |
| `GET` | `/trades` | No | Closed trade history with realized profit/loss |
| `POST` | `/price` | **Yes** | Update mark price for unrealized P/L calculation |
| `POST` | `/signal` | **Yes** | Submit trade signal for safety checks and SignalStack execution |

---

## 3. Configuration & Environment Variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8080` | HTTP listening port |
| `SIGNAL_SECRET` | `super-secret-trading-signal-token-2026` | Shared secret for authenticating Python signals |
| `SIGNALSTACK_WEBHOOK_URL` | `""` | SignalStack webhook endpoint (from dashboard) |
| `SIGNALSTACK_API_KEY` | `""` | Optional SignalStack API Key for authentication |
| `INITIAL_CAPITAL` | `100000` | Starting paper portfolio cash balance in USD |
| `MARGIN_MULTIPLIER` | `2.0` | Buying power multiplier relative to cash |
| `BOOTSTRAP_MODE` | `true` | Starts fresh; filters unheld historical exit signals |
| `SQLITE_DB_PATH` | `/data/agent.db` | Persistent SQLite database file path |
| `NEON_DATABASE_URL` | `""` | Optional Neon PostgreSQL connection string |
| `POCKETBASE_URL` | `""` | Optional PocketBase URL |
| `MAX_ORDER_VALUE` | `50000` | Hard cap on total order value in USD |
| `MAX_SHARES_PER_ORDER` | `5000` | Hard cap on shares per order |

---

## 4. SignalStack Webhook Setup

1. Log in to [SignalStack](https://app.signalstack.com/me/documentation).
2. Under **Brokers & Webhooks**, locate or create your webhook for paper trading.
3. Copy the generated Webhook URL (e.g. `https://api.signalstack.com/v1/alerts/<unique-id>`).
4. Set the environment variable:
   ```bash
   fly secrets set SIGNALSTACK_WEBHOOK_URL="https://api.signalstack.com/v1/alerts/YOUR_WEBHOOK_ID"
   ```

---

## 5. Fly.io Deployment

### Step 1: Create Fly App & Volume
```bash
cd /home/rollingw/fly/signalstack-agent

# Create the persistent storage volume for SQLite
fly volumes create signalstack_data --region ord --size 1
```

### Step 2: Set Secrets
```bash
fly secrets set \
  SIGNAL_SECRET="super-secret-trading-signal-token-2026" \
  SIGNALSTACK_WEBHOOK_URL="https://api.signalstack.com/v1/alerts/YOUR_ID" \
  NEON_DATABASE_URL="postgresql://neondb_owner:...@ep-...neon.tech/neondb?sslmode=require"
```

### Step 3: Deploy
```bash
fly deploy
```

---

## 6. Local Testing & Verification

Run the end-to-end test suite:
```bash
# In Terminal 1: Start agent
./bin/agent

# In Terminal 2: Run test suite
python3 python/test_pipeline.py
```
