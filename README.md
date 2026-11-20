# SignalStack Challenger Trading Agent (`M7d + Z-I Zero-Line`)

High-performance Go execution agent, live broker reconciler, and paired opposite-symbol dashboard for **Challenger M7d (Dynamic Intraday T+0 + Z-I Zero-Line)** on Fly.io (`https://signalstack-challenger-agent.fly.dev`).

```
┌──────────────────┐      ┌──────────────────────────────┐      ┌────────────────────────────────────────┐      ┌──────────────────────────┐
│ Alpaca SIP +     │ ───> │ Direxion Challenger Model    │ ───> │ SignalStack Challenger Agent (Fly.io)  │ ───> │ SignalStack Webhook /    │
│ Live Quotes      │      │ (soup3: M7d + Z-I Zero-Line) │      │ (Safety, Reconciliation, Z-I Dashboard)│      │ Alpaca Paper PA3HW1A38DR2│
└──────────────────┘      └──────────────────────────────┘      └────────────────────────────────────────┘      └──────────────────────────┘
                                                                                     │
                                                             ┌───────────────────────┼────────────────────────┐
                                                             ▼                       ▼                        ▼
                                                     ┌───────────────┐       ┌───────────────┐        ┌───────────────┐
                                                     │    SQLite     │       │  PocketBase   │        │     Neon      │
                                                     │ (/data/db +   │       │ (Collections) │        │ (PostgreSQL   │
                                                     │  Z-I Tables)  │       │               │        │  1:1 Mirror)  │
                                                     └───────────────┘       └───────────────┘        └───────────────┘
```

---

## 1. Core Principles & Features

1. **Model-Driven Execution (`soup3`):** The Python Challenger engine (`before_the_bell_alerts.py`, `signal_checker.py`, `zero_line_tracker.py`) computes 4H ZigZag pivots, `Linear` trend state, `Z-I` Entry-Pivot Zero-Line flip triggers, and share sizing. The Go agent acts as the execution gateway, safety gatekeeper, and ledger authority.
2. **Alpaca Broker Parity & Reconciliation:**
   - Supports direct Alpaca Paper (`PA3HW1A38DR2`) status queries and broker-authoritative position reconciliation via `POST /positions/reconcile`.
   - **Bootstrap Protection Filter:** Rejects/skips `SELL` signals for unheld symbols (`skipped_bootstrap_unheld`) to prevent short-sale rejections.
3. **Z-I Entry-Pivot Zero-Line & Repaint Tracking:**
   - Persists immutable entry-pivot zero-line anchors (`position_zero_lines`) and deduplicated repaint events (`position_repaints`) in SQLite, mirrored 1:1 from Neon PostgreSQL via `POST /zero-lines/sync`.
   - Tracks `flip_count` (`0` = primary mean-reversion leg, `1` = flipped `Linear Green Hold` trend-follow leg, max 1 flip), opposite Direxion partner ETF mark vs. entry-pivot zero line, and live flip readiness (`FLIP READY`, `LIN GREEN HOLD`, `BREACHED`, `WATCH`).
4. **Paired Opposite-Symbol Web Dashboard (`/`):**
   - Protected by cookie session auth (`/login`, `/logout`), HTTP Basic Auth, or `Authorization: Bearer <SIGNAL_SECRET>`.
   - **Active Paper Positions** table displays each open position paired side-by-side: **Active Position (left) → Entry Pivot Zero Line & Repaint Count (center) → Opposite Pair ETF, Mark vs. 0-Line, and Linear Trend badge (right)**.
   - **Entry Pivot Repaints & Zero-Line Audit Log** panel surfaces recent `position_repaints` events (`REPAINT`, `ZERO_LINE_BREACH`, `ZERO_LINE_FLIP`, `ZERO_LINE_STOP`).
5. **Hard Safety Limits & Idempotency:**
   - Enforces `MAX_ORDER_VALUE` (default `$50,000`), `MAX_SHARES_PER_ORDER` (default `5,000`), and buying power validation.
   - Rejects duplicate `signal_id` payloads with HTTP `409 Conflict`.
6. **Multi-Store Journaling (SQLite WAL):**
   - Core tables: `paper_account`, `paper_positions`, `paper_trades`, `execution_journal`, `position_zero_lines`, and `position_repaints`.
   - Optional mirrors to PocketBase and Neon PostgreSQL.

---

## 2. API Endpoints

| Method | Path | Auth Required | Description |
|---|---|---|---|
| `GET` | `/login` | No | Dashboard login form |
| `POST` | `/login` | Credentials | Establish `challenger_session` cookie |
| `GET` | `/logout` | No | Clear dashboard session cookie |
| `GET` | `/` | Session / Bearer (HTML) | Paired Opposite-Symbol Web Dashboard (HTML) or JSON summary |
| `GET` | `/health` | No | Liveness and broker connection status |
| `GET` | `/status` (`GET /account`) | No | Account balances, equity, open positions, `zero_lines`, and recent `repaints` |
| `GET` | `/positions` | No | List of all open positions in the paper portfolio |
| `GET` | `/journal` | No | Execution journal linking `signal_id`s to orders |
| `GET` | `/trades` | No | Closed trade history with realized P/L |
| `GET` | `/zero-lines` | No | Active `position_zero_lines` anchors, opposite partner marks, and flip status |
| `GET` | `/repaints` | No | Recent `position_repaints` audit log (`?limit=N`, default `50`) |
| `POST` | `/price` | **Bearer / Secret** | Update mark price for unrealized P/L calculation |
| `POST` | `/signal` | **Bearer / Secret** | Submit trade signal for safety checks and execution |
| `POST` | `/positions/reconcile` | **Bearer / Secret** | Reconcile SQLite positions and cash against live Alpaca broker state |
| `POST` | `/zero-lines/sync` | **Bearer / Secret** | Upsert `position_zero_lines` and `position_repaints` mirrored from Neon (`soup3`) |

---

## 3. Configuration & Environment Variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8080` | HTTP listening port |
| `SERVICE_NAME` | `SignalStack Challenger Trading Agent` | Service display name |
| `ACCOUNT_NUMBER` | `PA3HW1A38DR2` | Paper brokerage account identifier |
| `AGENT_ROLE` | `Challenger M7d (Intraday T+0 • Same-Day Buys & Sells)` | Strategy label shown in status/dashboard |
| `SIGNAL_SECRET` | *(set via Fly secrets)* | Shared secret for `Bearer` / `X-Signal-Secret` auth on `POST` endpoints |
| `DASHBOARD_USERNAME` | *(set via Fly secrets)* | Dashboard login username |
| `DASHBOARD_PASSWORD` | *(set via Fly secrets)* | Dashboard login password |
| `SIGNALSTACK_WEBHOOK_URL` | `""` | SignalStack webhook endpoint (dry-run if empty) |
| `SIGNALSTACK_API_KEY` | `""` | Optional SignalStack API key |
| `SIGNALSTACK_EXIT_ACTION` | `sell` | Exit webhook action (`sell` or `close`) |
| `ALPACA_API_KEY` / `ALPACA_SECRET_KEY` | `""` | Alpaca Paper API credentials (also reads `CHALLENGER_ALPACA_*`) |
| `ALPACA_BASE_URL` | `https://paper-api.alpaca.markets` | Alpaca REST endpoint |
| `INITIAL_CAPITAL` | `100000` | Starting paper portfolio cash balance in USD |
| `MARGIN_MULTIPLIER` | `2.0` | Buying power multiplier relative to cash |
| `BOOTSTRAP_MODE` | `true` | Filters unheld historical exit signals |
| `SQLITE_DB_PATH` | `/data/agent.db` (Fly) / `data/agent.db` | Persistent SQLite database path |
| `NEON_DATABASE_URL` | `""` | Optional Neon PostgreSQL connection string |
| `POCKETBASE_URL` | `""` | Optional PocketBase URL |
| `MAX_ORDER_VALUE` | `50000` | Hard cap on total order value in USD |
| `MAX_SHARES_PER_ORDER` | `5000` | Hard cap on shares per order |

---

## 4. Local Testing & Fly.io Deployment

### Run Go Unit & Integration Tests
```bash
cd /home/rollingw/fly/signalstack-challenger-agent
go test ./...
```

### Deploy to Fly.io
```bash
cd /home/rollingw/fly/signalstack-challenger-agent
./deploy.sh
```
