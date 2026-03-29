# PrizePicks +EV Finder

A local dashboard that compares PrizePicks lines against FanDuel odds to surface positive expected value (+EV) props.

## What It Does

- Scrapes PrizePicks projections from their public API.
- Scrapes FanDuel player prop markets with Playwright network interception.
- Matches props across books with fuzzy player/stat matching.
- Calculates individual bet EV and multi-pick slip EV.
- Serves a FastAPI backend plus a static web dashboard.
- Supports manual refresh and scheduled auto-refresh.

## Tech Stack

- Python 3.10+
- FastAPI + Uvicorn
- APScheduler
- Playwright + playwright-stealth
- httpx
- rapidfuzz

## Project Layout

```text
.
├─ main.py                # App entrypoint (starts Uvicorn)
├─ config.py              # Runtime config via env vars
├─ requirements.txt
├─ scrapers/
│  ├─ prizepicks.py       # PrizePicks API scraper
│  └─ fanduel.py          # FanDuel Playwright scraper
├─ engine/                # Matching + EV calculations
├─ web/
│  ├─ app.py              # FastAPI app + scheduler + API routes
│  └─ static/             # Frontend files
└─ data/                  # FanDuel intercepted JSON dumps
```

## Quick Start

1. Create and activate a virtual environment.
2. Install Python dependencies.
3. Install Playwright Chromium browser.
4. Run the app.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m playwright install chromium
python main.py
```

Then open:

- http://127.0.0.1:8000

## Configuration

Create a `.env` file in the project root (optional). Defaults are defined in `config.py`.

```env
# Scraping behavior
HEADLESS=false
REFRESH_INTERVAL_MINUTES=15

# EV filtering
MIN_INDIVIDUAL_EV_PCT=0.01

# Leagues
LEAGUE_NBA=true
LEAGUE_MLB=true
LEAGUE_NHL=true
LEAGUE_NCAAB=true

# Server
HOST=127.0.0.1
PORT=8000
```

Notes:

- `HEADLESS=false` is the default to improve reliability with FanDuel anti-bot checks.
- FanDuel response payloads are written to `data/` for debugging/offline analysis.

## API Endpoints

Core:

- `GET /api/bets` - Current matched +EV bets.
- `GET /api/status` - Scrape status, timing, and errors.
- `POST /api/refresh` - Trigger full pipeline refresh.
- `POST /api/slip` - Calculate slip EV for selected bet IDs.
- `GET /api/config` - Read runtime config.
- `POST /api/config` - Update interval/min EV/league toggles.

PrizePicks-only:

- `GET /api/prizepicks` - Current PrizePicks-only lines.
- `POST /api/prizepicks/refresh` - Refresh PrizePicks-only lines.

## Troubleshooting

- Browser not installed:

```powershell
python -m playwright install chromium
```

- PowerShell execution policy blocks venv activation:

```powershell
Set-ExecutionPolicy -Scope Process Bypass
```

- No bets returned:
  - Verify league toggles in config/UI.
  - Trigger manual refresh (`POST /api/refresh`).
  - Check logs for scraper/API changes.

## Disclaimer

This tool is for educational and informational use. Odds data and line availability can change quickly and may be restricted by location.
