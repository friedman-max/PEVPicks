# CoreProp

CoreProp is a local +EV betting dashboard that compares PrizePicks projections against sportsbook prices, matches similar props across books, and calculates both single-leg and slip-level expected value.

## Features

- Scrapes PrizePicks projections.
- Scrapes FanDuel, DraftKings, and Pinnacle player prop markets.
- Matches props with fuzzy player and stat matching.
- De-vigs sportsbook prices to estimate fair odds.
- Filters and sorts the best individual +EV bets.
- Builds 2-6 leg slips and estimates Power/Flex EV.
- Supports manual refresh plus scheduled auto-refresh.
- Exposes a runtime config API and UI for interval, EV threshold, and league toggles.
- Includes a backtest view for logging slips, checking results, and exporting CSV data.

## Dashboard

The web UI includes:

- A matched bets table for the current +EV plays.
- Separate views for raw PrizePicks, FanDuel, DraftKings, and Pinnacle lines.
- A slip builder that lets you select legs, calculate EV, and auto-build a best subset.
- A backtest dashboard with logged slips, filters, result checking, and CSV download.
- A config panel for changing refresh interval, EV threshold, and active leagues without editing code.

## Tech Stack

- Python 3.10+
- FastAPI + Uvicorn
- APScheduler
- Playwright
- httpx
- curl_cffi
- rapidfuzz

## Project Layout

```text
.
├─ main.py                # App entrypoint
├─ config.py              # Runtime config via env vars
├─ requirements.txt
├─ scrapers/
│  ├─ prizepicks.py       # PrizePicks API scraper
│  ├─ fanduel.py          # FanDuel scraper
│  ├─ draftkings.py       # DraftKings scraper
│  └─ pinnacle.py        # Pinnacle scraper
├─ engine/
│  ├─ matcher.py          # Cross-book prop matching
│  ├─ ev_calculator.py    # EV and slip EV calculations
│  ├─ backtest.py         # Backtest logging and slip tracking
│  └─ results_checker.py  # ESPN result checking
├─ web/
│  ├─ app.py              # FastAPI app, scheduler, API routes
│  └─ static/             # Frontend files
└─ data/                  # Local scraper output and debug dumps
```

## Quick Start

1. Create and activate a virtual environment.
2. Install Python dependencies.
3. Run the app.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python main.py
```

Then open:

- http://127.0.0.1:8000

## Configuration

Start from `.env.example` and copy it to `.env`, then adjust values for your machine. Defaults are also defined in `config.py`.

```env
# Scraping behavior
HEADLESS=false
REFRESH_INTERVAL_MINUTES=15

# EV filtering
MIN_INDIVIDUAL_EV_PCT=0.01

# Leagues
SCRAPE_ALL_LEAGUES=false
LEAGUE_NBA=true
LEAGUE_MLB=true
LEAGUE_NHL=true
LEAGUE_NCAAB=true

# Server
HOST=127.0.0.1
PORT=8000
```

Notes:

- `HEADLESS=false` is the default to improve reliability with sportsbook anti-bot checks.
- `SCRAPE_ALL_LEAGUES=false` keeps scraping limited to the enabled leagues.
- FanDuel, DraftKings, and Pinnacle payloads are cached in `data/` for debugging and offline analysis.
- If a scraper returns an empty response, the app can reuse the previous successful scrape when enough cached lines exist.

## API Endpoints

Core:

- `GET /api/bets` - Current matched +EV bets.
- `GET /api/matched` - Raw matched prop pairs before EV filtering.
- `GET /api/status` - Scrape status, timing, and errors.
- `POST /api/refresh` - Trigger a full pipeline refresh.
- `POST /api/slip` - Calculate slip EV for selected bet IDs.
- `POST /api/slip/auto` - Evaluate the best slip subset from a selection.
- `GET /api/config` - Read runtime config.
- `POST /api/config` - Update interval, min EV, and league toggles.

Book-specific lines:

- `GET /api/prizepicks` - Current PrizePicks lines.
- `POST /api/prizepicks/refresh` - Refresh PrizePicks lines.
- `GET /api/fanduel` - Current FanDuel lines.
- `POST /api/fanduel/refresh` - Refresh FanDuel lines.
- `GET /api/draftkings` - Current DraftKings lines.
- `POST /api/draftkings/refresh` - Refresh DraftKings lines.
- `GET /api/pinnacle` - Current Pinnacle lines.
- `POST /api/pinnacle/refresh` - Refresh Pinnacle lines.

Backtest:

- `GET /api/backtest/latest-slip` - Most recently logged slip.
- `GET /api/backtest/slips` - Logged slips from the backtest CSV.
- `POST /api/backtest/add-slip` - Log the currently selected bets as a slip.
- `GET /api/backtest/download-csv` - Download the backtest CSV.
- `POST /api/backtest/check-results` - Trigger result checking for pending slips.

## Troubleshooting

- No bets returned:
  - Verify league toggles in the UI or `.env`.
  - Trigger a manual refresh with `POST /api/refresh`.
  - Check logs for scraper or upstream API changes.
- FanDuel or DraftKings returns empty data:
  - Try `HEADLESS=false`.
  - Make sure Playwright dependencies are installed correctly.
- Backtest results are not updating:
  - Run the result check endpoint from the UI or call `POST /api/backtest/check-results`.

## Disclaimer

This tool is for educational and informational use. Odds data and line availability can change quickly and may be restricted by location.
