"""
Entry point. Starts the FastAPI server via uvicorn.
The APScheduler inside web/app.py handles auto-refresh.

Usage:
    python main.py
    open http://localhost:8000
"""
import sys
import pathlib

# Ensure project root is on sys.path
sys.path.insert(0, str(pathlib.Path(__file__).parent))

import uvicorn
from config import HOST, PORT

if __name__ == "__main__":
    print(f"\n  PrizePicks +EV Finder")
    print(f"  Dashboard -> http://{HOST}:{PORT}\n")
    uvicorn.run(
        "web.app:app",
        host=HOST,
        port=PORT,
        reload=False,
        log_level="info",
    )
