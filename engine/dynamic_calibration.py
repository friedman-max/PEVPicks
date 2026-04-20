import os
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from engine.database import get_db

logger = logging.getLogger(__name__)

CALIBRATION_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "calibration_map.json")
MIN_OBSERVATIONS = 30
MAX_MULTIPLIER = 1.5
MIN_MULTIPLIER = 0.5

def update_calibration_map():
    """
    Query market_observatory for resolved rows and update the global multiplier map.
    Should be called daily or on startup.
    """
    db = get_db()
    if not db:
        return
    
    try:
        # Fetch all resolved observations
        res = db.table("market_observatory").select("league, prop, true_prob, result").neq("result", "pending").execute()
        df = pd.DataFrame(res.data)
        
        if df.empty:
            logger.info("DynamicCalibration: No resolved data yet.")
            return

        # Filter for hit/miss (exclude pushes/dnps from multiplier math)
        df = df[df['result'].isin(['hit', 'miss'])].copy()
        df['outcome'] = df['result'].map({'hit': 1, 'miss': 0})
        
        # Calculate multipliers by (league, prop)
        stats = df.groupby(['league', 'prop']).agg(
            count=('outcome', 'count'),
            actual_rate=('outcome', 'mean'),
            expected_rate=('true_prob', 'mean')
        )
        
        # Apply the 30-observation filter
        mature_stats = stats[stats['count'] >= MIN_OBSERVATIONS].copy()
        
        # multiplier = actual / expected
        mature_stats['multiplier'] = (mature_stats['actual_rate'] / mature_stats['expected_rate']).clip(MIN_MULTIPLIER, MAX_MULTIPLIER)
        
        # Convert to dictionary map
        # Format: {"NBA|Points": 0.77, "MLB|Singles": 1.05}
        calibration_map = {}
        for index, row in mature_stats.iterrows():
            key = f"{index[0]}|{index[1]}"
            calibration_map[key] = round(float(row['multiplier']), 4)
            
        # Ensure data directory exists
        os.makedirs(os.path.dirname(CALIBRATION_FILE), exist_ok=True)
        
        # Write to JSON
        with open(CALIBRATION_FILE, "w") as f:
            json.dump(calibration_map, f, indent=2)
            
        logger.info("DynamicCalibration: Updated map with %d mature prop types.", len(calibration_map))
        return calibration_map
        
    except Exception as e:
        logger.error("DynamicCalibration: Failed to update map: %s", e)
        return None

def load_calibration_map() -> dict:
    """Load the persisted calibration map from disk."""
    if not os.path.exists(CALIBRATION_FILE):
        return {}
    try:
        with open(CALIBRATION_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}
