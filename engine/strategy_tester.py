import logging
import pandas as pd
import numpy as np
from typing import List, Dict, Optional
from datetime import datetime
from dataclasses import dataclass, field

from engine.database import get_db
from engine.constants import POWER_PAYOUTS, FLEX_PAYOUTS

logger = logging.getLogger(__name__)

@dataclass
class StrategyConfig:
    leagues: List[str] = field(default_factory=list)
    min_prob: float = 0.5408  # Default to optimal break-even
    slip_size: int = 6        # 2, 3, 4, 5, 6
    slip_type: str = "flex"   # "power", "flex"
    bankroll: float = 100.0
    bet_size: float = 1.0     # Fixed bet size per slip
    excluded_props: List[str] = field(default_factory=list)
    included_props: List[str] = field(default_factory=list)  # Empty = all props
    use_calibration: bool = True
    use_kelly: bool = False

class StrategyTester:
    def __init__(self):
        self.db = get_db()

    def _calculate_kelly_fraction(self, probs: List[float], slip_size: int, slip_type: str) -> float:
        import itertools
        outcomes = list(itertools.product([0, 1], repeat=slip_size))
        
        ev = 0.0
        ev_sq = 0.0
        
        for outcome in outcomes:
            prob = 1.0
            for i in range(slip_size):
                prob *= probs[i] if outcome[i] == 1 else (1.0 - probs[i])
            
            hits = sum(outcome)
            mult = 0.0
            if slip_type == "power":
                if hits == slip_size:
                    mult = POWER_PAYOUTS.get(slip_size, 0.0)
            else:
                mult = FLEX_PAYOUTS.get(slip_size, {}).get(hits, 0.0)
                
            net_profit = mult - 1.0
            ev += prob * net_profit
            ev_sq += prob * (net_profit ** 2)
            
        if ev <= 0:
            return 0.0
            
        variance = ev_sq - (ev ** 2)
        if variance <= 0:
            return 0.0
            
        # Quarter-Kelly is standard practice to manage drawdown risk
        kelly = (ev / variance) * 0.25 
        return max(0.0, min(kelly, 1.0))


    def run_simulation(self, config: StrategyConfig) -> Dict:
        """
        Runs a historical simulation based on the provided strategy configuration.
        """
        if not self.db:
            return {"error": "Database not connected"}

        try:
            # 1. Fetch resolved data
            query = self.db.table("market_observatory").select("*").neq("result", "pending")
            if config.leagues:
                query = query.in_("league", config.leagues)
            
            res = query.execute()
            df = pd.DataFrame(res.data)

            if df.empty:
                return {"error": "No resolved data found matching filters."}

            # 2. Pre-process outcomes
            df = df[df['result'].isin(['hit', 'miss'])].copy()
            df['outcome_bit'] = df['result'].map({'hit': 1, 'miss': 0})
            
            # Apply exclusion filters
            if config.excluded_props:
                df = df[~df['prop'].isin(config.excluded_props)]

            # Apply inclusion filters (stat-type specialization)
            if config.included_props:
                df = df[df['prop'].isin(config.included_props)]

            # Apply probability filter
            # Note: In a real simulation, we might want to apply calibration multipliers here
            # but for now we'll use the recorded true_prob.
            df = df[df['true_prob'] >= config.min_prob]

            if df.empty:
                return {"error": "No legs found above the probability threshold."}

            # 3. Group into Slates (By Day)
            # A slate represents all legs available on a given calendar day.
            df['game_start_dt'] = pd.to_datetime(df['game_start'])
            df['slate_id'] = df['game_start_dt'].dt.date.astype(str)
            slates = df.groupby('slate_id')

            sim_slips = []
            cumulative_profit = 0.0
            total_bet = 0.0
            bankroll = config.bankroll
            equity_curve = []
            
            # Sort slates by time to simulate chronological betting
            sorted_slate_ids = df.sort_values('game_start')['slate_id'].unique()

            for sid in sorted_slate_ids:
                slate_df = slates.get_group(sid)
                
                # Sort legs by true_prob to pick the best ones first
                sorted_legs = slate_df.sort_values('true_prob', ascending=False)
                
                # Build as many slips of 'slip_size' as possible from this day's pool
                for i in range(0, len(sorted_legs) - config.slip_size + 1, config.slip_size):
                    selected_legs = sorted_legs.iloc[i : i + config.slip_size]
                    
                    if config.use_kelly:
                        probs = selected_legs['true_prob'].tolist()
                        k_frac = self._calculate_kelly_fraction(probs, config.slip_size, config.slip_type)
                        bet_size = bankroll * k_frac
                    else:
                        bet_size = config.bet_size
                        
                    # Calculate result
                    hits = int(selected_legs['outcome_bit'].sum())
                    payout_mult = 0.0
                    
                    if config.slip_type == "power":
                        if hits == config.slip_size:
                            payout_mult = POWER_PAYOUTS.get(config.slip_size, 0.0)
                    else: # flex
                        payout_mult = FLEX_PAYOUTS.get(config.slip_size, {}).get(hits, 0.0)

                    profit = (bet_size * payout_mult) - bet_size
                    bankroll += profit
                    cumulative_profit += profit
                    total_bet += bet_size
                    
                    sim_slips.append({
                        "timestamp": selected_legs['game_start'].iloc[0],
                        "league": selected_legs['league'].iloc[0],
                        "hits": hits,
                        "n_legs": config.slip_size,
                        "payout": bet_size * payout_mult,
                        "bet_size": bet_size,
                        "profit": profit,
                        "legs": selected_legs[['player', 'prop', 'true_prob', 'result']].to_dict('records')
                    })
                    equity_curve.append({
                        "x": selected_legs['game_start'].iloc[0],
                        "y": round(cumulative_profit, 2)
                    })

            if not sim_slips:
                return {"error": f"Could not form any {config.slip_size}-leg slips from history."}

            # 4. Aggregate Results
            roi = (cumulative_profit / total_bet) if total_bet > 0 else 0
            win_rate = sum(1 for s in sim_slips if s['profit'] > 0) / len(sim_slips)

            return {
                "summary": {
                    "total_slips": len(sim_slips),
                    "total_bet": round(total_bet, 2),
                    "total_profit": round(cumulative_profit, 2),
                    "roi_pct": round(roi * 100, 2),
                    "win_rate_pct": round(win_rate * 100, 2),
                },
                "equity_curve": equity_curve,
                "slips": sim_slips[-50:] # Return last 50 for the UI log
            }

        except Exception as e:
            logger.exception("Simulation failed")
            return {"error": str(e)}
