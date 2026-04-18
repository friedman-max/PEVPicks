import sys
import os
from collections import defaultdict
from pprint import pprint

# Assuming we run from the project root
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + '/..'))
from engine.database import get_db
from engine.backtest import make_bet_key

def main():
    db = get_db()
    if not db:
        print("No db connection!")
        sys.exit(1)

    print("Fetching all legs from Supabase...")
    # Getting legs
    legs_res = db.table('legs').select('*').execute()
    legs = legs_res.data
    
    slips_res = db.table('slips').select('*').execute()
    slips = {s['id']: s for s in slips_res.data}

    print(f"Fetched {len(legs)} legs in {len(slips)} slips.")

    slips_with_dups = []
    
    legs_by_slip = defaultdict(list)
    for leg in legs:
        legs_by_slip[leg['slip_id']].append(leg)
        
    for slip_id, slip_legs in legs_by_slip.items():
        seen_players = defaultdict(list)
        for leg in slip_legs:
            # We want to check (player, game_start) logic
            player = leg.get('player', '')
            game_start = leg.get('game_start', '')
            key = make_bet_key(player, game_start)
            seen_players[key].append(leg)
            
        has_dup = False
        for key, duplicate_legs in seen_players.items():
            if len(duplicate_legs) > 1:
                if not has_dup:
                    print(f"\n--- Slip {slip_id} (Type: {slips[slip_id].get('slip_type')}, Time: {slips[slip_id].get('timestamp')}) has duplicates! ---")
                    has_dup = True
                print(f"  Key {key}:")
                for dleg in duplicate_legs:
                    print(f"    - {dleg['prop']} {dleg['line']} {dleg['side']} (game_start: {dleg['game_start']})")
                slips_with_dups.append(slip_id)
                
    if not slips_with_dups:
        print("\nNo auto-generated slips found with duplicate player-game pairs!")
    else:
        print(f"\nTotal slips with duplicates: {len(slips_with_dups)}")

if __name__ == '__main__':
    main()
