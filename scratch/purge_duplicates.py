import sys
import os
from collections import defaultdict

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + '/..'))
from engine.database import get_db
from engine.backtest import make_bet_key

def main():
    db = get_db()
    if not db:
        print("No db connection!")
        sys.exit(1)

    legs_res = db.table('legs').select('*').execute()
    legs = legs_res.data
    
    slips_res = db.table('slips').select('*').execute()
    slips = {s['id']: s for s in slips_res.data}

    print(f"Checking {len(slips)} slips for duplicates...")

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
                has_dup = True
                break
        
        if has_dup:
            slips_with_dups.append(slip_id)
                
    if not slips_with_dups:
        print("\nNo auto-generated slips found with duplicate player-game pairs!")
    else:
        print(f"\nPurging {len(slips_with_dups)} corrupt slips...")
        # Since legs have ON DELETE CASCADE usually, or we just delete slips and cascade happens.
        # But we'll delete slips explicitly, and let cascade handle legs, or we can explicitly delete legs too.
        # Wait, from migration_001.sql, it's ON DELETE CASCADE for users, but what about slips -> legs?
        # Let's delete legs first to be safe.
        db.table('legs').delete().in_('slip_id', slips_with_dups).execute()
        db.table('slips').delete().in_('id', slips_with_dups).execute()
        print("Purge complete.")

if __name__ == '__main__':
    main()
