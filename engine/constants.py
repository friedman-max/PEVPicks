"""
Hardcoded PrizePicks break-even table and payout structures.
"""

# Break-even probability per slip type (from PrizePicks official table)
BREAK_EVEN = {
    ("2", "power"): 0.5763,
    ("3", "power"): 0.5495,
    ("3", "flex"):  0.5781,
    ("4", "power"): 0.5614,
    ("4", "flex"):  0.5495,
    ("5", "power"): 0.5495,
    ("5", "flex"):  0.5434,
    ("6", "power"): 0.5475,
    ("6", "flex"):  0.5434,
}

# Power slip payout multipliers (decimal, e.g. 3x means you get 3x your stake back)
POWER_PAYOUTS = {
    2: 3.0,
    3: 5.0,
    4: 10.0,
    5: 20.0,
    6: 25.0,
}

# Flex payout tiers: {n_picks: {k_correct: decimal_multiplier}}
# Only tiers that pay out are listed; missing k → 0
FLEX_PAYOUTS = {
    3: {2: 1.25, 3: 2.5},
    4: {3: 1.5,  4: 5.0},
    5: {4: 2.0,  5: 10.0},
    6: {4: 0.4,  5: 2.0,  6: 10.0},
}

# The most efficient single-leg implied decimal odds (5-Flex / 6-Flex break-even = 54.34%)
OPTIMAL_BREAK_EVEN = 0.5434
OPTIMAL_IMPLIED_DECIMAL = 1.0 / OPTIMAL_BREAK_EVEN  # ≈ 1.8402

# Prop type normalization: FanDuel label → PrizePicks stat_type label
# Keys are lowercase FanDuel strings; values are PrizePicks stat_type strings
PROP_TYPE_MAP = {
    # ── Basketball (NBA / NCAAB) ──
    # PrizePicks actual stat_type labels confirmed from API:
    #   Points, Rebounds, Assists, 3-PT Made, Blocked Shots, Steals, Turnovers,
    #   Pts+Rebs+Asts, Rebs+Asts, Pts+Rebs, Pts+Asts, Fantasy Score,
    #   Blks+Stls, Offensive Rebounds, Defensive Rebounds, FG Made, FG Attempted,
    #   Free Throws Made, Free Throws Attempted, 3-PT Attempted, Personal Fouls
    "points":                        "Points",
    "player points":                 "Points",
    "rebounds":                      "Rebounds",
    "player rebounds":               "Rebounds",
    "assists":                       "Assists",
    "player assists":                "Assists",
    "3-point field goals made":      "3-PT Made",
    "3 point field goals made":      "3-PT Made",
    "3-pointers made":               "3-PT Made",
    "three point field goals made":  "3-PT Made",
    "made threes":                   "3-PT Made",
    "steals":                        "Steals",
    "blocks":                        "Blocked Shots",
    "blocked shots":                 "Blocked Shots",
    "turnovers":                     "Turnovers",
    "pts + reb + ast":               "Pts+Rebs+Asts",
    "pts+reb+ast":                   "Pts+Rebs+Asts",
    "points + rebounds + assists":   "Pts+Rebs+Asts",
    "rebounds + assists":            "Rebs+Asts",
    "points + rebounds":             "Pts+Rebs",
    "points + assists":              "Pts+Asts",
    "blocks + steals":               "Blks+Stls",
    "steals + blocks":               "Blks+Stls",
    "fantasy score":                 "Fantasy Score",
    "offensive rebounds":            "Offensive Rebounds",
    "defensive rebounds":            "Defensive Rebounds",
    "field goals made":              "FG Made",
    "field goals attempted":         "FG Attempted",
    "free throws made":              "Free Throws Made",
    "free throws attempted":         "Free Throws Attempted",
    "personal fouls":                "Personal Fouls",

    # ── Baseball (MLB) ──
    # PrizePicks: Pitcher Strikeouts, Hits, Total Bases, Runs, RBIs, Walks,
    #   Earned Runs Allowed, Hits Allowed, Hitter Fantasy Score, Pitching Outs
    "strikeouts":                    "Pitcher Strikeouts",
    "pitcher strikeouts":            "Pitcher Strikeouts",
    "hits":                          "Hits",
    "runs":                          "Runs",
    "rbis":                          "RBIs",
    "total bases":                   "Total Bases",
    "walks":                         "Walks",
    "earned runs allowed":           "Earned Runs Allowed",
    "hits allowed":                  "Hits Allowed",
    "hitter fantasy score":          "Hitter Fantasy Score",
    "pitching outs":                 "Pitching Outs",
    "batting runs":                  "Runs",

    # ── Hockey (NHL) ──
    # PrizePicks: Shots on Goal, Goals, Saves, Points, Assists, Time On Ice,
    #   Blocked Shots (NHL context)
    "shots on goal":                 "Shots on Goal",
    "goals":                         "Goals",
    "saves":                         "Saves",
    "total saves":                   "Saves",
    "total goals":                   "Goals",
    "points (nhl)":                  "Points",
    "assists (nhl)":                 "Assists",
    "time on ice":                   "Time On Ice",
    "shots":                         "Shots on Goal",
    # FanDuel market type keys (lowercased with underscores replaced)
    "player total shots":            "Shots on Goal",
    "player total saves":            "Saves",
    "player total goals":            "Goals",
    "player total assists":          "Assists",
    "player total points":           "Points",
}
