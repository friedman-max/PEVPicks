-- Create market_observatory table
CREATE TABLE IF NOT EXISTS market_observatory (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    market_key TEXT UNIQUE NOT NULL, -- formatted as "player|league|prop|line|side|game_start"
    player TEXT NOT NULL,
    league TEXT NOT NULL,
    prop TEXT NOT NULL,
    line FLOAT NOT NULL,
    side TEXT NOT NULL,
    true_prob FLOAT NOT NULL,
    game_start TIMESTAMPTZ NOT NULL,
    result TEXT DEFAULT 'pending', -- pending, hit, miss, push, dnp
    stat_actual FLOAT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for resolution performance
CREATE INDEX IF NOT EXISTS idx_observatory_pending ON market_observatory(result) WHERE result = 'pending';
CREATE INDEX IF NOT EXISTS idx_observatory_calibration ON market_observatory(league, prop, result);
