-- Migration: 013_create_table_silver_hand_players
-- Description: Players per hand - allows calculating VPIP, PFR, etc. for all players
-- Source: bronze.hand_history -> Seat lines + actions
-- KEY: seat number allows mapping anonymized player to screenshot player at same seat

CREATE TABLE IF NOT EXISTS poker.silver.hand_players (
    -- Composite primary key
    hand_id                 STRING          NOT NULL    COMMENT 'FK to silver.hands',
    seat                    INT             NOT NULL    COMMENT 'Seat number (1-9) - KEY FOR PLAYER MAPPING',
    
    -- Player identification (anonymized in history, real in screenshot)
    player_name             STRING                      COMMENT 'Player name (anonymized hash or Hero)',
    is_hero                 BOOLEAN                     COMMENT 'Is this the hero',
    
    -- Position
    position                STRING                      COMMENT 'Position: BTN, SB, BB, UTG, MP, CO',
    is_button               BOOLEAN                     COMMENT 'Is dealer button',
    is_sb                   BOOLEAN                     COMMENT 'Is small blind',
    is_bb                   BOOLEAN                     COMMENT 'Is big blind',
    
    -- Stack
    chips_start             DECIMAL(10,2)               COMMENT 'Starting chips',
    chips_end               DECIMAL(10,2)               COMMENT 'Ending chips (after hand)',
    
    -- Hole cards (if shown)
    hole_cards              STRING                      COMMENT 'Hole cards if shown (e.g. QdQs)',
    
    -- Hand result
    went_to_showdown        BOOLEAN                     COMMENT 'Did player go to showdown',
    won_hand                BOOLEAN                     COMMENT 'Did player win the hand',
    amount_won              DECIMAL(10,2)               COMMENT 'Amount won (0 if lost)',
    result_description      STRING                      COMMENT 'e.g. "won with pair of Aces"',
    
    -- Preflop actions (for VPIP/PFR calculation)
    vpip                    BOOLEAN                     COMMENT 'Voluntarily Put $ In Pot (called or raised preflop)',
    pfr                     BOOLEAN                     COMMENT 'Preflop Raise (raised preflop)',
    three_bet               BOOLEAN                     COMMENT 'Made a 3-bet preflop',
    
    -- Action summary per street
    preflop_actions         ARRAY<STRING>               COMMENT 'Preflop actions [fold/call/raise X]',
    flop_actions            ARRAY<STRING>               COMMENT 'Flop actions',
    turn_actions            ARRAY<STRING>               COMMENT 'Turn actions',
    river_actions           ARRAY<STRING>               COMMENT 'River actions',
    
    -- Aggression
    total_bets              INT                         COMMENT 'Number of bets made',
    total_raises            INT                         COMMENT 'Number of raises made',
    total_calls             INT                         COMMENT 'Number of calls made',
    went_allin              BOOLEAN                     COMMENT 'Went all-in during hand'
)
USING DELTA
COMMENT 'Silver layer: Players per hand with stats for VPIP/PFR calculation. Use seat to map anonymous -> screenshot player'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
