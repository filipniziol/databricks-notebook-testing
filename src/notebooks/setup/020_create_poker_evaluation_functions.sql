-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Poker Hand Evaluation Functions
-- MAGIC 
-- MAGIC Python UDFs for evaluating and comparing poker hands.
-- MAGIC 
-- MAGIC Functions:
-- MAGIC - `evaluate_hand(hole_cards, board)` - Returns hand rank and name
-- MAGIC - `compare_hands(hero_cards, opponent_cards_array, board)` - Returns winner position (0=hero, 1+=opponent index, -1=tie)

-- COMMAND ----------

-- Create utils schema if not exists
CREATE SCHEMA IF NOT EXISTS poker.utils;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Hand Evaluation Function

-- COMMAND ----------

CREATE OR REPLACE FUNCTION poker.utils.evaluate_hand(
    hole_cards STRING,  -- e.g. "AhKd" or "Ah Kd"
    board STRING        -- e.g. "2c5d7hJsQc" or ["2c","5d","7h","Js","Qc"]
)
RETURNS STRUCT<rank: INT, hand_class: INT, hand_name: STRING>
LANGUAGE PYTHON
COMMENT 'Evaluates a poker hand. Lower rank = better hand. hand_class: 1=straight flush, 2=quads, ..., 9=high card'
AS $$
from itertools import combinations

def parse_cards(cards_str):
    """Parse cards string into list of (rank, suit) tuples"""
    if not cards_str:
        return []
    cards_str = str(cards_str).replace('[', '').replace(']', '').replace('"', '').replace("'", '').replace(',', ' ')
    cards_str = cards_str.strip()
    parts = cards_str.split()
    if len(parts) == 1 and len(cards_str) >= 4:
        parts = [cards_str[i:i+2] for i in range(0, len(cards_str), 2)]
    cards = []
    rank_map = {'2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7, '8': 8, 
               '9': 9, 'T': 10, 't': 10, 'J': 11, 'j': 11, 'Q': 12, 'q': 12, 
               'K': 13, 'k': 13, 'A': 14, 'a': 14, '1': 10}
    for p in parts:
        p = p.strip()
        if len(p) >= 2:
            rank_char = p[0].upper()
            suit_char = p[1].lower()
            if rank_char in rank_map and suit_char in ['h', 'd', 'c', 's']:
                cards.append((rank_map[rank_char], suit_char))
    return cards

def evaluate_5_cards(cards):
    if len(cards) != 5:
        return (0, 0, "invalid", [])
    ranks = sorted([c[0] for c in cards], reverse=True)
    suits = [c[1] for c in cards]
    is_flush = len(set(suits)) == 1
    unique_ranks = sorted(set(ranks), reverse=True)
    is_straight = False
    straight_high = 0
    if len(unique_ranks) == 5:
        if unique_ranks[0] - unique_ranks[4] == 4:
            is_straight = True
            straight_high = unique_ranks[0]
        elif unique_ranks == [14, 5, 4, 3, 2]:
            is_straight = True
            straight_high = 5
    rank_counts = {}
    for r in ranks:
        rank_counts[r] = rank_counts.get(r, 0) + 1
    counts = sorted(rank_counts.values(), reverse=True)
    
    # Return (rank, hand_class, hand_name, high_cards) - lower rank = better
    if is_straight and is_flush:
        if straight_high == 14:
            return (1, 1, "Royal Flush", [straight_high])
        return (2, 1, "Straight Flush", [straight_high])
    if counts == [4, 1]:
        quad = [r for r, c in rank_counts.items() if c == 4][0]
        return (3, 2, "Four of a Kind", [quad])
    if counts == [3, 2]:
        trips = [r for r, c in rank_counts.items() if c == 3][0]
        pair = [r for r, c in rank_counts.items() if c == 2][0]
        return (4, 3, "Full House", [trips, pair])
    if is_flush:
        return (5, 4, "Flush", ranks)
    if is_straight:
        return (6, 5, "Straight", [straight_high])
    if counts == [3, 1, 1]:
        trips = [r for r, c in rank_counts.items() if c == 3][0]
        kickers = sorted([r for r, c in rank_counts.items() if c == 1], reverse=True)
        return (7, 6, "Three of a Kind", [trips] + kickers)
    if counts == [2, 2, 1]:
        pairs = sorted([r for r, c in rank_counts.items() if c == 2], reverse=True)
        kicker = [r for r, c in rank_counts.items() if c == 1][0]
        return (8, 7, "Two Pair", pairs + [kicker])
    if counts == [2, 1, 1, 1]:
        pair = [r for r, c in rank_counts.items() if c == 2][0]
        kickers = sorted([r for r, c in rank_counts.items() if c == 1], reverse=True)
        return (9, 8, "One Pair", [pair] + kickers)
    return (10, 9, "High Card", ranks)

def best_hand(hole, board_cards):
    all_cards = hole + board_cards
    best = (999, 0, "invalid", [])
    for combo in combinations(all_cards, 5):
        result = evaluate_5_cards(list(combo))
        if result[0] < best[0] or (result[0] == best[0] and result[3] > best[3]):
            best = result
    return best

try:
    hole = parse_cards(hole_cards)
    board_parsed = parse_cards(board)
    if len(hole) < 2 or len(board_parsed) < 3:
        return {"rank": 9999, "hand_class": 0, "hand_name": "invalid"}
    rank, hand_class, hand_name, _ = best_hand(hole, board_parsed)
    return {"rank": rank, "hand_class": hand_class, "hand_name": hand_name}
except Exception as e:
    return {"rank": 9999, "hand_class": 0, "hand_name": f"error: {str(e)}"}
$$;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Compare Hero vs Multiple Opponents
-- MAGIC Returns winner: 0 = hero wins, 1+ = opponent index wins, -1 = tie

-- COMMAND ----------

CREATE OR REPLACE FUNCTION poker.utils.compare_hands(
    hero_cards STRING,           -- Hero hole cards, e.g. "AhKd"
    opponent_cards ARRAY<STRING>, -- Array of opponent cards, e.g. ["JcJs", "QhQd"]
    board STRING                 -- Community cards
)
RETURNS STRUCT<
    winner_index: INT,           -- 0=hero, 1+=opponent index, -1=tie with hero
    hero_rank: INT,
    hero_hand: STRING,
    winning_rank: INT,
    winning_hand: STRING,
    hero_would_win: BOOLEAN
>
LANGUAGE PYTHON
COMMENT 'Compares hero vs multiple opponents. winner_index: 0=hero wins, 1,2,3...=opponent index, -1=hero ties for best'
AS $$
from itertools import combinations

def parse_cards(cards_str):
    if not cards_str:
        return []
    cards_str = str(cards_str).replace('[', '').replace(']', '').replace('"', '').replace("'", '').replace(',', ' ')
    cards_str = cards_str.strip()
    parts = cards_str.split()
    if len(parts) == 1 and len(cards_str) >= 4:
        parts = [cards_str[i:i+2] for i in range(0, len(cards_str), 2)]
    cards = []
    rank_map = {'2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7, '8': 8, 
               '9': 9, 'T': 10, 't': 10, 'J': 11, 'j': 11, 'Q': 12, 'q': 12, 
               'K': 13, 'k': 13, 'A': 14, 'a': 14, '1': 10}
    for p in parts:
        p = p.strip()
        if len(p) >= 2:
            rank_char = p[0].upper()
            suit_char = p[1].lower()
            if rank_char in rank_map and suit_char in ['h', 'd', 'c', 's']:
                cards.append((rank_map[rank_char], suit_char))
    return cards

def evaluate_5_cards(cards):
    if len(cards) != 5:
        return (999, "invalid", [])
    ranks = sorted([c[0] for c in cards], reverse=True)
    suits = [c[1] for c in cards]
    is_flush = len(set(suits)) == 1
    unique_ranks = sorted(set(ranks), reverse=True)
    is_straight = False
    straight_high = 0
    if len(unique_ranks) == 5:
        if unique_ranks[0] - unique_ranks[4] == 4:
            is_straight = True
            straight_high = unique_ranks[0]
        elif unique_ranks == [14, 5, 4, 3, 2]:
            is_straight = True
            straight_high = 5
    rank_counts = {}
    for r in ranks:
        rank_counts[r] = rank_counts.get(r, 0) + 1
    counts = sorted(rank_counts.values(), reverse=True)
    
    if is_straight and is_flush:
        return (1 if straight_high == 14 else 2, "Straight Flush", [straight_high])
    if counts == [4, 1]:
        quad = [r for r, c in rank_counts.items() if c == 4][0]
        return (3, "Four of a Kind", [quad])
    if counts == [3, 2]:
        trips = [r for r, c in rank_counts.items() if c == 3][0]
        pair = [r for r, c in rank_counts.items() if c == 2][0]
        return (4, "Full House", [trips, pair])
    if is_flush:
        return (5, "Flush", ranks)
    if is_straight:
        return (6, "Straight", [straight_high])
    if counts == [3, 1, 1]:
        trips = [r for r, c in rank_counts.items() if c == 3][0]
        kickers = sorted([r for r, c in rank_counts.items() if c == 1], reverse=True)
        return (7, "Three of a Kind", [trips] + kickers)
    if counts == [2, 2, 1]:
        pairs = sorted([r for r, c in rank_counts.items() if c == 2], reverse=True)
        kicker = [r for r, c in rank_counts.items() if c == 1][0]
        return (8, "Two Pair", pairs + [kicker])
    if counts == [2, 1, 1, 1]:
        pair = [r for r, c in rank_counts.items() if c == 2][0]
        kickers = sorted([r for r, c in rank_counts.items() if c == 1], reverse=True)
        return (9, "One Pair", [pair] + kickers)
    return (10, "High Card", ranks)

def best_hand(hole, board_cards):
    all_cards = hole + board_cards
    best = (999, "invalid", [])
    for combo in combinations(all_cards, 5):
        result = evaluate_5_cards(list(combo))
        if result[0] < best[0] or (result[0] == best[0] and result[2] > best[2]):
            best = result
    return best

try:
    hero = parse_cards(hero_cards)
    board_parsed = parse_cards(board)
    
    if len(hero) < 2 or len(board_parsed) < 5:
        return {
            "winner_index": -99, "hero_rank": 9999, "hero_hand": "invalid",
            "winning_rank": 9999, "winning_hand": "invalid", "hero_would_win": None
        }
    
    hero_rank, hero_hand, hero_high = best_hand(hero, board_parsed)
    best_rank = hero_rank
    best_hand_name = hero_hand
    best_high = hero_high
    winner_index = 0
    
    if opponent_cards:
        for i, opp_str in enumerate(opponent_cards):
            if not opp_str:
                continue
            opp = parse_cards(opp_str)
            if len(opp) >= 2:
                opp_rank, opp_hand, opp_high = best_hand(opp, board_parsed)
                if opp_rank < best_rank or (opp_rank == best_rank and opp_high > best_high):
                    best_rank = opp_rank
                    best_hand_name = opp_hand
                    best_high = opp_high
                    winner_index = i + 1
                elif opp_rank == best_rank and opp_high == best_high and winner_index == 0:
                    winner_index = -1
    
    hero_would_win = (hero_rank < best_rank) or (hero_rank == best_rank and hero_high >= best_high)
    
    return {
        "winner_index": winner_index,
        "hero_rank": hero_rank,
        "hero_hand": hero_hand,
        "winning_rank": best_rank,
        "winning_hand": best_hand_name,
        "hero_would_win": hero_would_win
    }
except Exception as e:
    return {
        "winner_index": -99, "hero_rank": 9999, "hero_hand": f"error: {str(e)}",
        "winning_rank": 9999, "winning_hand": "error", "hero_would_win": None
    }
$$;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Test Functions

-- COMMAND ----------

-- Test evaluate_hand (flush)
SELECT poker.utils.evaluate_hand('AhKh', '2h5h7hJsQc') as result;

-- COMMAND ----------

-- Test compare_hands: Hero AA vs opponents [KK, QQ]
SELECT poker.utils.compare_hands(
    'AhAd',                    -- Hero: AA
    ARRAY('KcKs', 'QhQd'),     -- Opponents: KK, QQ
    '2h5c7d9sJc'               -- Board
) as result;
-- Should return winner_index=0 (hero wins with AA)

-- COMMAND ----------

-- Test compare_hands: Hero AK vs opponent JJ on board with J
SELECT poker.utils.compare_hands(
    'AhKd',                    -- Hero: AK
    ARRAY('JcJs'),             -- Opponent: JJ
    'Jh2s5c8dQh'               -- Board (J on board = trips for opponent)
) as result;
-- Should return winner_index=1 (opponent wins with trips)

-- COMMAND ----------

-- Test compare_hands: Hero vs multiple - hero loses
SELECT poker.utils.compare_hands(
    '2h3d',                    -- Hero: 23 (weak)
    ARRAY('AcAs', 'KhKd'),     -- Opponents: AA, KK
    '4c5s6d7hJc'               -- Board
) as result;
-- Hero has straight (2-3-4-5-6), should win!
