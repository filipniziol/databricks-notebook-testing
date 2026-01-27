# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Hand History to Silver
# MAGIC Parse hand history files from bronze and load to silver tables:
# MAGIC - `poker.silver.hands` - main hand info
# MAGIC - `poker.silver.hand_players` - players per hand
# MAGIC - `poker.silver.hand_actions` - actions per hand
# MAGIC 
# MAGIC **Source:** `poker.bronze.hand_history`
# MAGIC Each row contains all hands from one tournament.

# COMMAND ----------

import sys
import os

# Add src folder to path for package imports
notebook_path = os.getcwd()
src_path = os.path.dirname(os.path.dirname(notebook_path))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, explode, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, 
    BooleanType, ArrayType, LongType, TimestampType
)
from package.gg_parser import GGHandHistoryParser

# COMMAND ----------

# Configuration
SOURCE_TABLE = "poker.bronze.hand_history"
TARGET_HANDS = "poker.silver.hands"
TARGET_PLAYERS = "poker.silver.hand_players"
TARGET_ACTIONS = "poker.silver.hand_actions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Parsing Function

# COMMAND ----------

def parse_hand_history(raw_content: str, file_name: str) -> list:
    """Parse raw hand history text into list of structured dicts.
    
    Returns list of dicts, each containing:
    - hand: dict with main hand info
    - players: list of player dicts
    - actions: list of action dicts
    """
    results = []
    try:
        parser = GGHandHistoryParser(text=raw_content)
        hands = parser.parse()
        
        for hand in hands:
            hand_dict = hand.to_dict()
            
            # Check if hero folded
            hero_folded = False
            hero_result_info = next(
                (r for r in hand_dict["results"] if r["player_name"] == hand.hero_name),
                None
            ) if hand.hero_name else None
            if hero_result_info and hero_result_info.get("folded"):
                hero_folded = True
            
            # Determine hero result
            if hand_dict["hero_won"]:
                hero_result = "won"
            elif hero_folded:
                hero_result = "folded"
            else:
                hero_result = "lost"  # Went to showdown and lost
            
            # Build main hand record
            hand_record = {
                "hand_id": hand_dict["hand_id"],
                "tournament_id": int(hand_dict["tournament_id"]) if hand_dict["tournament_id"] else None,
                "tournament_name": hand_dict["tournament_name"],
                "game_type": hand_dict["game_type"],
                "level": hand_dict["level"],
                "small_blind": float(hand_dict["sb"]),
                "big_blind": float(hand_dict["bb"]),
                "ante": float(hand_dict["ante"]),
                "table_name": hand_dict["table_name"],
                "max_seats": hand_dict["max_players"],
                "button_seat": hand_dict["button_seat"],
                "hand_timestamp": hand_dict["timestamp"],
                "board": hand_dict["board"],
                "total_pot": float(hand_dict["total_pot"]),
                "rake": float(hand_dict["rake"]),
                "hero_seat": hand.hero.seat if hand.hero else None,
                "hero_position": hand_dict["hero_position"],
                "hero_cards": "".join(hand_dict["hero_cards"]) if hand_dict["hero_cards"] else None,
                "hero_chips_start": float(hand_dict["hero_stack"]) if hand_dict["hero_stack"] else None,
                "hero_result": hero_result,
                "file_name": file_name,
            }
            
            # Build player records
            player_records = []
            for p in hand_dict["players"]:
                # Find result for this player
                player_result = next(
                    (r for r in hand_dict["results"] if r["player_name"] == p["name"]),
                    None
                )
                
                # Get preflop actions for VPIP/PFR
                player_preflop = [
                    a for a in hand_dict["preflop_actions"]
                    if a["player"] == p["name"]
                ]
                vpip = any(a["action"] in ("call", "raise", "bet") for a in player_preflop)
                pfr = any(a["action"] in ("raise", "bet") for a in player_preflop)
                
                # Count 3-bets (raise after a raise)
                three_bet = False
                raise_count = 0
                for a in hand_dict["preflop_actions"]:
                    if a["action"] == "raise":
                        raise_count += 1
                        if a["player"] == p["name"] and raise_count >= 2:
                            three_bet = True
                            break
                
                player_record = {
                    "hand_id": hand_dict["hand_id"],
                    "seat": p["seat"],
                    "player_name": p["name"],
                    "is_hero": p["is_hero"],
                    "position": p["position"],
                    "is_button": p["is_button"],
                    "is_sb": p["position"] == "SB",
                    "is_bb": p["position"] == "BB",
                    "chips_start": float(p["stack"]),
                    # hole_cards: prefer from results (showdown cards) over players (dealt cards)
                    "hole_cards": (
                        "".join(player_result["hole_cards"]) if player_result and player_result.get("hole_cards") 
                        else ("".join(p["hole_cards"]) if p["hole_cards"] else None)
                    ),
                    "went_to_showdown": player_result and not player_result.get("folded", True) if player_result else False,
                    "won_hand": player_result and player_result.get("won_amount", 0) > 0 if player_result else False,
                    "amount_won": float(player_result["won_amount"]) if player_result and player_result.get("won_amount") else 0.0,
                    "result_description": player_result.get("hand_description") if player_result else None,
                    "vpip": vpip,
                    "pfr": pfr,
                    "three_bet": three_bet,
                    "preflop_actions": [a["action"] + (f" {a['amount']}" if a.get("amount") else "") for a in player_preflop],
                    "total_bets": sum(1 for a in hand_dict.get("flop_actions", []) + hand_dict.get("turn_actions", []) + hand_dict.get("river_actions", []) if a["player"] == p["name"] and a["action"] == "bet"),
                    "total_raises": sum(1 for a in hand_dict.get("preflop_actions", []) + hand_dict.get("flop_actions", []) + hand_dict.get("turn_actions", []) + hand_dict.get("river_actions", []) if a["player"] == p["name"] and a["action"] == "raise"),
                    "total_calls": sum(1 for a in hand_dict.get("preflop_actions", []) + hand_dict.get("flop_actions", []) + hand_dict.get("turn_actions", []) + hand_dict.get("river_actions", []) if a["player"] == p["name"] and a["action"] == "call"),
                }
                player_records.append(player_record)
            
            # Build action records
            action_records = []
            action_order = 0
            
            for street, actions in [
                ("preflop", hand_dict["preflop_actions"]),
                ("flop", hand_dict["flop_actions"]),
                ("turn", hand_dict["turn_actions"]),
                ("river", hand_dict["river_actions"]),
            ]:
                for action in actions:
                    action_order += 1
                    # Find seat for player
                    player_seat = next(
                        (p["seat"] for p in hand_dict["players"] if p["name"] == action["player"]),
                        None
                    )
                    is_hero = action["player"] == "Hero"
                    
                    action_record = {
                        "hand_id": hand_dict["hand_id"],
                        "action_order": action_order,
                        "street": street,
                        "seat": player_seat,
                        "player_name": action["player"],
                        "is_hero": is_hero,
                        "action_type": action["action"],
                        "amount": float(action["amount"]) if action.get("amount") else None,
                        "is_allin": action.get("is_all_in", False),
                        "raise_to": None,  # Could be parsed from raw line
                        "raw_line": None,
                    }
                    action_records.append(action_record)
            
            results.append({
                "hand": hand_record,
                "players": player_records,
                "actions": action_records,
            })
            
    except Exception as e:
        print(f"Parse error: {e}")
    
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze and Parse

# COMMAND ----------

# Read bronze
df_bronze = spark.read.table(SOURCE_TABLE)

# Collect and parse (can't use UDF with complex nested return easily)
bronze_data = df_bronze.select("file_name", "raw_content").collect()

all_hands = []
all_players = []
all_actions = []

for row in bronze_data:
    file_name = row["file_name"]
    raw_content = row["raw_content"]
    
    parsed = parse_hand_history(raw_content, file_name)
    for p in parsed:
        all_hands.append(p["hand"])
        all_players.extend(p["players"])
        all_actions.extend(p["actions"])

print(f"Parsed {len(all_hands)} hands, {len(all_players)} player records, {len(all_actions)} actions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create DataFrames

# COMMAND ----------

# Schema for hands
hands_schema = StructType([
    StructField("hand_id", StringType(), False),
    StructField("tournament_id", LongType(), True),
    StructField("tournament_name", StringType(), True),
    StructField("game_type", StringType(), True),
    StructField("level", IntegerType(), True),
    StructField("small_blind", DoubleType(), True),
    StructField("big_blind", DoubleType(), True),
    StructField("ante", DoubleType(), True),
    StructField("table_name", StringType(), True),
    StructField("max_seats", IntegerType(), True),
    StructField("button_seat", IntegerType(), True),
    StructField("hand_timestamp", StringType(), True),  # Will convert to timestamp
    StructField("board", ArrayType(StringType()), True),
    StructField("total_pot", DoubleType(), True),
    StructField("rake", DoubleType(), True),
    StructField("hero_seat", IntegerType(), True),
    StructField("hero_position", StringType(), True),
    StructField("hero_cards", StringType(), True),
    StructField("hero_chips_start", DoubleType(), True),
    StructField("hero_result", StringType(), True),
    StructField("file_name", StringType(), True),
])

# Schema for players
players_schema = StructType([
    StructField("hand_id", StringType(), False),
    StructField("seat", IntegerType(), False),
    StructField("player_name", StringType(), True),
    StructField("is_hero", BooleanType(), True),
    StructField("position", StringType(), True),
    StructField("is_button", BooleanType(), True),
    StructField("is_sb", BooleanType(), True),
    StructField("is_bb", BooleanType(), True),
    StructField("chips_start", DoubleType(), True),
    StructField("hole_cards", StringType(), True),
    StructField("went_to_showdown", BooleanType(), True),
    StructField("won_hand", BooleanType(), True),
    StructField("amount_won", DoubleType(), True),
    StructField("result_description", StringType(), True),
    StructField("vpip", BooleanType(), True),
    StructField("pfr", BooleanType(), True),
    StructField("three_bet", BooleanType(), True),
    StructField("preflop_actions", ArrayType(StringType()), True),
    StructField("total_bets", IntegerType(), True),
    StructField("total_raises", IntegerType(), True),
    StructField("total_calls", IntegerType(), True),
])

# Schema for actions
actions_schema = StructType([
    StructField("hand_id", StringType(), False),
    StructField("action_order", IntegerType(), False),
    StructField("street", StringType(), True),
    StructField("seat", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("is_hero", BooleanType(), True),
    StructField("action_type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("is_allin", BooleanType(), True),
    StructField("raise_to", DoubleType(), True),
    StructField("raw_line", StringType(), True),
])

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

# Create DataFrames
df_hands = spark.createDataFrame(all_hands, hands_schema)
df_players = spark.createDataFrame(all_players, players_schema)
df_actions = spark.createDataFrame(all_actions, actions_schema)

# Convert timestamp
df_hands = df_hands.withColumn(
    "hand_timestamp", 
    to_timestamp(col("hand_timestamp"))
).withColumn(
    "ingested_at",
    current_timestamp()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver

# COMMAND ----------

# Write hands
df_hands.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_HANDS)

print(f"✅ Wrote {df_hands.count()} records to {TARGET_HANDS}")

# COMMAND ----------

# Write players
df_players.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_PLAYERS)

print(f"✅ Wrote {df_players.count()} records to {TARGET_PLAYERS}")

# COMMAND ----------

# Write actions
df_actions.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_ACTIONS)

print(f"✅ Wrote {df_actions.count()} records to {TARGET_ACTIONS}")
