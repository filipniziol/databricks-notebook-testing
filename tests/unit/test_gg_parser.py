"""
Unit tests for GGPoker Hand History and Tournament Summary Parser
"""

import pytest
from datetime import datetime
from decimal import Decimal

import sys
sys.path.insert(0, str(__file__).replace('\\tests\\unit\\test_gg_parser.py', '\\src\\package'))

from gg_parser import (
    GGHandHistoryParser,
    GGTournamentSummaryParser,
    Hand,
    TournamentSummary,
    Player,
    Action,
    ActionType,
    Street,
    parse_hand_history_file,
    parse_tournament_summary_file,
)


# =============================================================================
# Test Data - Sample Hand History
# =============================================================================

SAMPLE_HAND_SIMPLE = """
Poker Hand #BR993472125: Tournament #257961381, Mystery Battle Royale $10 Hold'em No Limit - Level8(100/200) - 2026/01/15 02:59:14
Table '1' 9-max Seat #1 is the button
Seat 1: 95dbb9e5 (792 in chips)
Seat 2: 602db36a (7,908 in chips)
Seat 3: 6f34516f (4,239 in chips)
Seat 4: 9419d3d2 (1,691 in chips)
Seat 5: Hero (1,749 in chips)
Seat 6: f150c4cc (1,621 in chips)
95dbb9e5: posts the ante 40
6f34516f: posts the ante 40
Hero: posts the ante 40
f150c4cc: posts the ante 40
602db36a: posts the ante 40
9419d3d2: posts the ante 40
602db36a: posts small blind 100
6f34516f: posts big blind 200
*** HOLE CARDS ***
Dealt to 95dbb9e5 
Dealt to 602db36a 
Dealt to 6f34516f 
Dealt to 9419d3d2 
Dealt to Hero [6d 7d]
Dealt to f150c4cc 
9419d3d2: folds
Hero: raises 1,509 to 1,709 and is all-in
f150c4cc: calls 1,581 and is all-in
95dbb9e5: folds
602db36a: raises 5,439 to 7,148
6f34516f: folds
Uncalled bet (5,439) returned to 602db36a
602db36a: shows [4s Ts]
Hero: shows [6d 7d]
f150c4cc: shows [Ah 5h]
*** FLOP *** [2d Jh Td]
*** TURN *** [2d Jh Td] [4c]
*** RIVER *** [2d Jh Td 4c] [2h]
*** SHOWDOWN ***
602db36a collected 5,183 from pot
602db36a collected 256 from pot
*** SUMMARY ***
Total pot 5,439 | Rake 0 | Jackpot 0 | Bingo 0 | Fortune 0 | Tax 0
Board [2d Jh Td 4c 2h]
Seat 1: 95dbb9e5 (button) folded before Flop
Seat 2: 602db36a (small blind) showed [4s Ts] and won (5,439) with two pair, Tens and Fours
Seat 3: 6f34516f (big blind) folded before Flop
Seat 4: 9419d3d2 folded before Flop
Seat 5: Hero showed [6d 7d] and lost with a pair of Twos
Seat 6: f150c4cc showed [Ah 5h] and lost with a pair of Twos
""".strip()


SAMPLE_HAND_POSTFLOP = """
Poker Hand #BR993471891: Tournament #257961381, Mystery Battle Royale $10 Hold'em No Limit - Level7(75/150) - 2026/01/15 02:56:08
Table '1' 9-max Seat #4 is the button
Seat 1: 95dbb9e5 (1,997 in chips)
Seat 2: 602db36a (4,392 in chips)
Seat 3: 6f34516f (1,423 in chips)
Seat 4: 9419d3d2 (1,801 in chips)
Seat 5: Hero (1,934 in chips)
Seat 6: f150c4cc (2,131 in chips)
Seat 7: ad9d1fb4 (2,441 in chips)
Seat 8: 1cfe895 (1,881 in chips)
ad9d1fb4: posts the ante 30
95dbb9e5: posts the ante 30
1cfe895: posts the ante 30
6f34516f: posts the ante 30
Hero: posts the ante 30
f150c4cc: posts the ante 30
602db36a: posts the ante 30
9419d3d2: posts the ante 30
Hero: posts small blind 75
f150c4cc: posts big blind 150
*** HOLE CARDS ***
Dealt to Hero [2d 9s]
ad9d1fb4: raises 150 to 300
1cfe895: folds
95dbb9e5: folds
602db36a: folds
6f34516f: folds
9419d3d2: folds
Hero: folds
f150c4cc: calls 150
*** FLOP *** [Ac 3s Jh]
f150c4cc: checks
ad9d1fb4: bets 375
f150c4cc: folds
Uncalled bet (375) returned to ad9d1fb4
ad9d1fb4 collected 915 from pot
*** SUMMARY ***
Total pot 915 | Rake 0 | Jackpot 0 | Bingo 0 | Fortune 0 | Tax 0
Board [Ac 3s Jh]
Seat 5: Hero (small blind) folded before Flop
Seat 6: f150c4cc (big blind) folded on the Flop
Seat 7: ad9d1fb4 collected (915)
""".strip()


SAMPLE_TOURNAMENT_SUMMARY = """
Tournament #257942312, Mystery Battle Royale $10, Hold'em No Limit
Buy-in: $5+$0.8+$4.2
18 Players
Total Prize Pool: $165.6
Tournament started 2026/01/15 00:22:55 
2nd : Hero, $30
You finished the tournament in 2nd place.
You received a total of $30.
""".strip()


SAMPLE_TOURNAMENT_SUMMARY_BUST = """
Tournament #260520685, Mystery Battle Royale $25, Hold'em No Limit
Buy-in: $12.5+$2+$10.5
18 Players
Total Prize Pool: $414
Tournament started 2026/01/25 23:41:11 
4th : Hero, $0
You finished the tournament in 4th place.
You received a total of $0.
""".strip()


SAMPLE_MULTIPLE_HANDS = SAMPLE_HAND_SIMPLE + "\n\n\n" + SAMPLE_HAND_POSTFLOP


# =============================================================================
# Hand History Parser Tests
# =============================================================================

class TestGGHandHistoryParser:
    """Tests for GGHandHistoryParser"""
    
    def test_parse_single_hand(self):
        """Test parsing a single hand"""
        parser = GGHandHistoryParser(text=SAMPLE_HAND_SIMPLE)
        hands = parser.parse()
        
        assert len(hands) == 1
        hand = hands[0]
        assert isinstance(hand, Hand)
    
    def test_parse_multiple_hands(self):
        """Test parsing multiple hands from one file"""
        parser = GGHandHistoryParser(text=SAMPLE_MULTIPLE_HANDS)
        hands = parser.parse()
        
        assert len(hands) == 2
    
    def test_hand_header_parsing(self):
        """Test parsing of hand header information"""
        parser = GGHandHistoryParser(text=SAMPLE_HAND_SIMPLE)
        hands = parser.parse()
        hand = hands[0]
        
        assert hand.hand_id == "BR993472125"
        assert hand.tournament_id == "257961381"
        assert hand.tournament_name == "Mystery Battle Royale"
        assert hand.buy_in == Decimal("10")
        assert hand.game_type == "Hold'em No Limit"
        assert hand.level == 8
        assert hand.blinds == (100, 200)
        assert hand.timestamp == datetime(2026, 1, 15, 2, 59, 14)
    
    def test_table_info_parsing(self):
        """Test parsing of table information"""
        parser = GGHandHistoryParser(text=SAMPLE_HAND_SIMPLE)
        hands = parser.parse()
        hand = hands[0]
        
        assert hand.table_name == "1"
        assert hand.max_players == 9
        assert hand.button_seat == 1
    
    def test_players_parsing(self):
        """Test parsing of player information"""
        parser = GGHandHistoryParser(text=SAMPLE_HAND_SIMPLE)
        hands = parser.parse()
        hand = hands[0]
        
        assert len(hand.players) == 6
        
        # Check Hero
        hero = hand.hero
        assert hero is not None
        assert hero.name == "Hero"
        assert hero.stack == 1749
        assert hero.is_hero == True
        assert hero.hole_cards == "6d 7d"
        
        # Check button player
        btn_player = next(p for p in hand.players if p.seat == 1)
        assert btn_player.name == "95dbb9e5"
        assert btn_player.stack == 792
        assert btn_player.position == "BTN"
    
    def test_hero_cards_parsing(self):
        """Test parsing of hero's hole cards"""
        parser = GGHandHistoryParser(text=SAMPLE_HAND_SIMPLE)
        hands = parser.parse()
        hand = hands[0]
        
        assert hand.hero_cards == "6d 7d"
        assert hand.hero_cards_list == ["6d", "7d"]
    
    def test_board_parsing(self):
        """Test parsing of board cards"""
        parser = GGHandHistoryParser(text=SAMPLE_HAND_SIMPLE)
        hands = parser.parse()
        hand = hands[0]
        
        assert hand.flop == "2d Jh Td"
        assert hand.turn == "4c"
        assert hand.river == "2h"
        assert hand.board == "2d Jh Td 4c 2h"
        assert hand.board_list == ["2d", "Jh", "Td", "4c", "2h"]
        assert hand.flop_list == ["2d", "Jh", "Td"]
    
    def test_flop_only_board(self):
        """Test board parsing when hand ends on flop"""
        parser = GGHandHistoryParser(text=SAMPLE_HAND_POSTFLOP)
        hands = parser.parse()
        hand = hands[0]
        
        assert hand.flop == "Ac 3s Jh"
        assert hand.turn is None
        assert hand.river is None
        assert hand.board_list == ["Ac", "3s", "Jh"]
    
    def test_preflop_actions_parsing(self):
        """Test parsing of preflop actions"""
        parser = GGHandHistoryParser(text=SAMPLE_HAND_SIMPLE)
        hands = parser.parse()
        hand = hands[0]
        
        # Should have actions (excluding antes/blinds in to_dict)
        assert len(hand.preflop_actions) > 0
        
        # Find Hero's raise action
        hero_raise = None
        for a in hand.preflop_actions:
            if a.player_name == "Hero" and a.action_type == ActionType.RAISE:
                hero_raise = a
                break
        
        assert hero_raise is not None
        assert hero_raise.amount == 1709
        assert hero_raise.is_all_in == True
    
    def test_postflop_actions_parsing(self):
        """Test parsing of postflop actions"""
        parser = GGHandHistoryParser(text=SAMPLE_HAND_POSTFLOP)
        hands = parser.parse()
        hand = hands[0]
        
        assert len(hand.flop_actions) > 0
        
        # Find bet action
        bet_action = None
        for a in hand.flop_actions:
            if a.action_type == ActionType.BET:
                bet_action = a
                break
        
        assert bet_action is not None
        assert bet_action.player_name == "ad9d1fb4"
        assert bet_action.amount == 375
    
    def test_ante_parsing(self):
        """Test parsing of ante amount"""
        parser = GGHandHistoryParser(text=SAMPLE_HAND_SIMPLE)
        hands = parser.parse()
        hand = hands[0]
        
        assert hand.ante == 40
    
    def test_pot_parsing(self):
        """Test parsing of total pot"""
        parser = GGHandHistoryParser(text=SAMPLE_HAND_SIMPLE)
        hands = parser.parse()
        hand = hands[0]
        
        assert hand.total_pot == 5439
        assert hand.rake == 0
    
    def test_winners_parsing(self):
        """Test parsing of winners"""
        parser = GGHandHistoryParser(text=SAMPLE_HAND_SIMPLE)
        hands = parser.parse()
        hand = hands[0]
        
        assert "602db36a" in hand.winners
        assert hand.hero_won == False
    
    def test_showdown_detection(self):
        """Test showdown detection"""
        parser = GGHandHistoryParser(text=SAMPLE_HAND_SIMPLE)
        hands = parser.parse()
        hand = hands[0]
        
        assert hand.went_to_showdown == True
    
    def test_no_showdown_detection(self):
        """Test no showdown detection when hand ends before showdown"""
        parser = GGHandHistoryParser(text=SAMPLE_HAND_POSTFLOP)
        hands = parser.parse()
        hand = hands[0]
        
        # This hand ended on flop without showdown
        assert hand.went_to_showdown == False
    
    def test_position_assignment(self):
        """Test position assignment for players"""
        parser = GGHandHistoryParser(text=SAMPLE_HAND_SIMPLE)
        hands = parser.parse()
        hand = hands[0]
        
        positions = {p.name: p.position for p in hand.players}
        
        assert positions["95dbb9e5"] == "BTN"  # Seat 1, button
        assert positions["602db36a"] == "SB"   # Seat 2
        assert positions["6f34516f"] == "BB"   # Seat 3
    
    def test_to_dict_format(self):
        """Test to_dict() output format"""
        parser = GGHandHistoryParser(text=SAMPLE_HAND_SIMPLE)
        hands = parser.parse()
        hand = hands[0]
        
        d = hand.to_dict()
        
        # Check key fields exist
        assert "hand_id" in d
        assert "tournament_id" in d
        assert "hero_cards" in d
        assert "players" in d
        assert "preflop_actions" in d
        assert "board" in d
        
        # Check hero_cards is a list
        assert isinstance(d["hero_cards"], list)
        assert d["hero_cards"] == ["6d", "7d"]
        
        # Check board is a list
        assert isinstance(d["board"], list)
        assert len(d["board"]) == 5
        
        # Check players have is_button flag
        btn_player = next(p for p in d["players"] if p["is_button"])
        assert btn_player["name"] == "95dbb9e5"
        
        # Check stack_bb is calculated
        assert d["hero_stack_bb"] == 8.7  # 1749 / 200 = 8.745
    
    def test_to_flat_dict_format(self):
        """Test to_flat_dict() output format"""
        parser = GGHandHistoryParser(text=SAMPLE_HAND_SIMPLE)
        hands = parser.parse()
        hand = hands[0]
        
        d = hand.to_flat_dict()
        
        # Check no nested structures
        assert "hero_card_1" in d
        assert "hero_card_2" in d
        assert d["hero_card_1"] == "6d"
        assert d["hero_card_2"] == "7d"
        
        # Check board cards are flat
        assert d["flop_1"] == "2d"
        assert d["flop_2"] == "Jh"
        assert d["flop_3"] == "Td"
        assert d["turn_card"] == "4c"
        assert d["river_card"] == "2h"
        
        # No nested players or actions
        assert "players" not in d
        assert "preflop_actions" not in d


# =============================================================================
# Tournament Summary Parser Tests
# =============================================================================

class TestGGTournamentSummaryParser:
    """Tests for GGTournamentSummaryParser"""
    
    def test_parse_tournament_summary(self):
        """Test parsing tournament summary"""
        parser = GGTournamentSummaryParser(text=SAMPLE_TOURNAMENT_SUMMARY)
        summary = parser.parse()
        
        assert summary is not None
        assert isinstance(summary, TournamentSummary)
    
    def test_tournament_id_parsing(self):
        """Test parsing of tournament ID"""
        parser = GGTournamentSummaryParser(text=SAMPLE_TOURNAMENT_SUMMARY)
        summary = parser.parse()
        
        assert summary.tournament_id == "257942312"
    
    def test_tournament_name_parsing(self):
        """Test parsing of tournament name"""
        parser = GGTournamentSummaryParser(text=SAMPLE_TOURNAMENT_SUMMARY)
        summary = parser.parse()
        
        assert summary.tournament_name == "Mystery Battle Royale"
    
    def test_buyin_parsing(self):
        """Test parsing of buy-in breakdown"""
        parser = GGTournamentSummaryParser(text=SAMPLE_TOURNAMENT_SUMMARY)
        summary = parser.parse()
        
        assert summary.buy_in_total == Decimal("10")
        assert summary.buy_in_prize == Decimal("5")
        assert summary.buy_in_fee == Decimal("0.8")
        assert summary.buy_in_bounty == Decimal("4.2")
    
    def test_players_count_parsing(self):
        """Test parsing of player count"""
        parser = GGTournamentSummaryParser(text=SAMPLE_TOURNAMENT_SUMMARY)
        summary = parser.parse()
        
        assert summary.total_players == 18
    
    def test_prize_pool_parsing(self):
        """Test parsing of prize pool"""
        parser = GGTournamentSummaryParser(text=SAMPLE_TOURNAMENT_SUMMARY)
        summary = parser.parse()
        
        assert summary.prize_pool == Decimal("165.6")
    
    def test_start_time_parsing(self):
        """Test parsing of tournament start time"""
        parser = GGTournamentSummaryParser(text=SAMPLE_TOURNAMENT_SUMMARY)
        summary = parser.parse()
        
        assert summary.start_time == datetime(2026, 1, 15, 0, 22, 55)
    
    def test_finish_position_parsing(self):
        """Test parsing of hero's finish position"""
        parser = GGTournamentSummaryParser(text=SAMPLE_TOURNAMENT_SUMMARY)
        summary = parser.parse()
        
        assert summary.hero_finish_position == 2
    
    def test_prize_parsing(self):
        """Test parsing of hero's prize"""
        parser = GGTournamentSummaryParser(text=SAMPLE_TOURNAMENT_SUMMARY)
        summary = parser.parse()
        
        assert summary.hero_prize == Decimal("30")
    
    def test_roi_calculation(self):
        """Test ROI calculation"""
        parser = GGTournamentSummaryParser(text=SAMPLE_TOURNAMENT_SUMMARY)
        summary = parser.parse()
        
        # ROI = (30 - 10) / 10 * 100 = 200%
        assert summary.roi == Decimal("200")
    
    def test_itm_positive(self):
        """Test ITM flag when hero won money"""
        parser = GGTournamentSummaryParser(text=SAMPLE_TOURNAMENT_SUMMARY)
        summary = parser.parse()
        
        assert summary.is_itm == True
    
    def test_itm_negative(self):
        """Test ITM flag when hero didn't win money"""
        parser = GGTournamentSummaryParser(text=SAMPLE_TOURNAMENT_SUMMARY_BUST)
        summary = parser.parse()
        
        assert summary.is_itm == False
        assert summary.hero_prize == Decimal("0")
        assert summary.hero_finish_position == 4
    
    def test_negative_roi(self):
        """Test negative ROI calculation"""
        parser = GGTournamentSummaryParser(text=SAMPLE_TOURNAMENT_SUMMARY_BUST)
        summary = parser.parse()
        
        # ROI = (0 - 25) / 25 * 100 = -100%
        assert summary.roi == Decimal("-100")
    
    def test_to_dict_format(self):
        """Test to_dict() output format"""
        parser = GGTournamentSummaryParser(text=SAMPLE_TOURNAMENT_SUMMARY)
        summary = parser.parse()
        
        d = summary.to_dict()
        
        assert d["tournament_id"] == "257942312"
        assert d["tournament_name"] == "Mystery Battle Royale"
        assert d["buy_in_total"] == 10.0
        assert d["hero_finish_position"] == 2
        assert d["hero_prize"] == 30.0
        assert d["roi"] == 200.0
        assert d["is_itm"] == True


# =============================================================================
# Edge Cases Tests
# =============================================================================

class TestEdgeCases:
    """Tests for edge cases and error handling"""
    
    def test_empty_text(self):
        """Test parsing empty text"""
        parser = GGHandHistoryParser(text="")
        hands = parser.parse()
        
        assert len(hands) == 0
    
    def test_none_text(self):
        """Test parsing None text"""
        parser = GGHandHistoryParser(text=None)
        hands = parser.parse()
        
        assert len(hands) == 0
    
    def test_invalid_hand_format(self):
        """Test parsing invalid hand format"""
        parser = GGHandHistoryParser(text="This is not a valid hand history")
        hands = parser.parse()
        
        assert len(hands) == 0
    
    def test_tournament_summary_none_text(self):
        """Test tournament summary with None text"""
        parser = GGTournamentSummaryParser(text=None)
        summary = parser.parse()
        
        assert summary is None


# =============================================================================
# Run tests
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
