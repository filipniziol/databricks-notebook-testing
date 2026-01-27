"""
GGPoker Hand History and Tournament Summary Parser

This module provides parsers for:
1. Hand History files - individual hands from tournaments
2. Tournament Summary files - tournament results
"""

import re
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Optional


class Street(Enum):
    PREFLOP = "preflop"
    FLOP = "flop"
    TURN = "turn"
    RIVER = "river"
    SHOWDOWN = "showdown"


class ActionType(Enum):
    FOLD = "fold"
    CHECK = "check"
    CALL = "call"
    BET = "bet"
    RAISE = "raise"
    ALL_IN = "all-in"
    POST_ANTE = "post_ante"
    POST_SB = "post_sb"
    POST_BB = "post_bb"
    SHOW = "show"
    MUCK = "muck"
    WIN = "win"
    RETURN = "return"


@dataclass
class Player:
    """Represents a player at the table"""
    name: str
    seat: int
    stack: int
    is_hero: bool = False
    hole_cards: Optional[str] = None  # e.g., "Ah Kd"
    position: Optional[str] = None  # button, sb, bb, etc.
    
    def __repr__(self):
        cards = f" [{self.hole_cards}]" if self.hole_cards else ""
        return f"Player(seat={self.seat}, name='{self.name}', stack={self.stack}{cards})"


@dataclass
class Action:
    """Represents a single action in a hand"""
    player_name: str
    action_type: ActionType
    amount: Optional[int] = None
    is_all_in: bool = False
    street: Street = Street.PREFLOP
    
    def __repr__(self):
        amount_str = f" {self.amount}" if self.amount else ""
        all_in_str = " (all-in)" if self.is_all_in else ""
        return f"Action({self.player_name}: {self.action_type.value}{amount_str}{all_in_str})"


@dataclass
class HandResult:
    """Result for a single player in a hand"""
    player_name: str
    hole_cards: Optional[str] = None
    won_amount: int = 0
    hand_description: Optional[str] = None  # e.g., "two pair, Kings and Fours"
    position_desc: Optional[str] = None  # e.g., "button", "small blind"
    folded: bool = False
    folded_street: Optional[str] = None  # e.g., "before Flop"


@dataclass
class Hand:
    """Represents a complete poker hand"""
    hand_id: str
    tournament_id: str
    tournament_name: str
    buy_in: Decimal
    game_type: str  # "Hold'em No Limit"
    level: int
    blinds: tuple[int, int]  # (sb, bb)
    ante: int
    timestamp: datetime
    table_name: str
    max_players: int
    button_seat: int
    
    players: list[Player] = field(default_factory=list)
    hero_name: Optional[str] = None
    hero_cards: Optional[str] = None
    
    # Board
    flop: Optional[str] = None
    turn: Optional[str] = None
    river: Optional[str] = None
    
    # Actions by street
    preflop_actions: list[Action] = field(default_factory=list)
    flop_actions: list[Action] = field(default_factory=list)
    turn_actions: list[Action] = field(default_factory=list)
    river_actions: list[Action] = field(default_factory=list)
    showdown_actions: list[Action] = field(default_factory=list)
    
    # Results
    total_pot: int = 0
    rake: int = 0
    results: list[HandResult] = field(default_factory=list)
    winners: list[str] = field(default_factory=list)
    
    @property
    def board(self) -> Optional[str]:
        """Full board as string"""
        parts = []
        if self.flop:
            parts.append(self.flop)
        if self.turn:
            parts.append(self.turn)
        if self.river:
            parts.append(self.river)
        return " ".join(parts) if parts else None
    
    @property
    def hero(self) -> Optional[Player]:
        """Get hero player"""
        for p in self.players:
            if p.is_hero:
                return p
        return None
    
    @property
    def went_to_showdown(self) -> bool:
        """Did hand go to showdown?"""
        return len(self.showdown_actions) > 0 or any(r.hole_cards and not r.folded for r in self.results)
    
    @property
    def hero_won(self) -> bool:
        """Did hero win?"""
        return self.hero_name in self.winners if self.hero_name else False
    
    @staticmethod
    def _cards_to_list(cards_str: Optional[str]) -> Optional[list[str]]:
        """Convert cards string '6d 7d' to list ['6d', '7d']"""
        if not cards_str:
            return None
        return cards_str.split()
    
    @property
    def hero_cards_list(self) -> Optional[list[str]]:
        """Hero cards as list"""
        return self._cards_to_list(self.hero_cards)
    
    @property
    def flop_list(self) -> Optional[list[str]]:
        """Flop cards as list"""
        return self._cards_to_list(self.flop)
    
    @property
    def board_list(self) -> Optional[list[str]]:
        """Full board as list"""
        cards = []
        if self.flop:
            cards.extend(self.flop.split())
        if self.turn:
            cards.extend(self.turn.split())
        if self.river:
            cards.extend(self.river.split())
        return cards if cards else None
    
    def _action_to_dict(self, action: Action) -> dict:
        """Convert Action to dict"""
        return {
            "player": action.player_name,
            "action": action.action_type.value,
            "amount": action.amount,
            "is_all_in": action.is_all_in,
        }
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization / Spark DataFrame"""
        return {
            "hand_id": self.hand_id,
            "tournament_id": self.tournament_id,
            "tournament_name": self.tournament_name,
            "buy_in": float(self.buy_in),
            "game_type": self.game_type,
            "level": self.level,
            "sb": self.blinds[0],
            "bb": self.blinds[1],
            "ante": self.ante,
            "timestamp": self.timestamp.isoformat(),
            "table_name": self.table_name,
            "max_players": self.max_players,
            "button_seat": self.button_seat,
            "num_players": len(self.players),
            
            # Hero info
            "hero_name": self.hero_name,
            "hero_cards": self._cards_to_list(self.hero_cards),  # ["6d", "7d"]
            "hero_position": self.hero.position if self.hero else None,
            "hero_stack": self.hero.stack if self.hero else None,
            "hero_stack_bb": round(self.hero.stack / self.blinds[1], 1) if self.hero and self.blinds[1] else None,
            
            # Board as arrays
            "flop": self._cards_to_list(self.flop),    # ["2d", "Jh", "Td"]
            "turn": self._cards_to_list(self.turn),    # ["4c"]
            "river": self._cards_to_list(self.river),  # ["2h"]
            "board": self.board_list,                   # ["2d", "Jh", "Td", "4c", "2h"]
            
            "total_pot": self.total_pot,
            "rake": self.rake,
            "winners": self.winners,
            "hero_won": self.hero_won,
            "went_to_showdown": self.went_to_showdown,
            
            # Players as list of dicts
            "players": [
                {
                    "name": p.name,
                    "seat": p.seat,
                    "stack": p.stack,
                    "stack_bb": round(p.stack / self.blinds[1], 1) if self.blinds[1] else None,
                    "position": p.position,
                    "hole_cards": self._cards_to_list(p.hole_cards),
                    "is_hero": p.is_hero,
                    "is_button": p.seat == self.button_seat,
                }
                for p in self.players
            ],
            
            # Full actions by street (excluding antes/blinds for cleaner data)
            "preflop_actions": [
                self._action_to_dict(a) for a in self.preflop_actions 
                if a.action_type not in (ActionType.POST_ANTE, ActionType.POST_SB, ActionType.POST_BB)
            ],
            "flop_actions": [self._action_to_dict(a) for a in self.flop_actions],
            "turn_actions": [self._action_to_dict(a) for a in self.turn_actions],
            "river_actions": [self._action_to_dict(a) for a in self.river_actions],
            
            # Results per player
            "results": [
                {
                    "player_name": r.player_name,
                    "hole_cards": self._cards_to_list(r.hole_cards),
                    "won_amount": r.won_amount,
                    "hand_description": r.hand_description,
                    "folded": r.folded,
                    "folded_street": r.folded_street,
                }
                for r in self.results
            ],
        }
    
    def to_flat_dict(self) -> dict:
        """Convert to flat dictionary (no nested structures) for simple DataFrame row"""
        return {
            "hand_id": self.hand_id,
            "tournament_id": self.tournament_id,
            "tournament_name": self.tournament_name,
            "buy_in": float(self.buy_in),
            "game_type": self.game_type,
            "level": self.level,
            "sb": self.blinds[0],
            "bb": self.blinds[1],
            "ante": self.ante,
            "timestamp": self.timestamp.isoformat(),
            "button_seat": self.button_seat,
            "num_players": len(self.players),
            
            # Hero info
            "hero_card_1": self.hero_cards_list[0] if self.hero_cards_list else None,
            "hero_card_2": self.hero_cards_list[1] if self.hero_cards_list and len(self.hero_cards_list) > 1 else None,
            "hero_position": self.hero.position if self.hero else None,
            "hero_stack": self.hero.stack if self.hero else None,
            "hero_stack_bb": round(self.hero.stack / self.blinds[1], 1) if self.hero and self.blinds[1] else None,
            
            # Board - individual cards
            "flop_1": self.flop_list[0] if self.flop_list else None,
            "flop_2": self.flop_list[1] if self.flop_list and len(self.flop_list) > 1 else None,
            "flop_3": self.flop_list[2] if self.flop_list and len(self.flop_list) > 2 else None,
            "turn_card": self.board_list[3] if self.board_list and len(self.board_list) > 3 else None,
            "river_card": self.board_list[4] if self.board_list and len(self.board_list) > 4 else None,
            
            # Results
            "total_pot": self.total_pot,
            "winners": ",".join(self.winners),
            "hero_won": self.hero_won,
            "went_to_showdown": self.went_to_showdown,
            
            # Action counts
            "preflop_action_count": len([a for a in self.preflop_actions if a.action_type not in (ActionType.POST_ANTE, ActionType.POST_SB, ActionType.POST_BB)]),
            "flop_action_count": len(self.flop_actions),
            "turn_action_count": len(self.turn_actions),
            "river_action_count": len(self.river_actions),
        }
    
    def __repr__(self):
        return f"Hand(#{self.hand_id}, {self.tournament_name}, L{self.level}, {len(self.players)} players)"


@dataclass
class TournamentSummary:
    """Represents a tournament summary/result"""
    tournament_id: str
    tournament_name: str
    game_type: str
    buy_in_total: Decimal
    buy_in_prize: Decimal
    buy_in_fee: Decimal
    buy_in_bounty: Decimal
    total_players: int
    prize_pool: Decimal
    start_time: datetime
    hero_finish_position: int
    hero_prize: Decimal
    
    @property
    def roi(self) -> Decimal:
        """Return on investment as percentage"""
        if self.buy_in_total == 0:
            return Decimal(0)
        return ((self.hero_prize - self.buy_in_total) / self.buy_in_total) * 100
    
    @property
    def is_itm(self) -> bool:
        """In the money?"""
        return self.hero_prize > 0
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization / Spark DataFrame"""
        return {
            "tournament_id": self.tournament_id,
            "tournament_name": self.tournament_name,
            "game_type": self.game_type,
            "buy_in_total": float(self.buy_in_total),
            "buy_in_prize": float(self.buy_in_prize),
            "buy_in_fee": float(self.buy_in_fee),
            "buy_in_bounty": float(self.buy_in_bounty),
            "total_players": self.total_players,
            "prize_pool": float(self.prize_pool),
            "start_time": self.start_time.isoformat(),
            "hero_finish_position": self.hero_finish_position,
            "hero_prize": float(self.hero_prize),
            "roi": float(self.roi),
            "is_itm": self.is_itm,
        }
    
    def __repr__(self):
        return f"Tournament(#{self.tournament_id}, {self.tournament_name}, {self.hero_finish_position}th, ${self.hero_prize})"


class GGHandHistoryParser:
    """Parser for GGPoker hand history files"""
    
    # Regex patterns
    HAND_HEADER_PATTERN = re.compile(
        r"Poker Hand #(\w+): Tournament #(\d+), (.+?) \$(\d+\.?\d*) (.+?) - Level(\d+)\(([^)]+)\) - (\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})"
    )
    TABLE_PATTERN = re.compile(r"Table '(\d+)' (\d+)-max Seat #(\d+) is the button")
    SEAT_PATTERN = re.compile(r"Seat (\d+): (\S+) \(([\d,]+) in chips\)")
    DEALT_PATTERN = re.compile(r"Dealt to (\S+)(?: \[(.+?)\])?")
    
    # Action patterns
    ACTION_PATTERNS = {
        'fold': re.compile(r"(\S+): folds"),
        'check': re.compile(r"(\S+): checks"),
        'call': re.compile(r"(\S+): calls ([\d,]+)(?: and is all-in)?"),
        'bet': re.compile(r"(\S+): bets ([\d,]+)(?: and is all-in)?"),
        'raise': re.compile(r"(\S+): raises ([\d,]+) to ([\d,]+)(?: and is all-in)?"),
        'post_ante': re.compile(r"(\S+): posts the ante ([\d,]+)"),
        'post_sb': re.compile(r"(\S+): posts small blind ([\d,]+)"),
        'post_bb': re.compile(r"(\S+): posts big blind ([\d,]+)"),
        'uncalled': re.compile(r"Uncalled bet \(([\d,]+)\) returned to (\S+)"),
        'show': re.compile(r"(\S+): shows \[(.+?)\](?: \((.+?)\))?"),
        'collected': re.compile(r"(\S+) collected ([\d,]+) from (?:main |side )?pot"),
        'muck': re.compile(r"(\S+): mucks hand"),
    }
    
    BOARD_PATTERNS = {
        'flop': re.compile(r"\*\*\* FLOP \*\*\* \[(.+?)\]"),
        'turn': re.compile(r"\*\*\* TURN \*\*\* \[.+?\] \[(.+?)\]"),
        'river': re.compile(r"\*\*\* RIVER \*\*\* \[.+?\] \[(.+?)\]"),
    }
    
    SUMMARY_PATTERN = re.compile(r"Total pot ([\d,]+) \| Rake (\d+)")
    RESULT_PATTERN = re.compile(
        r"Seat (\d+): (\S+)(?: \(([^)]+)\))? (?:showed \[(.+?)\] and (won|lost)(?: \(([\d,]+)\))?(?: with (.+))?|folded(?: (.+))?|mucked)"
    )
    
    def __init__(self, file_path: Optional[str] = None, text: Optional[str] = None):
        self.file_path = file_path
        self.raw_text = text
        self.hands: list[Hand] = []
        
        if file_path and not text:
            self.raw_text = Path(file_path).read_text(encoding='utf-8')
    
    def parse(self) -> list[Hand]:
        """Parse all hands from the file"""
        if not self.raw_text:
            return []
        
        # Split by double newlines (hands are separated by blank lines)
        hand_texts = re.split(r'\n\n+', self.raw_text.strip())
        
        for hand_text in hand_texts:
            hand_text = hand_text.strip()
            if hand_text and hand_text.startswith("Poker Hand"):
                try:
                    hand = self._parse_hand(hand_text)
                    if hand:
                        self.hands.append(hand)
                except Exception as e:
                    print(f"Error parsing hand: {e}")
                    continue
        
        return self.hands
    
    def _parse_hand(self, text: str) -> Optional[Hand]:
        """Parse a single hand"""
        lines = text.strip().split('\n')
        if not lines:
            return None
        
        # Parse header
        header_match = self.HAND_HEADER_PATTERN.match(lines[0])
        if not header_match:
            return None
        
        hand_id, tournament_id, tournament_name, buy_in, game_type, level, blinds_str, timestamp_str = header_match.groups()
        
        # Parse blinds (e.g., "500/1,000" or "100/200")
        blinds_parts = blinds_str.replace(',', '').split('/')
        sb = int(blinds_parts[0])
        bb = int(blinds_parts[1]) if len(blinds_parts) > 1 else sb * 2
        
        # Create hand object
        hand = Hand(
            hand_id=hand_id,
            tournament_id=tournament_id,
            tournament_name=tournament_name,
            buy_in=Decimal(buy_in),
            game_type=game_type,
            level=int(level),
            blinds=(sb, bb),
            ante=0,
            timestamp=datetime.strptime(timestamp_str, "%Y/%m/%d %H:%M:%S"),
            table_name="",
            max_players=9,
            button_seat=1,
        )
        
        # Parse table info
        if len(lines) > 1:
            table_match = self.TABLE_PATTERN.match(lines[1])
            if table_match:
                hand.table_name = table_match.group(1)
                hand.max_players = int(table_match.group(2))
                hand.button_seat = int(table_match.group(3))
        
        current_street = Street.PREFLOP
        
        for line in lines[2:]:
            line = line.strip()
            if not line:
                continue
            
            # Parse seats
            seat_match = self.SEAT_PATTERN.match(line)
            if seat_match:
                seat_num = int(seat_match.group(1))
                player_name = seat_match.group(2)
                stack = int(seat_match.group(3).replace(',', ''))
                is_hero = player_name == "Hero"
                
                # Determine position
                position = None
                if seat_num == hand.button_seat:
                    position = "BTN"
                
                player = Player(
                    name=player_name,
                    seat=seat_num,
                    stack=stack,
                    is_hero=is_hero,
                    position=position
                )
                hand.players.append(player)
                if is_hero:
                    hand.hero_name = player_name
                continue
            
            # Parse dealt cards
            dealt_match = self.DEALT_PATTERN.match(line)
            if dealt_match:
                player_name = dealt_match.group(1)
                cards = dealt_match.group(2)
                if cards:
                    for p in hand.players:
                        if p.name == player_name:
                            p.hole_cards = cards
                            if p.is_hero:
                                hand.hero_cards = cards
                            break
                continue
            
            # Parse street markers
            if "*** HOLE CARDS ***" in line:
                current_street = Street.PREFLOP
                continue
            elif "*** FLOP ***" in line:
                current_street = Street.FLOP
                flop_match = self.BOARD_PATTERNS['flop'].search(line)
                if flop_match:
                    hand.flop = flop_match.group(1)
                continue
            elif "*** TURN ***" in line:
                current_street = Street.TURN
                turn_match = self.BOARD_PATTERNS['turn'].search(line)
                if turn_match:
                    hand.turn = turn_match.group(1)
                continue
            elif "*** RIVER ***" in line:
                current_street = Street.RIVER
                river_match = self.BOARD_PATTERNS['river'].search(line)
                if river_match:
                    hand.river = river_match.group(1)
                continue
            elif "*** SHOWDOWN ***" in line:
                current_street = Street.SHOWDOWN
                continue
            elif "*** SUMMARY ***" in line:
                break  # Summary section handled separately
            
            # Parse actions
            action = self._parse_action(line, current_street)
            if action:
                if current_street == Street.PREFLOP:
                    hand.preflop_actions.append(action)
                elif current_street == Street.FLOP:
                    hand.flop_actions.append(action)
                elif current_street == Street.TURN:
                    hand.turn_actions.append(action)
                elif current_street == Street.RIVER:
                    hand.river_actions.append(action)
                elif current_street == Street.SHOWDOWN:
                    hand.showdown_actions.append(action)
                
                # Track ante
                if action.action_type == ActionType.POST_ANTE and action.amount:
                    hand.ante = action.amount
                
                # Track winners
                if action.action_type == ActionType.WIN and action.player_name not in hand.winners:
                    hand.winners.append(action.player_name)
        
        # Parse summary section
        self._parse_summary(text, hand)
        
        # Assign positions based on button
        self._assign_positions(hand)
        
        return hand
    
    def _parse_action(self, line: str, street: Street) -> Optional[Action]:
        """Parse an action line"""
        
        # Fold
        match = self.ACTION_PATTERNS['fold'].match(line)
        if match:
            return Action(match.group(1), ActionType.FOLD, street=street)
        
        # Check
        match = self.ACTION_PATTERNS['check'].match(line)
        if match:
            return Action(match.group(1), ActionType.CHECK, street=street)
        
        # Call
        match = self.ACTION_PATTERNS['call'].match(line)
        if match:
            amount = int(match.group(2).replace(',', ''))
            is_all_in = "all-in" in line
            return Action(match.group(1), ActionType.CALL, amount, is_all_in, street)
        
        # Bet
        match = self.ACTION_PATTERNS['bet'].match(line)
        if match:
            amount = int(match.group(2).replace(',', ''))
            is_all_in = "all-in" in line
            return Action(match.group(1), ActionType.BET, amount, is_all_in, street)
        
        # Raise
        match = self.ACTION_PATTERNS['raise'].match(line)
        if match:
            amount = int(match.group(3).replace(',', ''))  # Total amount
            is_all_in = "all-in" in line
            return Action(match.group(1), ActionType.RAISE, amount, is_all_in, street)
        
        # Post ante
        match = self.ACTION_PATTERNS['post_ante'].match(line)
        if match:
            amount = int(match.group(2).replace(',', ''))
            return Action(match.group(1), ActionType.POST_ANTE, amount, street=street)
        
        # Post small blind
        match = self.ACTION_PATTERNS['post_sb'].match(line)
        if match:
            amount = int(match.group(2).replace(',', ''))
            return Action(match.group(1), ActionType.POST_SB, amount, street=street)
        
        # Post big blind
        match = self.ACTION_PATTERNS['post_bb'].match(line)
        if match:
            amount = int(match.group(2).replace(',', ''))
            return Action(match.group(1), ActionType.POST_BB, amount, street=street)
        
        # Uncalled bet returned
        match = self.ACTION_PATTERNS['uncalled'].match(line)
        if match:
            amount = int(match.group(1).replace(',', ''))
            return Action(match.group(2), ActionType.RETURN, amount, street=street)
        
        # Show cards
        match = self.ACTION_PATTERNS['show'].match(line)
        if match:
            return Action(match.group(1), ActionType.SHOW, street=street)
        
        # Collected pot
        match = self.ACTION_PATTERNS['collected'].match(line)
        if match:
            amount = int(match.group(2).replace(',', ''))
            return Action(match.group(1), ActionType.WIN, amount, street=street)
        
        # Muck
        match = self.ACTION_PATTERNS['muck'].match(line)
        if match:
            return Action(match.group(1), ActionType.MUCK, street=street)
        
        return None
    
    def _parse_summary(self, text: str, hand: Hand):
        """Parse the summary section of a hand"""
        # Find summary section
        summary_start = text.find("*** SUMMARY ***")
        if summary_start == -1:
            return
        
        summary_text = text[summary_start:]
        
        # Parse total pot
        pot_match = self.SUMMARY_PATTERN.search(summary_text)
        if pot_match:
            hand.total_pot = int(pot_match.group(1).replace(',', ''))
            hand.rake = int(pot_match.group(2))
        
        # Parse player results
        for line in summary_text.split('\n'):
            if not line.startswith("Seat"):
                continue
            
            # Try to parse result line
            result = self._parse_result_line(line)
            if result:
                hand.results.append(result)
    
    def _parse_result_line(self, line: str) -> Optional[HandResult]:
        """Parse a result line from summary"""
        # Pattern: Seat N: player (position) showed [cards] and won/lost (amount) with description
        # Or: Seat N: player (position) folded before Flop
        
        # First, extract seat and player
        basic_match = re.match(r"Seat (\d+): (\S+)", line)
        if not basic_match:
            return None
        
        player_name = basic_match.group(2)
        result = HandResult(player_name=player_name)
        
        # Check for position
        pos_match = re.search(r"\(([^)]+)\)", line)
        if pos_match:
            pos_text = pos_match.group(1)
            if pos_text in ["button", "small blind", "big blind"]:
                result.position_desc = pos_text
        
        # Check if folded
        if "folded" in line:
            result.folded = True
            fold_match = re.search(r"folded (.+)", line)
            if fold_match:
                result.folded_street = fold_match.group(1)
            return result
        
        # Check if showed cards
        show_match = re.search(r"showed \[(.+?)\]", line)
        if show_match:
            result.hole_cards = show_match.group(1)
        
        # Check if won
        if "won" in line:
            won_match = re.search(r"won \(([\d,]+)\)", line)
            if won_match:
                result.won_amount = int(won_match.group(1).replace(',', ''))
        
        # Get hand description
        hand_desc_match = re.search(r"with (.+)$", line)
        if hand_desc_match:
            result.hand_description = hand_desc_match.group(1)
        
        return result
    
    def _assign_positions(self, hand: Hand):
        """Assign positions based on button seat.
        
        Positions go counter-clockwise from button:
        BTN -> SB -> BB -> UTG -> UTG+1 -> MP -> HJ -> CO -> (back to BTN)
        
        For 5-max: BTN, SB, BB, UTG, MP
        For 6-max: BTN, SB, BB, UTG, MP, CO
        For 9-max: BTN, SB, BB, UTG, UTG+1, MP, HJ, CO
        """
        if not hand.players:
            return
        
        # Sort players by seat
        players_by_seat = sorted(hand.players, key=lambda p: p.seat)
        n = len(players_by_seat)
        
        if n == 0:
            return
        
        # Find button index
        btn_idx = None
        for i, p in enumerate(players_by_seat):
            if p.seat == hand.button_seat:
                btn_idx = i
                p.position = "BTN"
                break
        
        if btn_idx is None:
            return
        
        # Assign SB and BB (next 2 players after button)
        if n >= 2:
            sb_idx = (btn_idx + 1) % n
            players_by_seat[sb_idx].position = "SB"
        
        if n >= 3:
            bb_idx = (btn_idx + 2) % n
            players_by_seat[bb_idx].position = "BB"
        
        # Assign other positions based on table size
        # Remaining players are between BB and BTN (going counter-clockwise)
        if n >= 4:
            # Define positions from UTG (first to act) to CO (last before BTN)
            # For different table sizes:
            position_maps = {
                4: ["UTG"],              # 4-max: BTN, SB, BB, UTG
                5: ["UTG", "MP"],        # 5-max: BTN, SB, BB, UTG, MP
                6: ["UTG", "MP", "CO"],  # 6-max: BTN, SB, BB, UTG, MP, CO
                7: ["UTG", "UTG+1", "MP", "CO"],
                8: ["UTG", "UTG+1", "MP", "HJ", "CO"],
                9: ["UTG", "UTG+1", "MP", "MP+1", "HJ", "CO"],
            }
            
            positions = position_maps.get(n, position_maps[9][:n-3])
            
            # Start from BB+1 (UTG position)
            for i, pos_name in enumerate(positions):
                idx = (btn_idx + 3 + i) % n  # btn+1=SB, btn+2=BB, btn+3=UTG, ...
                if players_by_seat[idx].position is None:
                    players_by_seat[idx].position = pos_name


class GGTournamentSummaryParser:
    """Parser for GGPoker tournament summary files"""
    
    HEADER_PATTERN = re.compile(
        r"Tournament #(\d+), (.+?) \$(\d+), (.+)"
    )
    BUYIN_PATTERN = re.compile(
        r"Buy-in: \$([0-9.]+)\+\$([0-9.]+)\+\$([0-9.]+)"
    )
    PLAYERS_PATTERN = re.compile(r"(\d+) Players")
    PRIZE_POOL_PATTERN = re.compile(r"Total Prize Pool: \$([0-9.]+)")
    START_TIME_PATTERN = re.compile(r"Tournament started (\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})")
    FINISH_PATTERN = re.compile(r"(\d+)(?:st|nd|rd|th) : Hero, \$([0-9.]+)")
    
    def __init__(self, file_path: Optional[str] = None, text: Optional[str] = None):
        self.file_path = file_path
        self.raw_text = text
        
        if file_path and not text:
            self.raw_text = Path(file_path).read_text(encoding='utf-8')
    
    def parse(self) -> Optional[TournamentSummary]:
        """Parse tournament summary"""
        if not self.raw_text:
            return None
        
        text = self.raw_text.strip()
        
        # Parse header
        header_match = self.HEADER_PATTERN.search(text)
        if not header_match:
            return None
        
        tournament_id = header_match.group(1)
        tournament_name = header_match.group(2)
        buy_in_total = Decimal(header_match.group(3))
        game_type = header_match.group(4)
        
        # Parse buy-in breakdown
        buy_in_prize = Decimal(0)
        buy_in_fee = Decimal(0)
        buy_in_bounty = Decimal(0)
        
        buyin_match = self.BUYIN_PATTERN.search(text)
        if buyin_match:
            buy_in_prize = Decimal(buyin_match.group(1))
            buy_in_fee = Decimal(buyin_match.group(2))
            buy_in_bounty = Decimal(buyin_match.group(3))
        
        # Parse players
        total_players = 0
        players_match = self.PLAYERS_PATTERN.search(text)
        if players_match:
            total_players = int(players_match.group(1))
        
        # Parse prize pool
        prize_pool = Decimal(0)
        pool_match = self.PRIZE_POOL_PATTERN.search(text)
        if pool_match:
            prize_pool = Decimal(pool_match.group(1))
        
        # Parse start time
        start_time = datetime.now()
        time_match = self.START_TIME_PATTERN.search(text)
        if time_match:
            start_time = datetime.strptime(time_match.group(1), "%Y/%m/%d %H:%M:%S")
        
        # Parse finish position and prize
        hero_finish = 0
        hero_prize = Decimal(0)
        finish_match = self.FINISH_PATTERN.search(text)
        if finish_match:
            hero_finish = int(finish_match.group(1))
            hero_prize = Decimal(finish_match.group(2))
        
        return TournamentSummary(
            tournament_id=tournament_id,
            tournament_name=tournament_name,
            game_type=game_type,
            buy_in_total=buy_in_total,
            buy_in_prize=buy_in_prize,
            buy_in_fee=buy_in_fee,
            buy_in_bounty=buy_in_bounty,
            total_players=total_players,
            prize_pool=prize_pool,
            start_time=start_time,
            hero_finish_position=hero_finish,
            hero_prize=hero_prize,
        )


def parse_hand_history_file(file_path: str) -> list[Hand]:
    """Convenience function to parse a hand history file"""
    parser = GGHandHistoryParser(file_path=file_path)
    return parser.parse()


def parse_tournament_summary_file(file_path: str) -> Optional[TournamentSummary]:
    """Convenience function to parse a tournament summary file"""
    parser = GGTournamentSummaryParser(file_path=file_path)
    return parser.parse()


# CLI for testing
if __name__ == "__main__":
    import sys
    import json
    
    if len(sys.argv) < 2:
        print("Usage: python gg_parser.py <file_path> [--type hand|tournament]")
        sys.exit(1)
    
    file_path = sys.argv[1]
    file_type = "hand"
    
    if len(sys.argv) >= 4 and sys.argv[2] == "--type":
        file_type = sys.argv[3]
    
    if file_type == "tournament":
        result = parse_tournament_summary_file(file_path)
        if result:
            print(f"\n{'='*60}")
            print(f"Tournament Summary: {result.tournament_name}")
            print(f"{'='*60}")
            print(f"Tournament ID: {result.tournament_id}")
            print(f"Game: {result.game_type}")
            print(f"Buy-in: ${result.buy_in_total} (prize: ${result.buy_in_prize}, fee: ${result.buy_in_fee}, bounty: ${result.buy_in_bounty})")
            print(f"Players: {result.total_players}")
            print(f"Prize Pool: ${result.prize_pool}")
            print(f"Started: {result.start_time}")
            print(f"Hero finished: {result.hero_finish_position}th place")
            print(f"Hero won: ${result.hero_prize}")
            print(f"ROI: {result.roi:.1f}%")
            print(f"ITM: {result.is_itm}")
    else:
        hands = parse_hand_history_file(file_path)
        print(f"\n{'='*60}")
        print(f"Parsed {len(hands)} hands from file")
        print(f"{'='*60}")
        
        for i, hand in enumerate(hands[:5]):  # Show first 5 hands
            print(f"\n--- Hand #{hand.hand_id} ---")
            print(f"Tournament: {hand.tournament_name} (ID: {hand.tournament_id})")
            print(f"Level: {hand.level}, Blinds: {hand.blinds[0]}/{hand.blinds[1]}, Ante: {hand.ante}")
            print(f"Time: {hand.timestamp}")
            print(f"Players ({len(hand.players)}):")
            for p in hand.players:
                pos = f"[{p.position}]" if p.position else ""
                cards = f" - {p.hole_cards}" if p.hole_cards else ""
                hero = " (HERO)" if p.is_hero else ""
                print(f"  Seat {p.seat}: {p.name} ({p.stack}){pos}{cards}{hero}")
            
            if hand.hero_cards:
                print(f"Hero cards: {hand.hero_cards}")
            
            if hand.board:
                print(f"Board: {hand.board}")
            
            print(f"Pot: {hand.total_pot}")
            print(f"Winners: {', '.join(hand.winners)}")
            print(f"Went to showdown: {hand.went_to_showdown}")
            print(f"Hero won: {hand.hero_won}")
            
            # Show actions summary
            print(f"Actions: PF={len(hand.preflop_actions)}, F={len(hand.flop_actions)}, T={len(hand.turn_actions)}, R={len(hand.river_actions)}")
        
        if len(hands) > 5:
            print(f"\n... and {len(hands) - 5} more hands")
