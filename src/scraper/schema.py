"""
Shared column and field names for FIDE scraper outputs.

Use these constants for consistency across scripts and downstream consumers.
"""

# Tournament identifiers
TOURNAMENT_ID = "tournament_id"
TOURNAMENT_CODE = "tournament_id"  # Alias: reports use "tournament_code" historically

# Player identifiers
PLAYER_ID = "player_id"
WHITE_PLAYER_ID = "white_player_id"
BLACK_PLAYER_ID = "black_player_id"

# Federation / country (3-letter FIDE code)
FED = "fed"
COUNTRY = "fed"  # Alias: reports use "country" in player dicts

# Reports players (no rating - use profile chart if needed)
PLAYER_TOTAL = "player_total"

# Tournament details
EVENT_CODE = "id"
N_PLAYERS = "n_players"

# Reports / games
ROUND_NUMBER = "round_number"
ROUND_DATE = "round_date"
SCORE = "score"
FORFEIT = "forfeit"

# Player list
BYEAR = "byear"
NAME = "name"
SEX = "sex"
TITLE = "title"
W_TITLE = "w_title"
