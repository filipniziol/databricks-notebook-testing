# Poker Analyzer - Databricks Asset Bundle

Databricks solution for analyzing poker hands from screenshots and GG Poker hand histories.

## Project Structure

```
poker_analyzer/
├── databricks.yml          # Bundle configuration
├── src/
│   ├── notebooks/          # Databricks notebooks (SQL, Python)
│   └── package/            # Python modules to import in notebooks
└── README.md
```

## Setup

1. Configure Databricks CLI authentication
2. Deploy bundle: `databricks bundle deploy -t dev`
3. Run jobs: `databricks bundle run -t dev <job_name>`

## Usage in Notebooks

To import package modules in notebooks:

```python
import sys
sys.path.append('/Workspace/Users/<user>/poker_analyzer/src')
from package import <module>
```

## Architecture (Planned)

### Bronze Layer
- `screenshot_analysis` - JSON analysis from screenshots
- `gg_hand_history` - Exported hand histories from GG Poker

### Silver Layer  
- `matched_hands` - Screenshots matched with GG history by timestamp
- `player_mappings` - Anonymous nick to internal_id mapping

### Gold Layer
- `decision_mismatches` - GPT recommended vs player action differences
- `player_stats` - VPIP, PFR, 3Bet%, AF calculations
- `allin_decisions` - All-in decision analysis with ML scoring
