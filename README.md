# FIDE Glicko Rating System

A Python implementation of the FIDE Glicko rating system for chess players.

## Overview

This project implements the Glicko rating system as used by FIDE (Fédération Internationale des Échecs) for calculating chess player ratings. The Glicko system is an improvement over the Elo rating system, incorporating rating deviation (RD) to better represent the uncertainty in a player's rating.

**Architecture:**
- **Python**: Exploratory work and prototyping
- **Go**: Heavy web scraping
- **Rust**: Core rating computations

## Installation

### Python (Exploratory Work)

We use `uv` for fast Python dependency management:

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv pip install -r requirements.txt

# Or use uv to run scripts directly
uv run exploratory/get_federations.py
```

Alternatively, use standard pip:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Usage

```python
# Example usage will be added here
```

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
