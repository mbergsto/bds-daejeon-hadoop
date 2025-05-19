#!/usr/bin/env python3

import json
import sys
from collections import defaultdict

# Initialize dictionaries 
wins = defaultdict(int)
losses = defaultdict(int)
draws = defaultdict(int)
score_diff = defaultdict(int)

for line in sys.stdin:
    parts = line.strip().split('\t')
    if len(parts) != 3:
        continue

    team, result, diff = parts
    diff = int(diff)

    if result == 'WIN':
        wins[team] += 1
    elif result == 'LOSE':
        losses[team] += 1
    elif result == 'DRAW':
        draws[team] += 1

    score_diff[team] += diff

for team in sorted(wins.keys() | losses.keys() | draws.keys()):
    print(json.dumps({
        "team": team,
        "wins": wins[team],
        "losses": losses[team],
        "draws": draws[team],
        "score_difference": score_diff[team]
    }))
