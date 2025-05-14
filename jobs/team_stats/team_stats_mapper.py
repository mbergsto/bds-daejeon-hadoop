#!/usr/bin/env python3

import sys
import json

# Iterate through each line of input (each line is a JSON object representing a game)
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        # Parse the JSON string into a Python dictionary
        game = json.loads(line)

        # The 'teams' key contains a list with one dictionary that includes both home and away
        teams = game["teams"][0]

        home = teams["home"]
        away = teams["away"]

        home_name = home["name"]
        away_name = away["name"]

        home_score = home["score"]
        away_score = away["score"]

        if home_score > away_score:
            print(f"{home_name}\tWIN\t{home_score - away_score}")
            print(f"{away_name}\tLOSE\t{away_score - home_score}")
        elif home_score < away_score:
            print(f"{home_name}\tLOSE\t{home_score - away_score}")
            print(f"{away_name}\tWIN\t{away_score - home_score}")
        else:
            print(f"{home_name}\tDRAW\t0")
            print(f"{away_name}\tDRAW\t0")

    except Exception as e:
        # If there's an error processing a line, print it to stderr
        print(f"Error: {e}", file=sys.stderr)
