#!/usr/bin/env python3
import sys
import json

for line in sys.stdin:
    try:
        game = json.loads(line)
        for team_side in ["home", "away"]:
            team = game["teams"][0][team_side]
            team_name = team["name"]
            for pitcher in team.get("pitching_stats", []):
                name = pitcher["player_name"]
                ip = pitcher.get("innings_pitched", 0)
                er = pitcher.get("earned_runs_allowed", 0)
                bb = pitcher.get("walks", 0)
                h = pitcher.get("hits_allowed", 0)
                so = pitcher.get("strikeouts", 0)
                print(f"{team_name}\t{name}\t{ip},{er},{bb},{h},{so}")
    except:
        continue
