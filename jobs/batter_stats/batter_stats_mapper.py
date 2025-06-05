
#!/usr/bin/env python3
import sys
import json

for line in sys.stdin:
    try:
        game = json.loads(line)
        for team_side in ["home", "away"]:
            team = game["teams"][0][team_side]
            team_name = team["name"]
            for player in team.get("batting_stats", []):
                name = player["player_name"]
                H = player.get("hits", 0)
                BB = player.get("walks", 0)
                AB = player.get("at_bats", 0)
                R = player.get("runs", 0)
                SO = player.get("strikeouts", 0)
                HR = player.get("home_runs", 0)
                print(f"{name}\t{team_name}\t{AB},{H},{BB},{R},{SO},{HR}")
    except:
        continue

