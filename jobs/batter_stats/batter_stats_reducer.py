
#!/usr/bin/env python3
import sys

current_key = None
total_ab = total_h = total_bb = 0

for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) != 3:
        continue

    player_name, team_name, values = parts
    ab, h, bb = map(int, values.split(","))

    key = f"{player_name}\t{team_name}"

    if current_key != key:
        if current_key:
            player, team = current_key.split("\t")
            obp = (total_h + total_bb) / (total_ab + total_bb) if (total_ab + total_bb) > 0 else 0
            avg = total_h / total_ab if total_ab > 0 else 0
            outs = total_ab - total_h
            form_score = (
                50
                + total_h
                + total_bb
                + 2 * ((total_ab + total_bb) // 4)
                - outs
            )
            print(f"{player}\tFormScore:{form_score:.1f}\t{team}\tAVG:{avg:.3f} OBP:{obp:.3f}")
        current_key = key
        total_ab = total_h = total_bb = 0

    total_ab += ab
    total_h += h
    total_bb += bb

# Print last key
if current_key:
    player, team = current_key.split("\t")
    obp = (total_h + total_bb) / (total_ab + total_bb) if (total_ab + total_bb) > 0 else 0
    avg = total_h / total_ab if total_ab > 0 else 0
    outs = total_ab - total_h
    form_score = (
        50
        + total_h
        + total_bb
        + 2 * ((total_ab + total_bb) // 4)
        - outs
        )
    print(f"{player}\tFormScore:{form_score:.1f}\t{team}\tAVG:{avg:.3f} OBP:{obp:.3f}")


