#!/usr/bin/env python3
import sys

current_key = None
total_ip = total_er = total_bb = total_h = total_so = 0.0

for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) != 3:
        continue

    key = f"{parts[0]}\t{parts[1]}"  # team_name \t player_name
    try:
        ip, er, bb, h, so = map(float, parts[2].split(","))
    except:
        continue

    if current_key != key:
        if current_key:
            team, player = current_key.split("\t")
            ERA = (total_er / total_ip * 9) if total_ip > 0 else 0
            WHIP = ((total_bb + total_h) / total_ip) if total_ip > 0 else 0
            K9 = (total_so / total_ip * 9) if total_ip > 0 else 0
            BB9 = (total_bb / total_ip * 9) if total_ip > 0 else 0
            outs = total_ip * 3
            form_score = (
                40
                + (2 * outs)
                + (1 * total_so)
                - (2 * total_bb)
                - (2 * total_h)
                - (3 * total_er)
            )
            print(f"{player}\t{team}\tFormScore:{form_score:.1f}\tERA:{ERA:.2f}\tWHIP:{WHIP:.2f}\tK/9:{K9:.2f}\tBB/9:{BB9:.2f}")
        current_key = key
        total_ip = total_er = total_bb = total_h = total_so = 0.0

    total_ip += ip
    total_er += er
    total_bb += bb
    total_h += h
    total_so += so

# Print last pitcher
if current_key:
    team, player = current_key.split("\t")
    ERA = (total_er / total_ip * 9) if total_ip > 0 else 0
    WHIP = ((total_bb + total_h) / total_ip) if total_ip > 0 else 0
    K9 = (total_so / total_ip * 9) if total_ip > 0 else 0
    BB9 = (total_bb / total_ip * 9) if total_ip > 0 else 0
    outs = total_ip * 3
    form_score = (
        40
        + (2 * outs)
        + (1 * total_so)
        - (2 * total_bb)
        - (2 * total_h)
        - (3 * total_er)
        )
    print(f"{player}\t{team}\tFormScore:{form_score:.1f}\tERA:{ERA:.2f}\tWHIP:{WHIP:.2f}\tK/9:{K9:.2f}\tBB/9:{BB9:.2f}")
