#!/usr/bin/env python3
import sys

current_key = None
total_ip = total_er = total_bb = total_h = total_so = num_games = form_score_sum = 0.0

for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) != 3:
        continue
    
    player_name, team_name, values = parts

    #key = f"{parts[0]}\t{parts[1]}"  # team_name \t player_name
    key = f"{player_name}\t{team_name}"
    try:
        ip, er, bb, h, so = map(float, parts[2].split(","))
    except:
        continue

    outs = ip * 3
    form_score = ( 2.8
        + (2 * outs)
        + (1 * so)
        - (2 * bb)
        - (2 * h)
        - (3 * er)
    )

    if current_key != key:
        if current_key:
            team, player = current_key.split("\t")
            ERA = (total_er / total_ip * 9) if total_ip > 0 else 0
            WHIP = ((total_bb + total_h) / total_ip) if total_ip > 0 else 0
            K9 = (total_so / total_ip * 9) if total_ip > 0 else 0
            BB9 = (total_bb / total_ip * 9) if total_ip > 0 else 0
            avg_form = form_score_sum / num_games if num_games > 0 else 0
            print(f"{player}\t{team}\tFormScore:{avg_form:.1f}\tERA:{ERA:.2f}\tWHIP:{WHIP:.2f}\tK/9:{K9:.2f}\tBB/9:{BB9:.2f}")
        current_key = key
        total_ip = total_er = total_bb = total_h = total_so = num_games = form_score_sum = 0.0

    total_ip += ip
    total_er += er
    total_bb += bb
    total_h += h
    total_so += so
    form_score_sum += form_score
    num_games += 1

# Print last pitcher
if current_key:
    team, player = current_key.split("\t")
    ERA = (total_er / total_ip * 9) if total_ip > 0 else 0
    WHIP = ((total_bb + total_h) / total_ip) if total_ip > 0 else 0
    K9 = (total_so / total_ip * 9) if total_ip > 0 else 0
    BB9 = (total_bb / total_ip * 9) if total_ip > 0 else 0
    avg_form = form_score_sum / num_games if num_games > 0 else 0
    print(f"{player}\t{team}\tFormScore:{avg_form:.1f}\tERA:{ERA:.2f}\tWHIP:{WHIP:.2f}\tK/9:{K9:.2f}\tBB/9:{BB9:.2f}")

