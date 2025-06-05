
#!/usr/bin/env python3
import sys

global_score_sum = 0.0
global_num_pitchers = 0

current_key = None

for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) != 3:
        continue

    player_name, team_name, values = parts
    ab, h, bb, r, so, hr= map(int, values.split(","))

    key = f"{player_name}\t{team_name}"
   
    if current_key != key:
        if current_key:

            player, team = current_key.split("\t")
            obp = (total_h + total_bb) / (total_ab + total_bb) if (total_ab + total_bb) > 0 else 0
            avg = total_h / total_ab if total_ab > 0 else 0
            run_rate = total_r / total_ab if total_ab > 0 else 0
            hr_rate = total_hr / total_ab if total_ab > 0 else 0
            so_rate = total_so/ total_ab if total_ab > 0 else 0
            form_score = 5 + 6 * obp + 4 * avg + 1 * run_rate + 1 * hr_rate - 1 * so_rate
            print(f"{player}\tFormScore:{form_score:.1f}\t{team}\tAVG:{avg:.3f} OBP:{obp:.3f}")
            global_score_sum += form_score
            global_num_pitchers += 1
        current_key = key
        total_ab = total_h = total_bb = total_r = total_hr = total_so = 0

    total_ab += ab
    total_h += h
    total_bb += bb
    total_r += r
    total_hr += hr 
    total_so += so 

# Print last key
if current_key:
    player, team = current_key.split("\t")
    obp = (total_h + total_bb) / (total_ab + total_bb) if (total_ab + total_bb) > 0 else 0
    avg = total_h / total_ab if total_ab > 0 else 0
    run_rate = total_r / total_ab if total_ab > 0 else 0
    hr_rate = total_hr / total_ab if total_ab > 0 else 0
    so_rate = total_so/ total_ab if total_ab > 0 else 0
    form_score = 6 * obp + 5 * avg + 1 * run_rate + 1 * hr_rate - 1 * so_rate

    print(f"{player}\tFormScore:{form_score:.1f}\t{team}\tAVG:{avg:.3f} OBP:{obp:.3f}")
    global_score_sum += form_score
    global_num_pitchers += 1

if global_num_pitchers > 0:
    global_avg = global_score_sum / global_num_pitchers
    print(f"\nGLOBAL average batter form score: {global_avg:.2f}")