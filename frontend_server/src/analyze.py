from collections import defaultdict

_stats_cache = {}

def normalized_win_equiv(wins, games, actual_size, target_size):
    if actual_size <= 1 or target_size <= 1 or games == 0:
        return 0, 0
    fair_actual = 1 / actual_size
    fair_target = 1 / target_size
    factor = fair_target / fair_actual
    return wins * factor, games * factor

def calc_games_and_absences(sessions, main_idx):
    games = absences = 0
    for session in sessions:
        for game in session:
            val = game[main_idx]
            if val == 1:
                absences += 1
            else:
                games += 1
    return games, absences

def calc_wins(sessions, main_idx):
    wins = 0
    for session in sessions:
        for game in session:
            if game[main_idx] == 0:
                wins += 1
    return wins

def calc_losses(sessions, main_idx):
    losses = 0
    for session in sessions:
        for game in session:
            val = game[main_idx]
            if val not in (0, 1):
                losses += 1
    return losses

def calc_win_rate(wins, games):
    return round(100 * wins / games, 2) if games else 0

def calc_avg_points_left(sessions, main_idx):
    points = []
    for session in sessions:
        for game in session:
            val = game[main_idx]
            if val not in (0, 1):
                points.append(val)
    return round(sum(points) / len(points), 2) if points else 0

def calc_max_points_left(sessions, main_idx):
    max_points = 0
    for session in sessions:
        for game in session:
            val = game[main_idx]
            if val not in (0, 1) and val > max_points:
                max_points = val
    return max_points

def calc_total_points(sessions, main_idx, absence_as=0, avg_points=None):
    total = 0
    count_abs = 0
    for session in sessions:
        for game in session:
            val = game[main_idx]
            if val == 1:
                count_abs += 1
            elif val != 0:
                total += val
    if absence_as == 0:
        return total
    elif absence_as == 'avg' and avg_points is not None:
        return int(round(total + count_abs * avg_points))
    else:
        return total

def calc_sessions(sessions):
    return len(sessions)

def calc_win_counts(sessions, main_idx):
    per_session = []
    for session in sessions:
        per_session.append(sum(1 for game in session if game[main_idx] == 0))
    return per_session

def calc_avg_wins_per_session(win_counts):
    return round(sum(win_counts) / len(win_counts), 2) if win_counts else 0

def calc_best_session_wins(win_counts):
    return max(win_counts) if win_counts else 0

def calc_worst_session_wins(win_counts):
    return min(win_counts) if win_counts else 0

def calc_longest_streak(sessions, main_idx):
    max_streak = streak = 0
    for session in sessions:
        for game in session:
            if game[main_idx] == 0:
                streak += 1
                max_streak = max(max_streak, streak)
            else:
                streak = 0
    return max_streak

def calc_longest_streak_per_session(sessions, main_idx):
    max_streaks = []
    for session in sessions:
        streak = max_streak = 0
        for game in session:
            if game[main_idx] == 0:
                streak += 1
                max_streak = max(max_streak, streak)
            else:
                streak = 0
        max_streaks.append(max_streak)
    return max(max_streaks) if max_streaks else 0

def calc_avg_points_per_session(sessions, main_idx):
    session_points = []
    for session in sessions:
        s_points = sum(game[main_idx] for game in session if game[main_idx] not in (0, 1))
        session_points.append(s_points)
    return round(sum(session_points) / len(session_points), 2) if session_points else 0

def calc_game_list(sessions, main_idx):
    # Returns list of {session, game, val}
    game_list = []
    idx = 1
    for s_idx, session in enumerate(sessions, 1):
        for game in session:
            game_list.append({'session': s_idx, 'game': idx, 'val': game[main_idx]})
            idx += 1
    return game_list

def calc_global_max_points(sessions, players, top_n=25):
    # Find top N max points left, globally
    global_points = []
    for sess in sessions:
        for idx, player in enumerate(players):
            for g in sess:
                v = g[idx]
                if v not in (0, 1):
                    global_points.append((v, idx, player[0]))
    global_points_sorted = sorted(global_points, key=lambda x: -x[0])
    global_points_ranking = []
    seen = set()
    for rank, (val, idx, name) in enumerate(global_points_sorted, 1):
        if (name, val) not in seen:
            global_points_ranking.append((rank, name, val))
            seen.add((name, val))
        if len(global_points_ranking) >= top_n:
            break
    return global_points_ranking

def calc_player_max_rank(global_max_points_ranking, player_name, max_points):
    player_max_rank = None
    for rank, name, val in global_max_points_ranking:
        if name == player_name and val == max_points:
            player_max_rank = rank
            break
    return player_max_rank

def calc_win_ranks(sessions, players, main_idx):
    all_win_counts = []
    all_win_rates = []
    for idx, player in enumerate(players):
        pl_games = pl_wins = 0
        for sess in sessions:
            for g in sess:
                v = g[idx]
                if v == 1:
                    continue
                pl_games += 1
                if v == 0:
                    pl_wins += 1
        wr = 100 * pl_wins / pl_games if pl_games else 0
        all_win_counts.append((player[0], pl_wins))
        all_win_rates.append((player[0], wr))
    rank_by_wins = sorted(all_win_counts, key=lambda x: -x[1])
    rank_by_winrate = sorted(all_win_rates, key=lambda x: -x[1])
    player_winrank = next((i + 1 for i, (n, w) in enumerate(rank_by_wins) if n == players[main_idx][0]), None)
    player_winraterank = next((i + 1 for i, (n, w) in enumerate(rank_by_winrate) if n == players[main_idx][0]), None)
    return player_winrank, player_winraterank

def calc_win_chance_with(sessions, players, main_idx):
    win_with = {i: [0, 0] for i in range(len(players)) if i != main_idx}
    for session in sessions:
        for game in session:
            val = game[main_idx]
            for idx in win_with:
                if game[idx] != 1:
                    win_with[idx][1] += 1
                    if val == 0:
                        win_with[idx][0] += 1
    win_chance_with = {}
    for idx in win_with:
        other_name = players[idx][0]
        total = win_with[idx][1]
        won = win_with[idx][0]
        win_chance_with[other_name] = (100 * won / total) if total else 0
    return win_chance_with

def calc_win_with_by_size(sessions, players, main_idx):
    win_with_detailed = defaultdict(lambda: defaultdict(lambda: [0, 0]))
    for session in sessions:
        for game in session:
            present = [idx for idx, v in enumerate(game) if v != 1]
            if main_idx not in present:
                continue
            num_players = len(present)
            main_val = game[main_idx]
            for idx in present:
                if idx == main_idx:
                    continue
                win_with_detailed[idx][num_players][1] += 1
                if main_val == 0:
                    win_with_detailed[idx][num_players][0] += 1
    win_with_by_size_display = []
    for idx, name in enumerate(players):
        if idx == main_idx:
            continue
        for num_players in sorted(win_with_detailed[idx]):
            wins, games = win_with_detailed[idx][num_players]
            rate = (wins / games) * 100 if games else 0
            fair_pct = 100 / num_players if games else 0
            diff = rate - fair_pct
            win_with_by_size_display.append({
                "player": name[0],
                "num_players": num_players,
                "rate": round(rate, 2),
                "fair": round(fair_pct, 2),
                "diff": round(diff, 2),
                "games": games,
            })
    return win_with_by_size_display

def calc_normalized_win_chance_with(sessions, players, main_idx):
    max_group_size = max(
        (sum(1 for v in game if v != 1)
         for session in sessions for game in session),
        default=2
    )
    win_with_detailed = defaultdict(lambda: defaultdict(lambda: [0, 0]))
    for session in sessions:
        for game in session:
            present = [idx for idx, v in enumerate(game) if v != 1]
            if main_idx not in present:
                continue
            num_players = len(present)
            main_val = game[main_idx]
            for idx in present:
                if idx == main_idx:
                    continue
                win_with_detailed[idx][num_players][1] += 1
                if main_val == 0:
                    win_with_detailed[idx][num_players][0] += 1
    normalized_win_chance_with = {}
    for idx, name in enumerate(players):
        if idx == main_idx:
            continue
        adj_wins = 0.0
        adj_games = 0.0
        for num_players in sorted(win_with_detailed[idx]):
            wins, games = win_with_detailed[idx][num_players]
            norm_wins, norm_games = normalized_win_equiv(wins, games, num_players, max_group_size)
            adj_wins += norm_wins
            adj_games += norm_games
        norm_rate = (adj_wins / adj_games) * 100 if adj_games else 0
        normalized_win_chance_with[name[0]] = round(norm_rate, 2)
    return normalized_win_chance_with

def calc_win_rate_by_game_size(sessions, main_idx):
    """
    Returns a dict: {number_of_players: win_rate_percentage}
    """
    win_by_size = defaultdict(lambda: [0, 0])  # {num_players: [wins, total]}
    for session in sessions:
        for game in session:
            present = [v for v in game if v != 1]
            num_players = len(present)
            val = game[main_idx]
            if val == 1:
                continue  # absent
            win_by_size[num_players][1] += 1
            if val == 0:
                win_by_size[num_players][0] += 1
    result = []
    for size in sorted(win_by_size):
        wins, total = win_by_size[size]
        rate = (wins / total) * 100 if total else 0
        fair = 100 / size if size else 0
        diff = rate - fair
        result.append({
            "num_players": size,
            "rate": round(rate, 2),
            "fair": round(fair, 2),
            "diff": round(diff, 2),
            "games": total
        })
    return result

def calculate_romee_hand_wins(sessions, main_idx) -> int:
    """
    Returns an int: number_of_romee_hand_wins

        wins = 0
    for i, session in enumerate(sessions):
        for game in session:
            val = game[main_idx]
            if val == 1: continue  # absent
            elif val == 0:
                if romee_hand_scores[i] == 1:
                    wins += 1
    return wins
    """
    wins = 0
    i = 0
    for session in sessions:
        for game in session:
            if game[main_idx] == 0 and game.hand:
                wins += 1
            i += 1
    return wins


def analyze_stats(sessions, players, main_idx):
    # Cache key
    cache_key = (tuple(tuple(map(tuple, sessions))), tuple(players), main_idx)
    if cache_key in _stats_cache:
        return _stats_cache[cache_key]

    games, absences = calc_games_and_absences(sessions, main_idx)
    wins = calc_wins(sessions, main_idx)
    romee_hand_wins = calculate_romee_hand_wins(sessions, main_idx)
    romee_hand_win_rate = calc_win_rate(romee_hand_wins, games)
    losses = calc_losses(sessions, main_idx)
    win_rate = calc_win_rate(wins, games)
    avg_points_left = calc_avg_points_left(sessions, main_idx)
    max_points = calc_max_points_left(sessions, main_idx)
    total_points_absence_zero = calc_total_points(sessions, main_idx, absence_as=0)
    total_points_absence_avg = calc_total_points(sessions, main_idx, absence_as='avg', avg_points=avg_points_left)
    session_count = calc_sessions(sessions)
    win_counts = calc_win_counts(sessions, main_idx)
    avg_wins_per_session = calc_avg_wins_per_session(win_counts)
    best_session_wins = calc_best_session_wins(win_counts)
    worst_session_wins = calc_worst_session_wins(win_counts)
    longest_streak = calc_longest_streak(sessions, main_idx)
    longest_streak_per_session = calc_longest_streak_per_session(sessions, main_idx)
    avg_points_per_session = calc_avg_points_per_session(sessions, main_idx)
    game_list = calc_game_list(sessions, main_idx)
    global_max_points_ranking = calc_global_max_points(sessions, players, top_n=25)
    player_max_rank = calc_player_max_rank(global_max_points_ranking, players[main_idx][0], max_points)
    player_winrank, player_winraterank = calc_win_ranks(sessions, players, main_idx)
    win_chance_with = calc_win_chance_with(sessions, players, main_idx)
    win_with_by_size = calc_win_with_by_size(sessions, players, main_idx)
    normalized_win_chance_with = calc_normalized_win_chance_with(sessions, players, main_idx)
    max_group_size = max(
        (sum(1 for v in game if v != 1)
         for session in sessions for game in session),
        default=2
    )
    win_rate_by_game_size = calc_win_rate_by_game_size(sessions, main_idx)

    result = dict(
        games=games,
        absences=absences,
        wins=wins,
        romee_hand_wins=romee_hand_wins,
        romee_hand_win_rate=romee_hand_win_rate,
        losses=losses,
        win_rate=win_rate,
        avg_points_left=avg_points_left,
        max_points=max_points,
        total_points_absence_zero=total_points_absence_zero,
        total_points_absence_avg=total_points_absence_avg,
        sessions=session_count,
        avg_wins_per_session=avg_wins_per_session,
        best_session_wins=best_session_wins,
        worst_session_wins=worst_session_wins,
        longest_streak=longest_streak,
        longest_streak_per_session=longest_streak_per_session,
        avg_points_per_session=avg_points_per_session,
        game_list=game_list,
        global_max_points=global_max_points_ranking,
        player_max_rank=player_max_rank,
        winrank=player_winrank,
        winraterank=player_winraterank,
        win_chance_with=win_chance_with,
        win_with_by_size=win_with_by_size,
        normalized_win_chance_with=normalized_win_chance_with,
        max_group_size=max_group_size,
        general_win_by_size=win_rate_by_game_size,
    )
    _stats_cache[cache_key] = result
    return result
