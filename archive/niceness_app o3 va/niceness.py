# Simple niceness score: 100 - |temp - 75| * 2 - rain_penalty

def score(row):
    temp_penalty = abs(row["temp_f"] - 75) * 2
    rain_penalty = 30 if row["weathercode"] >= 60 else 0  # crude check
    return max(0, 100 - temp_penalty - rain_penalty)
