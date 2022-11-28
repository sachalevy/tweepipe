import json
import os

import pandas as pd

file_root = "scripts/us_primaries/candidates_usernames/"
files = [
    "house_candidates.csv",
    "senate_candidates.csv",
    "midterm_senate_candidates.csv",
]
output_file = "usernames.json"

users = {}
for file in files:
    df = pd.read_csv(os.path.join(file_root, file))
    for name, username in zip(df["Name"], df["Twitter Handle"]):
        if name != "TBD" and not isinstance(username, float):
            users[name] = username

print("Found {} users.".format(len(users)))

with open(os.path.join(file_root, output_file), "w") as f:
    json.dump(users, f)
