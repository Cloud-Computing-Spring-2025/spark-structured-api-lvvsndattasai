from faker import Faker
import pandas as pd
import random
from datetime import timedelta

fake = Faker()

users = [f'u{i}' for i in range(1, 21)]
songs = [f's{i}' for i in range(1, 101)]

logs = []

for _ in range(1000):
    logs.append({
        'user_id': random.choice(users),
        'song_id': random.choice(songs),
        'timestamp': fake.date_time_between(start_date='-30d', end_date='now').strftime('%Y-%m-%d %H:%M:%S'),
        'duration_sec': random.randint(30, 300)
    })

df = pd.DataFrame(logs)
df.to_csv('listening_logs.csv', index=False)

