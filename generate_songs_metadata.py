from faker import Faker
import pandas as pd
import random

fake = Faker()
genres = ['Pop', 'Rock', 'Jazz', 'Classical', 'Hip-Hop']
moods = ['Happy', 'Sad', 'Energetic', 'Chill']

data = []
for i in range(1, 101):
    data.append({
        'song_id': f's{i}',
        'title': fake.catch_phrase(),
        'artist': fake.name(),
        'genre': random.choice(genres),
        'mood': random.choice(moods)
    })

df = pd.DataFrame(data)
df.to_csv('songs_metadata.csv', index=False)

