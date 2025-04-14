# 🎧 Spark Structured API Project – Streaming Platform User Insights

## 🔍 Introduction
This project applies **Apache Spark Structured APIs** to analyze behavior and engagement on a simulated music streaming platform. Using synthetic datasets, we perform transformations and aggregations to derive user preferences, popular content trends, and behavioral insights.

Two CSV datasets are generated to simulate platform activity:
- `listening_logs.csv` – represents when and how users listen to music
- `songs_metadata.csv` – includes details of songs like genre, artist, and mood

---

## 📂 Data Overview

### 🎵 listening_logs.csv
| Field        | Description                              |
|--------------|------------------------------------------|
| user_id      | Unique user identifier                   |
| song_id      | ID referencing the song played           |
| timestamp    | When the song was played                 |
| duration_sec | Length of play session in seconds        |

### 🎶 songs_metadata.csv
| Field     | Description                              |
|-----------|------------------------------------------|
| song_id   | Unique identifier for each track         |
| title     | Song title                               |
| artist    | Artist name                              |
| genre     | Musical genre (Pop, Rock, etc.)          |
| mood      | Mood classification (Happy, Sad, etc.)   |

---

## 🧪 Spark Tasks & Output Directories

| Task No. | Description                                                                    | Output Path                         |
|---------:|--------------------------------------------------------------------------------|-------------------------------------|
| Task 1   | Determine most preferred genre by each user                                   | `output/user_favorite_genres/`      |
| Task 2   | Compute mean listening time per song                                           | `output/avg_listen_time_per_song/`  |
| Task 3   | Extract top 10 most played tracks in the current week                         | `output/top_songs_this_week/`       |
| Task 4   | Suggest 3 "Happy" songs to users leaning toward "Sad" music                   | `output/happy_recommendations/`     |
| Task 5   | Calculate and filter users with genre loyalty > 0.5                           | `output/genre_loyalty_scores/`      |
| Task 6   | Identify “night listeners” active between 12 AM and 5 AM                     | `output/night_owl_users/`           |

---

#Outputs

Task1  u1,Hip-Hop,17
u10,Jazz,14
u11,Hip-Hop,17
u12,Pop,16
u13,Hip-Hop,12
u13,Classical,12
u14,Hip-Hop,13
u15,Jazz,15
u16,Pop,14
u17,Pop,11
u17,Jazz,11
u18,Pop,14
u19,Hip-Hop,13
u2,Pop,12
u20,Hip-Hop,12
u20,Jazz,12
u3,Hip-Hop,17
u4,Hip-Hop,13
u4,Jazz,13
u5,Jazz,15
u6,Jazz,17
u7,Hip-Hop,12
u8,Jazz,12
u9,Rock,10
u9,Jazz,10


  task2  

 s53,152.77777777777777
s25,121.55555555555556
s65,156.66666666666666
s41,154.5
s21,163.8
s6,106.85714285714286
s50,166.0
s80,133.45454545454547
s55,180.69230769230768
s49,172.72727272727272
s8,200.0
s10,170.66666666666666
s51,157.25
s24,160.2941176470588
s12,194.11111111111111
s17,173.4
s9,180.1818181818182
s32,180.83333333333334
s67,159.0
s22,170.28571428571428
s43,211.84615384615384
s61,164.21428571428572
s63,171.7
s35,191.1
s11,208.85714285714286
s96,181.0
s75,177.71428571428572
s48,177.1
s34,151.5
s14,145.75
s52,164.1818181818182
s23,163.36363636363637
s27,144.45454545454547
s4,164.9
s5,118.28571428571429
s28,147.14285714285714
s82,185.0
s18,170.5
s20,146.8235294117647
s7,171.54545454545453
s56,151.5
s73,167.25
s16,190.27272727272728
s78,203.6
s66,160.08333333333334
s2,179.6
s64,192.5
s70,139.9090909090909
s42,200.75
s100,192.05555555555554
s60,163.44444444444446
s54,165.6
s77,178.25

 task 3  
s42,6
s22,5
s100,5
s85,5
s86,5
s50,4
s55,4
s20,4
s56,4
s66,4


 task4  
u12,s65
u12,s51
u12,s17
u14,s53
u14,s65
u14,s41
u16,s53
u16,s65
u16,s41
u6,s53
u6,s65
u6,s41
u9,s53
u9,s65
u9,s41

 task 5  
u1,Hip-Hop,0.30357142857142855
u10,Jazz,0.3181818181818182
u11,Hip-Hop,0.3469387755102041
u12,Pop,0.2711864406779661
u13,Hip-Hop,0.2222222222222222
u13,Classical,0.2222222222222222
u14,Hip-Hop,0.2826086956521739
u15,Jazz,0.28846153846153844
u16,Pop,0.2857142857142857
u17,Pop,0.24444444444444444
u17,Jazz,0.24444444444444444
u18,Pop,0.2641509433962264
u19,Hip-Hop,0.28888888888888886
u2,Pop,0.2727272727272727
u20,Hip-Hop,0.26666666666666666
u20,Jazz,0.26666666666666666
u3,Hip-Hop,0.3148148148148148
u4,Hip-Hop,0.22807017543859648
u4,Jazz,0.22807017543859648
u5,Jazz,0.2830188679245283
u6,Jazz,0.2786885245901639
u7,Hip-Hop,0.2727272727272727
u8,Jazz,0.2608695652173913
u9,Rock,0.22727272727272727
u9,Jazz,0.22727272727272727

 task 6  
u3
u19
u10
u16
u20
u13
u5
u4
u8
u15
u1
u11
u14
u12
u6
u9
u7
u18
u17
u2
