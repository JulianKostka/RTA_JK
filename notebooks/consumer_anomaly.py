from kafka import KafkaConsumer
import json, time
from collections import defaultdict

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

historia_czasow = defaultdict(list)

print("Czekam na oszustów (więcej niż 3 transakcje w 60 sekund)...")

for message in consumer:
    dane = message.value
    user = dane['user_id']

    teraz = time.time() 

    historia_czasow[user].append(teraz)

    historia_czasow[user] = [czas for czas in historia_czasow[user] if teraz - czas <= 60]
    
    if len(historia_czasow[user]) > 3:
        print(f"ALARM! {user} wariuje! Zrobił {len(historia_czasow[user])} transakcje w minutę!")
