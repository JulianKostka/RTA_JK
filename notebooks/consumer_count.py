from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = {}
msg_count = 0


for message in consumer:
    dane = message.value
    sklep = dane['store']
    kwota = dane['amount']
    

    store_counts[sklep] += 1
     
    total_amount[sklep] = total_amount.get(sklep, 0) + kwota
    
    msg_count += 1
    
    if msg_count % 10 == 0:
        print(f"\n--- PODSUMOWANIE (po {msg_count} transakcjach) ---")
        print("Sklep | Liczba | Suma | Średnia")
        
        for s, liczba in store_counts.items():
            suma = total_amount[s]
            srednia = suma / liczba
            print(f"{s} | {liczba} | {suma:.2f} | {srednia:.2f}")
