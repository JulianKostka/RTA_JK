from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

stats = defaultdict(lambda: {'liczba': 0, 'suma': 0.0, 'min': 999999.0, 'max': 0.0})
msg_count = 0


for message in consumer:
    dane = message.value
    kategoria = dane['category']
    kwota = dane['amount']
    
    stats[kategoria]['liczba'] += 1
    stats[kategoria]['suma'] += kwota
    
    if kwota < stats[kategoria]['min']:
        stats[kategoria]['min'] = kwota
        
    if kwota > stats[kategoria]['max']:
        stats[kategoria]['max'] = kwota
        
    msg_count += 1
    
    if msg_count % 10 == 0:
        print(f"\n--- STATYSTYKI KATEGORII (po {msg_count} transakcjach) ---")
        print("Kategoria | Liczba | Suma | Min | Max")
        
        for kat, s in stats.items():
            print(f"{kat} | {s['liczba']} | {s['suma']:.2f} | {s['min']:.2f} | {s['max']:.2f}")
