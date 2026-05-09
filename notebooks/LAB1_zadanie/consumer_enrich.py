from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='grupa_oceniajaca_ryzyko',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    dane = message.value 
    kwota = dane['amount']
    
    if kwota > 3000:
        dane['risk_level'] = "HIGH"
    elif kwota > 1000:
        dane['risk_level'] = "MEDIUM"
    else:
        dane['risk_level'] = "LOW"
        
    print(dane)
