import time
import json
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

categorias = ['electronica', 'hogar', 'ropa', 'juguetes']
print("--- INICIANDO GENERADOR DE DATOS KAFKA (Presione Ctrl+C para detener) ---")

id_transaccion = 1
try:
    while True:
        data = {
            "id": id_transaccion,
            "categoria": random.choice(categorias),
            "monto": round(random.uniform(10.0, 500.0), 2),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        producer.send('topic-ventas', value=data)
        print(f"Mensaje enviado a Kafka: {data}")
        id_transaccion += 1
        time.sleep(2)
except KeyboardInterrupt:
    print("\nProductor detenido.")
