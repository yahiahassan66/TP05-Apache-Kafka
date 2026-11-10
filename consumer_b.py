
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "campus-sensors",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="experiment-same-group",
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

print("Consumer B started")

try:
    for message in consumer:
        event = message.value
        print(
            f"Consumer B | Event: {event['event_id']} | "
            f"Temperature: {event['temperature_c']}°C"
        )

except KeyboardInterrupt:
    print("Consumer B stopped")

finally:
    consumer.close()
