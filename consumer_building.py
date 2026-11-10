
import json
from kafka import KafkaConsumer

TOPIC = "campus-sensors"
TARGET_BUILDING = "Building-A"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="tp05-building-filter",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

print(f"Listening to topic: {TOPIC}")
print(f"Filtering building: {TARGET_BUILDING}")
print("-" * 60)

try:
    for message in consumer:
        event = message.value

        building = event.get("campus_building", "Unknown")

        if building == TARGET_BUILDING:
            print(
                f'Event: {event["event_id"]} | '
                f'Sensor: {event["sensor_id"]} | '
                f'Building: {building} | '
                f'Temperature: {event["temperature_c"]}°C | '
                f'Humidity: {event["humidity_pct"]}%'
            )

except KeyboardInterrupt:
    print("Consumer stopped by user.")

finally:
    consumer.close()
