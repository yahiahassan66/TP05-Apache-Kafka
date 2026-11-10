# TP05 – Real-Time Data Streaming and Processing Using Apache Kafka

**Master 2 – Artificial Intelligence & Data Science**  
**University of El Oued – Academic Year 2026/2027**

## 1. Project Overview

This project demonstrates real-time data streaming and processing using Apache Kafka.

The scenario simulates a smart campus monitoring system where temperature and humidity sensors generate events. A Kafka producer sends sensor data to a Kafka topic, while consumers process the incoming messages and detect abnormal temperature values.

## 2. Technologies Used

- Apache Kafka 3.9.1
- Docker and Docker Compose
- Python
- kafka-python
- JSON

## 3. Project Structure

```text
tp05-kafka/
│
├── compose.yaml
├── requirements.txt
├── producer.py
├── consumer.py
├── consumer_A.py
├── consumer_B.py
├── consumer_building.py
├── README.md
└── screenshots/
```

## 4. Requirements

- Docker Desktop
- Python 3.10 or newer
- Docker Compose
- pip

## 5. Installation

Create and activate a Python virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install the required Python package:

```powershell
pip install -r requirements.txt
```

## 6. Start Kafka

Start the Kafka broker using Docker Compose:

```powershell
docker compose up -d
```

Check the container status:

```powershell
docker compose ps
```

Display Kafka logs:

```powershell
docker compose logs --tail=50 kafka
```

## 7. Create the Kafka Topic

Create the `campus-sensors` topic with three partitions:

```powershell
docker exec -it tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic campus-sensors --partitions 3 --replication-factor 1
```

Describe the topic:

```powershell
docker exec -it tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic campus-sensors
```

## 8. Run the Producer

In the first terminal:

```powershell
python producer.py
```

The producer generates sensor events containing:

- Sensor ID
- Temperature
- Humidity
- Timestamp

Messages are serialized as JSON and sent to the `campus-sensors` topic.

## 9. Run the Consumer

In a second terminal:

```powershell
python consumer.py
```

The consumer reads messages from Kafka, processes the sensor data, and detects high temperatures (35°C or above).

## 10. Experiments

The following experiments were performed:

1. Sending 100 sensor events.
2. Calculating the running average temperature.
3. Comparing consumers using the same and different consumer groups.
4. Comparing topics with one and three partitions.
5. Adding campus building information and filtering events by building.

## 11. Screenshots

The `screenshots/` directory contains evidence of the Kafka deployment, producer and consumer execution, topic configuration, consumer groups, and experimental results.

## 12. Stop Kafka

Stop the Kafka containers:

```powershell
docker compose down
```

To remove the persistent Kafka data volume as well:

```powershell
docker compose down -v
```

**Note:** The second command permanently removes the stored Kafka data.