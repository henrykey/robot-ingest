# Robot Ingest Pipeline (IBC-AI CO.)

Two services:
- **ingest-service**: MQTT subscribe → push payloads into Redis queue (skeleton; add dedupe later)
- **writer-service**: drain Redis queue → bulk insert into MongoDB

## Build
```bash
./gradlew clean build
```

## Run locally
```bash
cp config/config.example.yml ./config.yml
java -jar ingest-service/build/libs/ingest-service-0.1.0.jar --config=./config.yml
java -jar writer-service/build/libs/writer-service-0.1.0.jar --config=./config.yml
```

## Docker Compose
```bash
docker compose up -d --build
docker compose logs -f ingest
docker compose logs -f writer
```

## Env override
Use env like `CFG__mqtt__brokerUrl=tcp://192.168.123.61:1883` to override `mqtt.brokerUrl`.
