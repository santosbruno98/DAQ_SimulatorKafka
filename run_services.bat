@echo off
REM Open first terminal: Kafka Producer
start cmd /k "docker exec -it fastapi-boilerplate-web-1 bash -c "python -m app.core.worker.daq.kafka_producer""

REM Open second terminal: Correlation Service
start cmd /k "docker exec -it fastapi-boilerplate-web-1 bash -c "python -m app.core.worker.services.correlation_service""

REM Open third terminal: Conversion Service
start cmd /k "docker exec -it fastapi-boilerplate-web-1 bash -c "python -m app.core.worker.services.conversion_service""

REM Open fourth terminal: Storage Service
start cmd /k "docker exec -it fastapi-boilerplate-web-1 bash -c "python -m app.core.worker.services.writer_service""