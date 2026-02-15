@echo off
REM ============================================
REM Setup Kafka Topics - Projet Crypto Binance
REM ============================================
REM Usage: setup_kafka_topics.bat
REM Prerequis: docker-compose up -d (Kafka doit etre running)

set KAFKA_CONTAINER=kafka
set BOOTSTRAP_SERVER=localhost:9092

echo ======================================================
echo      Configuration Topics Kafka - Crypto Binance
echo ======================================================
echo.

REM Verifier que Kafka est running
echo [INFO] Verification de Kafka...
docker exec %KAFKA_CONTAINER% kafka-broker-api-versions --bootstrap-server %BOOTSTRAP_SERVER% >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERREUR] Kafka n'est pas accessible. Lancez d'abord: docker-compose up -d
    exit /b 1
)
echo [OK] Kafka est operationnel
echo.

REM Topic 1: raw_trades (3 partitions, retention 24h)
echo [CREATION] Topic: raw_trades
docker exec %KAFKA_CONTAINER% kafka-topics --create --bootstrap-server %BOOTSTRAP_SERVER% --topic raw_trades --partitions 3 --replication-factor 1 --config retention.ms=86400000 --config cleanup.policy=delete --if-not-exists
echo    [OK] raw_trades cree (3 partitions, retention 24h)

REM Topic 2: raw_klines (3 partitions, retention 7j)
echo [CREATION] Topic: raw_klines
docker exec %KAFKA_CONTAINER% kafka-topics --create --bootstrap-server %BOOTSTRAP_SERVER% --topic raw_klines --partitions 3 --replication-factor 1 --config retention.ms=604800000 --config cleanup.policy=delete --if-not-exists
echo    [OK] raw_klines cree (3 partitions, retention 7j)

REM Topic 3: processed_data (3 partitions, retention 7j)
echo [CREATION] Topic: processed_data
docker exec %KAFKA_CONTAINER% kafka-topics --create --bootstrap-server %BOOTSTRAP_SERVER% --topic processed_data --partitions 3 --replication-factor 1 --config retention.ms=604800000 --config cleanup.policy=delete --if-not-exists
echo    [OK] processed_data cree (3 partitions, retention 7j)

REM Topic 4: anomalies (1 partition, retention 30j)
echo [CREATION] Topic: anomalies
docker exec %KAFKA_CONTAINER% kafka-topics --create --bootstrap-server %BOOTSTRAP_SERVER% --topic anomalies --partitions 1 --replication-factor 1 --config retention.ms=2592000000 --config cleanup.policy=delete --if-not-exists
echo    [OK] anomalies cree (1 partition, retention 30j)

echo.
echo ======================================================
echo.

REM Verification
echo [INFO] Liste des topics crees:
echo.
docker exec %KAFKA_CONTAINER% kafka-topics --list --bootstrap-server %BOOTSTRAP_SERVER%

echo.
echo ======================================================
echo [OK] Configuration Kafka terminee!
echo.
echo Topics crees:
echo   raw_trades     (3 parts, 24h)  - WebSocket trades
echo   raw_klines     (3 parts, 7j)   - OHLCV candlesticks
echo   processed_data (3 parts, 7j)   - Spark ETL output
echo   anomalies      (1 part, 30j)   - Alertes detectees
echo ======================================================
pause
