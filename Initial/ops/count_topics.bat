@echo off
REM Script to count messages in all Kafka topics

echo.
echo ==========================================
echo Kafka Topic Message Counts
echo ==========================================
echo.

docker-compose exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic behav.stream_session
echo.

docker-compose exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic behav.engagement
echo.

docker-compose exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic behav.search
echo.

docker-compose exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic behav.browse
echo.

docker-compose exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic behav.tile_impression
echo.

docker-compose exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic behav.commerce
echo.

docker-compose exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic behav.notification
echo.

echo ==========================================
echo Done!
echo ==========================================
pause