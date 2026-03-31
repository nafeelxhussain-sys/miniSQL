@echo off
:: Start the container in the background
docker-compose up -d

:: Open a new CMD window and attach to the miniSQL process
start "miniSQL Database" cmd /k "docker attach minisql"