#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "[1/3] Остановка старого контейнера..."
docker compose down

echo "[2/3] Сбор образа и запуск сервиса..."
docker compose up --build -d --remove-orphans --force-recreate

echo "[3/3] Готово. Логи:"
docker compose logs -f
