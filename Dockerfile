# Базовый образ с Node
FROM node:20-bookworm

# Ставим Python и requests из пакетов Debian
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      python3 \
      python3-requests && \
    rm -rf /var/lib/apt/lists/*

# Рабочая директория внутри контейнера
WORKDIR /usr/src/voice_ai_bot

# Сначала только package-файлы (для кеша npm install)
COPY package*.json ./

# Установка прод-зависимостей
RUN if [ -f package-lock.json ]; then \
      npm ci --only=production; \
    else \
      npm install --production; \
    fi

# Копируем остальной код (без того, что отфильтровал .dockerignore)
COPY . .

# Продакшен-окружение
ENV NODE_ENV=production

# Старт приложения
CMD ["node", "index.js"]
