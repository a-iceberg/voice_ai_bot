FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt

RUN pip3 install --upgrade pip && \
    pip3 install torch==2.6.0 --extra-index-url https://download.pytorch.org/whl/cpu && \
    pip3 install --no-cache-dir -r /app/requirements.txt

COPY rag_manager.py rag_server.py update_rag.py /app/

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV DOC_DIR=/app/data/instructions
ENV VECTOR_CACHE_DIR=/app/vector_db_cache
ENV QDRANT_COLLECTION=instructions
ENV RAG_HOST=0.0.0.0
ENV RAG_PORT=7420

EXPOSE 7420

CMD ["python", "-u", "rag_server.py"]
