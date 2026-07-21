import os
import logging
import asyncio
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import uvicorn

from rag_manager import RagManager

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
RAG_HOST = os.getenv("RAG_HOST", "0.0.0.0")
RAG_PORT = int(os.getenv("RAG_PORT", "7420"))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("voice_rag")

rag_manager: Optional[RagManager] = None
rag_ready = False


class SearchRequest(BaseModel):
    query: str = Field(..., min_length=1)
    k_vector: int = Field(default=6, ge=1, le=20)
    k_bm25: int = Field(default=6, ge=1, le=20)
    top_k: int = Field(default=6, ge=1, le=20)


class SearchResponse(BaseModel):
    ok: bool
    chunks: list
    context: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    global rag_manager, rag_ready
    logger.info("RAG sidecar starting (search-only, no sync)")
    try:
        rag_manager = RagManager(logger)
        await rag_manager.initialize(ensure_collection=False)
        rag_ready = True
        logger.info("RAG sidecar ready")
    except Exception as e:
        logger.error(f"RAG sidecar init failed: {e}")
        rag_ready = False
    yield
    rag_ready = False
    rag_manager = None


app = FastAPI(title="voice_ai_bot RAG", lifespan=lifespan)


@app.get("/health")
async def health():
    body = {
        "ok": rag_ready,
        "bm25": bool(rag_manager and rag_manager.bm25_retriever),
    }
    # 503 пока модель не загружена — docker healthcheck / depends_on ждут ready
    if not rag_ready:
        return JSONResponse(status_code=503, content=body)
    return body


@app.post("/search", response_model=SearchResponse)
async def search(req: SearchRequest):
    if not rag_ready or not rag_manager:
        raise HTTPException(status_code=503, detail="RAG not ready")
    query = (req.query or "").strip()
    if not query:
        return SearchResponse(ok=True, chunks=[], context="")
    try:
        chunks = await rag_manager.hybrid_search(
            query,
            k_vector=req.k_vector,
            k_bm25=req.k_bm25,
            top_k=req.top_k,
        )
        context = "\n\n".join(
            c.get("page_content", "") for c in chunks if c.get("page_content")
        )
        return SearchResponse(ok=True, chunks=chunks, context=context)
    except Exception as e:
        logger.error(f"Search error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def main():
    uvicorn.run(
        "rag_server:app",
        host=RAG_HOST,
        port=RAG_PORT,
        log_level=LOG_LEVEL.lower(),
    )


if __name__ == "__main__":
    main()
