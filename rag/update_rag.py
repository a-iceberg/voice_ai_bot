import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rag_manager import RagManager


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    logger = logging.getLogger("update_rag")
    logger.info("Rebuilding local BM25 cache from Qdrant collection (no file sync)")

    async def run():
        rag = RagManager(logger)
        await rag.initialize(ensure_collection=False)
        await rag.refresh_bm25_cache()
        n = len(rag.chunk_store or {})
        logger.info(f"BM25 cache refreshed, chunks={n}")

    asyncio.run(run())


if __name__ == "__main__":
    main()
