from fastapi import APIRouter
from functools import lru_cache
from pydantic import BaseModel
from typing import List, Optional

from backend.service.news_service import NewsService

router = APIRouter()


@lru_cache
def get_news_svc() -> NewsService:
    return NewsService()


class NewsAIReq(BaseModel):
    article_ids: Optional[List[str]] = None


class NewsAddRequest(BaseModel):
    title: str
    content: str
    source: str
    sector: str


@router.post("/news/init")
async def news_init():
    return await get_news_svc().init_table()


@router.post("/news/import")
async def news_import():
    return await get_news_svc().import_news()


@router.post("/news/add-manual")
async def news_add_manual(req: NewsAddRequest):
    return await get_news_svc().add_manual_news(req.title, req.content, req.source, req.sector)


@router.get("/news/list")
async def news_list(sector: str = None, sentiment: str = None, keyword: str = None):
    return await get_news_svc().get_list(sector, sentiment, keyword)


@router.get("/news/detail/{article_id}")
async def news_detail(article_id: str):
    return await get_news_svc().get_detail(article_id)


@router.get("/news/stats")
async def news_stats():
    return await get_news_svc().get_stats()


@router.post("/news/summarize")
async def news_summarize(req: NewsAIReq = None):
    ids = req.article_ids if req else None
    return await get_news_svc().run_summarize(ids)


@router.post("/news/sentiment")
async def news_sentiment(req: NewsAIReq = None):
    ids = req.article_ids if req else None
    return await get_news_svc().run_sentiment(ids)


@router.post("/news/extract")
async def news_extract(req: NewsAIReq = None):
    ids = req.article_ids if req else None
    return await get_news_svc().run_extract(ids)


@router.get("/news/tag-analysis")
async def news_tag_analysis():
    return await get_news_svc().get_tag_analysis()


@router.get("/news/sector-metrics")
async def news_sector_metrics():
    return await get_news_svc().get_sector_metrics()


@router.get("/news/signals")
async def news_signals():
    return await get_news_svc().get_signals()


@router.get("/news/hot-companies")
async def news_hot_companies():
    return await get_news_svc().get_hot_companies()


@router.post("/news/run-all-ai")
async def news_run_all_ai():
    return await get_news_svc().run_all_ai()
