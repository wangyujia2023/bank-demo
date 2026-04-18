from fastapi import APIRouter, Query
from pydantic import BaseModel
from functools import lru_cache

from backend.service.benchmark_service import BenchmarkService

router = APIRouter()


@lru_cache
def get_bench_svc() -> BenchmarkService:
    return BenchmarkService()


class BenchmarkReq(BaseModel):
    threads: int = 10
    iterations: int = 20
    query_type: str = "point"


@router.post("/benchmark/run")
async def benchmark_run(req: BenchmarkReq):
    return await get_bench_svc().run(req.threads, req.iterations, req.query_type)


@router.get("/benchmark/audit-stats")
async def benchmark_audit_stats(limit: int = Query(300, ge=100, le=500)):
    return await get_bench_svc().audit_log_stats(limit)
