"""
银行CDP平台 - FastAPI 全部路由
"""
from fastapi import APIRouter, Query, HTTPException, UploadFile, File, Form
from pydantic import BaseModel
from typing import List, Optional, Dict
from backend.service.user_service import UserService, SegmentService
from backend.service.behavior_service import BehaviorService
from backend.service.dashboard_service import DashboardService
from backend.service.tag_analysis_service import TagAnalysisService
from backend.service.report_service import ReportService
from backend.service.management_dashboard import ManagementDashboard
from backend.service.metrics_service import MetricsService
from backend.service.observe_service import ObserveService
from backend.service.benchmark_service import BenchmarkService
from backend.doris.connect import ping, get_doris_version

router = APIRouter()
user_svc  = UserService()
seg_svc   = SegmentService()
tag_svc   = TagAnalysisService()
beh_svc   = BehaviorService()
dash_svc  = DashboardService()
mgmt_svc  = ManagementDashboard()
rpt_svc   = ReportService()
met_svc   = MetricsService()
obs_svc   = ObserveService()
bench_svc = BenchmarkService()


# ================================================================
# 用户宽表
# ================================================================
@router.get("/user/wide")
async def query_user_wide(
    user_name: Optional[str] = None,
    id_card: Optional[str] = None,
    phone: Optional[str] = None,
    asset_level: Optional[str] = None,
    active_level: Optional[str] = None,
    lifecycle_stage: Optional[str] = None,
    preferred_channel: Optional[str] = None,
    age_min: Optional[int] = None,
    age_max: Optional[int] = None,
    aum_min: Optional[float] = None,
    aum_max: Optional[float] = None,
    anomaly_flag: Optional[int] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
):
    return await user_svc.query_wide(
        user_name, id_card, phone, asset_level, active_level,
        lifecycle_stage, preferred_channel, age_min, age_max,
        aum_min, aum_max, anomaly_flag, page, page_size,
    )


@router.get("/user/{user_id}")
async def get_user_detail(user_id: int):
    data = await user_svc.get_user_detail(user_id)
    if not data:
        raise HTTPException(status_code=404, detail="用户不存在")
    return data


# ================================================================
# 人群圈选
# ================================================================
class SegmentRule(BaseModel):
    tag_name: str
    tag_values: List[str]
    op: str = "OR"
    exclude: bool = False


class CreateSegmentReq(BaseModel):
    segment_name: str
    description: str = ""
    rules: List[SegmentRule]
    created_by: str = "system"


class CountSegmentReq(BaseModel):
    rules: List[SegmentRule]


@router.post("/segment/count")
async def count_segment(req: CountSegmentReq):
    rules = [r.model_dump() for r in req.rules]
    return await seg_svc.count_segment(rules)


@router.post("/segment/create")
async def create_segment(req: CreateSegmentReq):
    rules = [r.model_dump() for r in req.rules]
    return await seg_svc.create_segment(req.segment_name, rules, req.description, req.created_by)


@router.get("/segment/list")
async def list_segments():
    return await seg_svc.list_segments()


@router.delete("/segment/{segment_id}")
async def delete_segment(segment_id: int):
    ok = await seg_svc.delete_segment(segment_id)
    return {"success": ok}


@router.get("/segment/{segment_id}/users")
async def segment_users(
    segment_id: int,
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
):
    return await seg_svc.get_segment_users(segment_id, page, size)


@router.get("/segment/{segment_id}/stats")
async def segment_stats(segment_id: int):
    return await seg_svc.get_segment_stats(segment_id)


# ================================================================
# 行为分析（日期仅作展示参数，不参与 SQL 过滤）
# ================================================================
class FunnelReq(BaseModel):
    steps: Optional[List[str]] = None
    window_seconds: int = 86400
    channel: Optional[str] = None
    segment_id: Optional[int] = None


class RetentionReq(BaseModel):
    cohort_event: str = "REGISTER"
    return_event: str = "LOGIN"
    retention_days: Optional[List[int]] = None


@router.post("/behavior/funnel")
async def funnel_analysis(req: FunnelReq):
    return await beh_svc.funnel_analysis(req.steps, req.window_seconds, req.channel, req.segment_id)


@router.post("/behavior/retention")
async def retention_analysis(req: RetentionReq):
    return await beh_svc.retention_analysis(req.cohort_event, req.return_event, req.retention_days)


@router.get("/behavior/transaction")
async def transaction_analysis(channel: Optional[str] = None):
    return await beh_svc.transaction_analysis(channel=channel)


@router.get("/behavior/rfm")
async def rfm_analysis():
    return await beh_svc.rfm_analysis()


# ================================================================
# 大盘
# ================================================================
@router.get("/dashboard")
async def dashboard():
    return await dash_svc.get_overview()

@router.get("/management")
async def management_dashboard():
    return await mgmt_svc.get_overview()

# ================================================================
# 系统配置 & 健康检查
# ================================================================
# ================================================================
# AI 日志标签分析
# ================================================================
@router.get("/tag-analysis/overview")
async def tag_overview():
    return await tag_svc.overview()


@router.get("/tag-analysis/users")
async def tag_users(tag_name: Optional[str] = None, is_risk: Optional[int] = None):
    return await tag_svc.tag_user_list(tag_name, is_risk)


@router.get("/tag-analysis/risk")
async def tag_risk():
    return await tag_svc.risk_tag_analysis()


@router.get("/tag-analysis/cross")
async def tag_cross():
    return await tag_svc.tag_asset_cross()


@router.get("/tag-analysis/cooccurrence")
async def tag_cooccurrence():
    return await tag_svc.tag_cooccurrence()


@router.post("/tag-analysis/run-classify")
async def run_classify():
    return await tag_svc.run_classify()


@router.get("/system/health")
async def health():
    ok = await ping()
    version = await get_doris_version() if ok else "N/A"
    return {"status": "ok" if ok else "error", "doris_version": version}


@router.get("/system/config")
async def get_config():
    from backend.settings import settings
    return {
        "doris_host": settings.DORIS_HOST,
        "doris_port": settings.DORIS_PORT,
        "doris_database": settings.DORIS_DATABASE,
        "hasp_enabled": settings.DORIS_HASP_ENABLED,
        "ai_provider": settings.AI_FUNCTION_PROVIDER,
        "ai_model": settings.AI_MODEL,
    }

# ================================================================
# 银行报表
# ================================================================
@router.get("/report/overview")
async def report_overview():
    return await rpt_svc.business_overview()

@router.get("/report/transaction")
async def report_transaction():
    return await rpt_svc.transaction_report()

@router.get("/report/risk")
async def report_risk():
    return await rpt_svc.risk_report()


# ================================================================
# 指标平台
# ================================================================
class MetricsQueryReq(BaseModel):
    dimensions: List[str] = []
    measures:   List[str] = []
    limit:      int = 100

@router.get("/metrics/definitions")
async def metrics_definitions():
    return await met_svc.get_definitions()

@router.post("/metrics/query")
async def metrics_query(req: MetricsQueryReq):
    return await met_svc.query(req.dimensions, req.measures, req.limit)


# ================================================================
# 日志可观测性
# ================================================================
@router.get("/observe/logs")
async def observe_logs(
    search:  Optional[str] = None,
    level:   Optional[str] = None,
    service: Optional[str] = None,
    page:    int = Query(1, ge=1),
    size:    int = Query(50, ge=1, le=200),
):
    return await obs_svc.query_logs(search, level, service, page, size)

@router.get("/observe/stats")
async def observe_stats():
    return await obs_svc.log_stats()


# ================================================================
# 链路追踪
# ================================================================
@router.get("/trace/list")
async def trace_list(
    service: Optional[str] = None,
    status:  Optional[str] = None,
    page:    int = Query(1, ge=1),
    size:    int = Query(20, ge=1, le=100),
):
    return await obs_svc.trace_list(service, status, page, size)

@router.get("/trace/{trace_id}")
async def trace_detail(trace_id: str):
    return await obs_svc.trace_detail(trace_id)


# ================================================================
# 高并发点查压测
# ================================================================
class BenchmarkReq(BaseModel):
    threads:    int = 10
    iterations: int = 20
    query_type: str = "point"

@router.post("/benchmark/run")
async def benchmark_run(req: BenchmarkReq):
    return await bench_svc.run(req.threads, req.iterations, req.query_type)
