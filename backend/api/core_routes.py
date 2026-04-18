from fastapi import APIRouter, Query, HTTPException
from functools import lru_cache
from pydantic import BaseModel
from typing import List, Optional, Dict

from backend.service.user_service import UserService, SegmentService
from backend.service.behavior_service import BehaviorService
from backend.service.dashboard_service import DashboardService
from backend.service.management_dashboard import ManagementDashboard
from backend.service.tag_analysis_service import TagAnalysisService
from backend.service.report_service import ReportService
from backend.service.metrics_service import MetricsService
from backend.service.observe_service import ObserveService
from backend.doris.connect import ping, get_doris_version

router = APIRouter()


@lru_cache
def get_user_svc() -> UserService:
    return UserService()


@lru_cache
def get_seg_svc() -> SegmentService:
    return SegmentService()


@lru_cache
def get_beh_svc() -> BehaviorService:
    return BehaviorService()


@lru_cache
def get_dash_svc() -> DashboardService:
    return DashboardService()


@lru_cache
def get_mgmt_svc() -> ManagementDashboard:
    return ManagementDashboard()


@lru_cache
def get_tag_svc() -> TagAnalysisService:
    return TagAnalysisService()


@lru_cache
def get_rpt_svc() -> ReportService:
    return ReportService()


@lru_cache
def get_met_svc() -> MetricsService:
    return MetricsService()


@lru_cache
def get_obs_svc() -> ObserveService:
    return ObserveService()


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
    return await get_user_svc().query_wide(
        user_name, id_card, phone, asset_level, active_level,
        lifecycle_stage, preferred_channel, age_min, age_max,
        aum_min, aum_max, anomaly_flag, page, page_size,
    )


@router.get("/user/{user_id}")
async def get_user_detail(user_id: int):
    data = await get_user_svc().get_user_detail(user_id)
    if not data:
        raise HTTPException(status_code=404, detail="用户不存在")
    return data


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
    return await get_seg_svc().count_segment(rules)


@router.post("/segment/create")
async def create_segment(req: CreateSegmentReq):
    rules = [r.model_dump() for r in req.rules]
    return await get_seg_svc().create_segment(req.segment_name, rules, req.description, req.created_by)


@router.get("/segment/list")
async def list_segments():
    return await get_seg_svc().list_segments()


@router.delete("/segment/{segment_id}")
async def delete_segment(segment_id: int):
    ok = await get_seg_svc().delete_segment(segment_id)
    return {"success": ok}


@router.get("/segment/{segment_id}/users")
async def segment_users(
    segment_id: int,
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
):
    return await get_seg_svc().get_segment_users(segment_id, page, size)


@router.get("/segment/{segment_id}/stats")
async def segment_stats(segment_id: int):
    return await get_seg_svc().get_segment_stats(segment_id)


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
    return await get_beh_svc().funnel_analysis(req.steps, req.window_seconds, req.channel, req.segment_id)


@router.post("/behavior/retention")
async def retention_analysis(req: RetentionReq):
    return await get_beh_svc().retention_analysis(req.cohort_event, req.return_event, req.retention_days)


@router.get("/behavior/transaction")
async def transaction_analysis(channel: Optional[str] = None):
    return await get_beh_svc().transaction_analysis(channel=channel)


@router.get("/behavior/rfm")
async def rfm_analysis():
    return await get_beh_svc().rfm_analysis()


@router.get("/dashboard")
async def dashboard():
    return await get_dash_svc().get_overview()


@router.get("/management")
async def management_dashboard():
    return await get_mgmt_svc().get_overview()


@router.get("/tag-analysis/overview")
async def tag_overview():
    return await get_tag_svc().overview()


@router.get("/tag-analysis/users")
async def tag_users(tag_name: Optional[str] = None, is_risk: Optional[int] = None):
    return await get_tag_svc().tag_user_list(tag_name, is_risk)


@router.get("/tag-analysis/risk")
async def tag_risk():
    return await get_tag_svc().risk_tag_analysis()


@router.get("/tag-analysis/cross")
async def tag_cross():
    return await get_tag_svc().tag_asset_cross()


@router.get("/tag-analysis/cooccurrence")
async def tag_cooccurrence():
    return await get_tag_svc().tag_cooccurrence()


@router.post("/tag-analysis/run-classify")
async def run_classify():
    return await get_tag_svc().run_classify()


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
    }


@router.get("/report/overview")
async def report_overview():
    return await get_rpt_svc().business_overview()


@router.get("/report/transaction")
async def report_transaction():
    return await get_rpt_svc().transaction_report()


@router.get("/report/risk")
async def report_risk():
    return await get_rpt_svc().risk_report()


class MetricsQueryReq(BaseModel):
    dimensions: List[str] = []
    measures: List[str] = []
    limit: int = 100
    page: int = 1
    filters: List[Dict] = []
    sort_by: Optional[str] = None
    sort_dir: str = "DESC"
    top_n: Optional[int] = None
    calc_fields: List[Dict] = []
    time_range: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None


class MetricsCompareReq(BaseModel):
    dimensions: List[str] = []
    measures: List[str] = []
    compare_type: str = "mom"
    current_start: Optional[str] = None
    current_end: Optional[str] = None


class MetricsDrillReq(BaseModel):
    parent_dim: str
    parent_value: str
    child_dim: str
    measures: List[str] = []
    filters: List[Dict] = []


class SaveQueryReq(BaseModel):
    name: str
    config: Dict


@router.get("/metrics/definitions")
async def metrics_definitions():
    return await get_met_svc().get_definitions()


@router.get("/metrics/templates")
async def metrics_templates():
    return await get_met_svc().get_templates()


@router.post("/metrics/query")
async def metrics_query(req: MetricsQueryReq):
    return await get_met_svc().query(
        req.dimensions, req.measures, req.limit, req.page,
        req.filters, req.sort_by, req.sort_dir, req.top_n,
        req.calc_fields, req.time_range, req.start_date, req.end_date,
    )


@router.post("/metrics/compare")
async def metrics_compare(req: MetricsCompareReq):
    return await get_met_svc().compare(
        req.dimensions, req.measures, req.compare_type,
        req.current_start, req.current_end,
    )


@router.post("/metrics/drilldown")
async def metrics_drilldown(req: MetricsDrillReq):
    return await get_met_svc().drilldown(
        req.parent_dim, req.parent_value, req.child_dim, req.measures, req.filters,
    )


@router.post("/metrics/saved")
async def metrics_save(req: SaveQueryReq):
    return get_met_svc().save_query(req.name, req.config)


@router.get("/metrics/saved")
async def metrics_list_saved():
    return get_met_svc().list_saved()


@router.delete("/metrics/saved/{qid}")
async def metrics_delete_saved(qid: str):
    return {"success": get_met_svc().delete_saved(qid)}


@router.get("/metrics/history")
async def metrics_history(limit: int = Query(30, ge=1, le=100)):
    return get_met_svc().get_history(limit)


@router.get("/observe/logs")
async def observe_logs(
    path: Optional[str] = None,
    status_code: Optional[int] = None,
    method: Optional[str] = None,
    page: int = Query(1, ge=1),
    size: int = Query(100, ge=1, le=200),
):
    return await get_obs_svc().logs(path, status_code, method, page, size)


@router.get("/observe/stats")
async def observe_stats():
    return await get_obs_svc().stats()


@router.post("/observe/classify")
async def observe_classify():
    return await get_obs_svc().classify_logs()


@router.get("/observe/tag-stats")
async def observe_tag_stats():
    return await get_obs_svc().tag_stats()


@router.get("/trace/list")
async def trace_list(
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
):
    return await get_obs_svc().traces(page, size)


@router.get("/trace/{trace_id}")
async def trace_detail(trace_id: str):
    return await get_obs_svc().trace_detail(trace_id)
