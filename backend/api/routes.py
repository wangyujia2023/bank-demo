"""
银行CDP平台 - FastAPI 全部路由
"""
from fastapi import APIRouter, Query, HTTPException, UploadFile, File, Form
from pydantic import BaseModel
from typing import List, Optional, Dict
from backend.service.user_service import UserService, SegmentService
from backend.service.behavior_service import BehaviorService
from backend.service.cdp_service import WideQueryService, EtlService, BitmapOpsService, BehaviorAnalysisService
from backend.service.portrait_service import PortraitService, CrowdPackageService, TagAnalysisService as CdpTagAnalysisService
from backend.service.dashboard_service import DashboardService
from backend.service.tag_analysis_service import TagAnalysisService
from backend.service.report_service import ReportService
from backend.service.management_dashboard import ManagementDashboard
from backend.service.metrics_service import MetricsService
from backend.service.observe_service import ObserveService
from backend.service.benchmark_service import BenchmarkService
from backend.service.vector_search_service import VectorSearchService
from backend.service.satellite_service import SatelliteService
from backend.doris.connect import ping, get_doris_version

router = APIRouter()
user_svc  = UserService()
wide_svc  = WideQueryService()
etl_svc   = EtlService()
bmp_svc   = BitmapOpsService()
beh2_svc  = BehaviorAnalysisService()
seg_svc   = SegmentService()
tag_svc   = TagAnalysisService()
beh_svc   = BehaviorService()
dash_svc  = DashboardService()
mgmt_svc  = ManagementDashboard()
rpt_svc   = ReportService()
met_svc   = MetricsService()
obs_svc   = ObserveService()
bench_svc = BenchmarkService()
vec_svc      = VectorSearchService()
sat_svc      = SatelliteService()
portrait_svc = PortraitService()
crowd_svc    = CrowdPackageService()
cdp_tag_svc  = CdpTagAnalysisService()


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
    dimensions:  List[str] = []
    measures:    List[str] = []
    limit:       int = 100
    page:        int = 1
    filters:     List[Dict] = []
    sort_by:     Optional[str] = None
    sort_dir:    str = "DESC"
    top_n:       Optional[int] = None
    calc_fields: List[Dict] = []
    time_range:  Optional[str] = None
    start_date:  Optional[str] = None
    end_date:    Optional[str] = None

class MetricsCompareReq(BaseModel):
    dimensions:    List[str] = []
    measures:      List[str] = []
    compare_type:  str = "mom"
    current_start: Optional[str] = None
    current_end:   Optional[str] = None

class MetricsDrillReq(BaseModel):
    parent_dim:   str
    parent_value: str
    child_dim:    str
    measures:     List[str] = []
    filters:      List[Dict] = []

class SaveQueryReq(BaseModel):
    name:   str
    config: Dict

@router.get("/metrics/definitions")
async def metrics_definitions():
    return await met_svc.get_definitions()

@router.get("/metrics/templates")
async def metrics_templates():
    return await met_svc.get_templates()

@router.post("/metrics/query")
async def metrics_query(req: MetricsQueryReq):
    return await met_svc.query(
        req.dimensions, req.measures, req.limit, req.page,
        req.filters, req.sort_by, req.sort_dir, req.top_n,
        req.calc_fields, req.time_range, req.start_date, req.end_date,
    )

@router.post("/metrics/compare")
async def metrics_compare(req: MetricsCompareReq):
    return await met_svc.compare(
        req.dimensions, req.measures, req.compare_type,
        req.current_start, req.current_end,
    )

@router.post("/metrics/drilldown")
async def metrics_drilldown(req: MetricsDrillReq):
    return await met_svc.drilldown(
        req.parent_dim, req.parent_value, req.child_dim, req.measures, req.filters,
    )

@router.post("/metrics/saved")
async def metrics_save(req: SaveQueryReq):
    return met_svc.save_query(req.name, req.config)

@router.get("/metrics/saved")
async def metrics_list_saved():
    return met_svc.list_saved()

@router.delete("/metrics/saved/{qid}")
async def metrics_delete_saved(qid: str):
    return {"success": met_svc.delete_saved(qid)}

@router.get("/metrics/history")
async def metrics_history(limit: int = Query(30, ge=1, le=100)):
    return met_svc.get_history(limit)


# ================================================================
# 日志可观测性
# ================================================================
@router.get("/observe/logs")
async def observe_logs(
    path:        Optional[str] = None,
    status_code: Optional[int] = None,
    method:      Optional[str] = None,
    page:        int = Query(1, ge=1),
    size:        int = Query(100, ge=1, le=200),
):
    return await obs_svc.logs(path, status_code, method, page, size)

@router.get("/observe/stats")
async def observe_stats():
    return await obs_svc.stats()

@router.post("/observe/classify")
async def observe_classify():
    return await obs_svc.classify_logs()

@router.get("/observe/tag-stats")
async def observe_tag_stats():
    return await obs_svc.tag_stats()


# ================================================================
# 链路追踪
# ================================================================
@router.get("/trace/list")
async def trace_list(
    page:    int = Query(1, ge=1),
    size:    int = Query(20, ge=1, le=100),
):
    return await obs_svc.traces(page, size)

@router.get("/trace/{trace_id}")
async def trace_detail(trace_id: str):
    return await obs_svc.trace_detail(trace_id)


# ================================================================
# 用户标签分析（CDP）- 宽表查询 / 高表ETL / Bitmap人群 / 行为分析
# ================================================================

class WideQueryReq(BaseModel):
    tag_ids:   List[int] = []   # 选中的 tag_id 列表（AND 逻辑，对应列=1）
    page:      int = 1
    page_size: int = 20

class BitmapOpsReq(BaseModel):
    include_tag_ids: List[int] = []
    exclude_tag_ids: List[int] = []
    return_users:    bool = False
    limit:           int = 100

class TwoSetOpsReq(BaseModel):
    tag_ids_a:  List[int]
    tag_ids_b:  List[int]
    operation:  str = "AND"    # AND=交集 OR=并集 NOT=A差B

class CdpFunnelReq(BaseModel):
    steps:           List[str] = ["REGISTER", "LOGIN", "BROWSE_PRODUCT", "APPLY", "TRANSACTION"]
    window_seconds:  int = 86400
    filter_tag_ids:  Optional[List[int]] = None

class CdpRetentionReq(BaseModel):
    cohort_event:   str = "REGISTER"
    return_event:   str = "LOGIN"
    retention_days: Optional[List[int]] = None

# 宽表
@router.get("/cdp/wide/tag-meta")
async def cdp_wide_tag_meta():
    return wide_svc.get_tag_meta()

@router.post("/cdp/wide/query")
async def cdp_wide_query(req: WideQueryReq):
    return await wide_svc.query(req.tag_ids, req.page, req.page_size)

@router.get("/cdp/wide/distribution")
async def cdp_wide_distribution():
    return await wide_svc.distribution()

# 高表 ETL
@router.post("/cdp/etl/sync")
async def cdp_etl_sync():
    return await etl_svc.sync_wide_to_tall()

@router.get("/cdp/etl/overview")
async def cdp_etl_overview():
    return await etl_svc.get_tall_overview()

# Bitmap 人群交并差
@router.post("/cdp/bitmap/compute")
async def cdp_bitmap_compute(req: BitmapOpsReq):
    return await bmp_svc.compute(req.include_tag_ids, req.exclude_tag_ids, req.return_users, req.limit)

@router.post("/cdp/bitmap/two-set")
async def cdp_bitmap_two_set(req: TwoSetOpsReq):
    return await bmp_svc.two_set_ops(req.tag_ids_a, req.tag_ids_b, req.operation)

# 行为分析（Doris 内置函数）
@router.post("/cdp/behavior/funnel")
async def cdp_funnel(req: CdpFunnelReq):
    return await beh2_svc.funnel(req.steps, req.window_seconds, req.filter_tag_ids)

@router.post("/cdp/behavior/retention")
async def cdp_retention(req: CdpRetentionReq):
    return await beh2_svc.retention(req.cohort_event, req.return_event, req.retention_days)

@router.get("/cdp/behavior/path")
async def cdp_path(top_n: int = 10):
    return await beh2_svc.path_analysis(top_n)


# ================================================================
# 人群画像 & 人群包 & 标签分析（扩展）
# ================================================================

class TgiReq(BaseModel):
    include_tag_ids: List[int] = []

class CrossReq(BaseModel):
    cat1: str
    cat2: str

class GeoReq(BaseModel):
    include_tag_ids: List[int] = []

class TargetingReq(BaseModel):
    provinces: List[str]

class SaveCrowdReq(BaseModel):
    name:            str
    desc:            str = ""
    include_tag_ids: List[int] = []
    exclude_tag_ids: List[int] = []
    crowd_size:      int = 0

class CompareCrowdReq(BaseModel):
    id_a: str
    id_b: str

# 人群画像
@router.post("/cdp/portrait/tgi")
async def cdp_tgi(req: TgiReq):
    return await portrait_svc.tgi_analysis(req.include_tag_ids)

@router.post("/cdp/portrait/cross")
async def cdp_cross(req: CrossReq):
    return await portrait_svc.cross_analysis(req.cat1, req.cat2)

@router.post("/cdp/portrait/geo")
async def cdp_geo(req: GeoReq):
    return await portrait_svc.geo_distribution(req.include_tag_ids or None)

@router.post("/cdp/portrait/targeting")
async def cdp_targeting(req: TargetingReq):
    return await portrait_svc.targeting_distribution(req.provinces)

# 人群包
@router.post("/cdp/crowd/save")
async def cdp_crowd_save(req: SaveCrowdReq):
    return crowd_svc.save(req.name, req.desc, req.include_tag_ids, req.exclude_tag_ids, req.crowd_size)

@router.get("/cdp/crowd/list")
async def cdp_crowd_list():
    return crowd_svc.list_all()

@router.delete("/cdp/crowd/{crowd_id}")
async def cdp_crowd_delete(crowd_id: str):
    return {"success": crowd_svc.delete(crowd_id)}

@router.post("/cdp/crowd/compare")
async def cdp_crowd_compare(req: CompareCrowdReq):
    return await crowd_svc.compare(req.id_a, req.id_b)

# 标签分析
@router.post("/cdp/tag/weight")
async def cdp_tag_weight(req: TgiReq):
    return await cdp_tag_svc.tag_weight(req.include_tag_ids)

@router.get("/cdp/tag/anomaly")
async def cdp_anomaly():
    return await cdp_tag_svc.anomaly_detect()


# ================================================================
# ================================================================
# 向量检索（图片识别标签检索）
# ================================================================
class VectorSearchReq(BaseModel):
    query_vector: List[float]
    top_k: int = 5

class HybridSearchReq(BaseModel):
    query_vector:  Optional[List[float]] = None
    label_filters: Optional[List[str]]  = None
    description:   Optional[str]        = None
    top_k:         int                  = 5

@router.post("/vector/init")
async def vector_init():
    return await vec_svc.init_tables()

@router.get("/vector/users")
async def vector_users():
    return await vec_svc.get_users()

@router.get("/vector/labels")
async def vector_labels():
    return await vec_svc.get_labels()

@router.get("/vector/dim-labels")
async def vector_dim_labels():
    return await vec_svc.get_dim_labels()

@router.post("/vector/search/users")
async def vector_search_users(req: VectorSearchReq):
    if len(req.query_vector) != 8:
        raise HTTPException(status_code=400, detail="向量维度必须为 8")
    return await vec_svc.search_similar_users(req.query_vector, req.top_k)

@router.post("/vector/search/labels")
async def vector_search_labels(req: VectorSearchReq):
    if len(req.query_vector) != 8:
        raise HTTPException(status_code=400, detail="向量维度必须为 8")
    return await vec_svc.search_similar_labels(req.query_vector, req.top_k)

@router.post("/vector/search/hybrid")
async def vector_search_hybrid(req: HybridSearchReq):
    return await vec_svc.hybrid_search(
        req.query_vector, req.label_filters, req.description, req.top_k
    )

@router.post("/vector/search/by-photo")
async def vector_search_by_photo(
    photo:         UploadFile = File(...),
    label_filters: str        = Form("[]"),
    description:   str        = Form(""),
    top_k:         int        = Form(5),
):
    import json as _json
    filters     = _json.loads(label_filters)
    image_bytes = await photo.read()
    return await vec_svc.search_by_photo(image_bytes, filters, description or None, top_k)

@router.post("/vector/users/upload")
async def vector_add_user(
    user_name:    str        = Form(...),
    description:  str        = Form(""),
    avatar_style: str        = Form("custom"),
    labels:       str        = Form("[]"),
    photo:        UploadFile = File(...),
):
    import json as _json
    labels_list = _json.loads(labels)
    image_bytes = await photo.read()
    return await vec_svc.add_user_from_image(
        user_name, description, avatar_style, labels_list, image_bytes
    )


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

@router.get("/benchmark/audit-stats")
async def benchmark_audit_stats(limit: int = Query(300, ge=100, le=500)):
    return await bench_svc.audit_log_stats(limit)


# ================================================================
# 卫星数据采集分析
# ================================================================
@router.post("/satellite/init")
async def satellite_init():
    return await sat_svc.init_table()


# ================================================================
# 智能制造 - 数字孪生沙盘
# ================================================================
from backend.service.manufacturing_service import ManufacturingService
mfg_svc = ManufacturingService()

@router.post("/mfg/init")
async def mfg_init():
    return await mfg_svc.init_table()

@router.post("/mfg/generate")
async def mfg_generate():
    return await mfg_svc.generate_step()

@router.get("/mfg/overview")
async def mfg_overview():
    return await mfg_svc.get_overview()

@router.get("/mfg/oee-trend")
async def mfg_oee_trend():
    return await mfg_svc.get_oee_trend()

@router.get("/mfg/machine-status")
async def mfg_machine_status():
    return await mfg_svc.get_machine_status()

@router.get("/mfg/causal")
async def mfg_causal():
    return await mfg_svc.get_causal_analysis()

@router.post("/mfg/batch")
async def mfg_batch(steps: int = 5):
    return await mfg_svc.batch_generate(steps)

@router.get("/mfg/detail")
async def mfg_detail(limit: int = Query(60, ge=1, le=200)):
    return await mfg_svc.get_detail(limit)

@router.get("/mfg/machine-trend")
async def mfg_machine_trend(machine_id: str):
    return await mfg_svc.get_machine_trend(machine_id)

@router.get("/mfg/quality-stats")
async def mfg_quality_stats():
    return await mfg_svc.get_quality_stats()

@router.get("/mfg/energy-stats")
async def mfg_energy_stats():
    return await mfg_svc.get_energy_stats()

@router.get("/mfg/maintenance-stats")
async def mfg_maintenance_stats():
    return await mfg_svc.get_maintenance_stats()

@router.get("/mfg/process-trend")
async def mfg_process_trend():
    return await mfg_svc.get_process_trend()

@router.post("/mfg/reset")
async def mfg_reset():
    return await mfg_svc.reset()


# ================================================================
# 基金数字沙盘
# ================================================================
from backend.service.fund_service import FundService
fund_svc = FundService()

@router.post("/fund/init")
async def fund_init():
    return await fund_svc.init_table()

@router.post("/fund/generate")
async def fund_generate():
    return await fund_svc.generate_step()

@router.post("/fund/batch")
async def fund_batch(days: int = 5):
    return await fund_svc.batch_generate(days)

@router.post("/fund/reset")
async def fund_reset():
    return await fund_svc.reset()

@router.get("/fund/overview")
async def fund_overview():
    return await fund_svc.get_overview()

@router.get("/fund/list")
async def fund_list(sector: str = None, fund_type: str = None, risk: int = None):
    return await fund_svc.get_fund_list(sector, fund_type, risk)

@router.get("/fund/detail/{fund_id}")
async def fund_detail(fund_id: str):
    return await fund_svc.get_fund_detail(fund_id)

@router.get("/fund/nav/{fund_id}")
async def fund_nav(fund_id: str, days: int = Query(90, ge=10, le=365)):
    return await fund_svc.get_nav_history(fund_id, days)

@router.get("/fund/position/{fund_id}")
async def fund_position(fund_id: str):
    return await fund_svc.get_position(fund_id)

@router.get("/fund/manager/{manager_id}")
async def fund_manager(manager_id: str):
    return await fund_svc.get_manager(manager_id)

@router.get("/fund/peers/{fund_id}")
async def fund_peers(fund_id: str):
    return await fund_svc.get_peers(fund_id)

@router.get("/fund/sector-stats")
async def fund_sector_stats():
    return await fund_svc.get_sector_stats()

@router.get("/satellite/overview")
async def satellite_overview():
    return await sat_svc.overview()

@router.get("/satellite/charts")
async def satellite_charts():
    return await sat_svc.charts()

@router.get("/satellite/query")
async def satellite_query(
    satellite_id:   Optional[str] = None,
    satellite_name: Optional[str] = None,
    satellite_type: Optional[str] = None,
    data_type:      Optional[str] = None,
    target_type:    Optional[str] = None,
    quality_min:    Optional[int] = None,
    status:         Optional[str] = None,
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
):
    return await sat_svc.query(satellite_id, satellite_name, satellite_type,
                               data_type, target_type, quality_min, status, page, size)
