from fastapi import APIRouter
from functools import lru_cache
from pydantic import BaseModel
from typing import List, Optional

from backend.service.cdp_service import WideQueryService, EtlService, BitmapOpsService, BehaviorAnalysisService
from backend.service.portrait_service import PortraitService, CrowdPackageService, TagAnalysisService as CdpTagAnalysisService

router = APIRouter()


@lru_cache
def get_wide_svc() -> WideQueryService:
    return WideQueryService()


@lru_cache
def get_etl_svc() -> EtlService:
    return EtlService()


@lru_cache
def get_bmp_svc() -> BitmapOpsService:
    return BitmapOpsService()


@lru_cache
def get_beh2_svc() -> BehaviorAnalysisService:
    return BehaviorAnalysisService()


@lru_cache
def get_portrait_svc() -> PortraitService:
    return PortraitService()


@lru_cache
def get_crowd_svc() -> CrowdPackageService:
    return CrowdPackageService()


@lru_cache
def get_cdp_tag_svc() -> CdpTagAnalysisService:
    return CdpTagAnalysisService()


class WideQueryReq(BaseModel):
    tag_ids: List[int] = []
    page: int = 1
    page_size: int = 20


class BitmapOpsReq(BaseModel):
    include_tag_ids: List[int] = []
    exclude_tag_ids: List[int] = []
    return_users: bool = False
    limit: int = 100


class TwoSetOpsReq(BaseModel):
    tag_ids_a: List[int]
    tag_ids_b: List[int]
    operation: str = "AND"


class CdpFunnelReq(BaseModel):
    steps: List[str] = ["REGISTER", "LOGIN", "BROWSE_PRODUCT", "APPLY", "TRANSACTION"]
    window_seconds: int = 86400
    filter_tag_ids: Optional[List[int]] = None


class CdpRetentionReq(BaseModel):
    cohort_event: str = "REGISTER"
    return_event: str = "LOGIN"
    retention_days: Optional[List[int]] = None


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
    name: str
    desc: str = ""
    include_tag_ids: List[int] = []
    exclude_tag_ids: List[int] = []
    crowd_size: int = 0


class CompareCrowdReq(BaseModel):
    id_a: str
    id_b: str


@router.get("/cdp/wide/tag-meta")
async def cdp_wide_tag_meta():
    return get_wide_svc().get_tag_meta()


@router.post("/cdp/wide/query")
async def cdp_wide_query(req: WideQueryReq):
    return await get_wide_svc().query(req.tag_ids, req.page, req.page_size)


@router.get("/cdp/wide/distribution")
async def cdp_wide_distribution():
    return await get_wide_svc().distribution()


@router.post("/cdp/etl/sync")
async def cdp_etl_sync():
    return await get_etl_svc().sync_wide_to_tall()


@router.get("/cdp/etl/overview")
async def cdp_etl_overview():
    return await get_etl_svc().get_tall_overview()


@router.post("/cdp/bitmap/compute")
async def cdp_bitmap_compute(req: BitmapOpsReq):
    return await get_bmp_svc().compute(req.include_tag_ids, req.exclude_tag_ids, req.return_users, req.limit)


@router.post("/cdp/bitmap/two-set")
async def cdp_bitmap_two_set(req: TwoSetOpsReq):
    return await get_bmp_svc().two_set_ops(req.tag_ids_a, req.tag_ids_b, req.operation)


@router.post("/cdp/behavior/funnel")
async def cdp_funnel(req: CdpFunnelReq):
    return await get_beh2_svc().funnel(req.steps, req.window_seconds, req.filter_tag_ids)


@router.post("/cdp/behavior/retention")
async def cdp_retention(req: CdpRetentionReq):
    return await get_beh2_svc().retention(req.cohort_event, req.return_event, req.retention_days)


@router.get("/cdp/behavior/path")
async def cdp_path(top_n: int = 10):
    return await get_beh2_svc().path_analysis(top_n)


@router.post("/cdp/portrait/tgi")
async def cdp_tgi(req: TgiReq):
    return await get_portrait_svc().tgi_analysis(req.include_tag_ids)


@router.post("/cdp/portrait/cross")
async def cdp_cross(req: CrossReq):
    return await get_portrait_svc().cross_analysis(req.cat1, req.cat2)


@router.post("/cdp/portrait/geo")
async def cdp_geo(req: GeoReq):
    return await get_portrait_svc().geo_distribution(req.include_tag_ids or None)


@router.post("/cdp/portrait/targeting")
async def cdp_targeting(req: TargetingReq):
    return await get_portrait_svc().targeting_distribution(req.provinces)


@router.post("/cdp/crowd/save")
async def cdp_crowd_save(req: SaveCrowdReq):
    return get_crowd_svc().save(req.name, req.desc, req.include_tag_ids, req.exclude_tag_ids, req.crowd_size)


@router.get("/cdp/crowd/list")
async def cdp_crowd_list():
    return get_crowd_svc().list_all()


@router.delete("/cdp/crowd/{crowd_id}")
async def cdp_crowd_delete(crowd_id: str):
    return {"success": get_crowd_svc().delete(crowd_id)}


@router.post("/cdp/crowd/compare")
async def cdp_crowd_compare(req: CompareCrowdReq):
    return await get_crowd_svc().compare(req.id_a, req.id_b)


@router.post("/cdp/tag/weight")
async def cdp_tag_weight(req: TgiReq):
    return await get_cdp_tag_svc().tag_weight(req.include_tag_ids)


@router.get("/cdp/tag/anomaly")
async def cdp_anomaly():
    return await get_cdp_tag_svc().anomaly_detect()
