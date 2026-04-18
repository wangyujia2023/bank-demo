from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from functools import lru_cache
from pydantic import BaseModel
from typing import List, Optional

from backend.service.vector_search_service import VectorSearchService

router = APIRouter()


@lru_cache
def get_vec_svc() -> VectorSearchService:
    return VectorSearchService()


class VectorSearchReq(BaseModel):
    query_vector: List[float]
    top_k: int = 5


class HybridSearchReq(BaseModel):
    query_vector: Optional[List[float]] = None
    label_filters: Optional[List[str]] = None
    description: Optional[str] = None
    top_k: int = 5


@router.post("/vector/init")
async def vector_init():
    return await get_vec_svc().init_tables()


@router.get("/vector/users")
async def vector_users():
    return await get_vec_svc().get_users()


@router.get("/vector/labels")
async def vector_labels():
    return await get_vec_svc().get_labels()


@router.get("/vector/dim-labels")
async def vector_dim_labels():
    return await get_vec_svc().get_dim_labels()


@router.post("/vector/search/users")
async def vector_search_users(req: VectorSearchReq):
    if len(req.query_vector) != 8:
        raise HTTPException(status_code=400, detail="向量维度必须为 8")
    return await get_vec_svc().search_similar_users(req.query_vector, req.top_k)


@router.post("/vector/search/labels")
async def vector_search_labels(req: VectorSearchReq):
    if len(req.query_vector) != 8:
        raise HTTPException(status_code=400, detail="向量维度必须为 8")
    return await get_vec_svc().search_similar_labels(req.query_vector, req.top_k)


@router.post("/vector/search/hybrid")
async def vector_search_hybrid(req: HybridSearchReq):
    return await get_vec_svc().hybrid_search(
        req.query_vector, req.label_filters, req.description, req.top_k
    )


@router.post("/vector/search/by-photo")
async def vector_search_by_photo(
    photo: UploadFile = File(...),
    label_filters: str = Form("[]"),
    description: str = Form(""),
    top_k: int = Form(5),
):
    import json as _json
    filters = _json.loads(label_filters)
    image_bytes = await photo.read()
    return await get_vec_svc().search_by_photo(image_bytes, filters, description or None, top_k)


@router.post("/vector/users/upload")
async def vector_add_user(
    user_name: str = Form(...),
    description: str = Form(""),
    avatar_style: str = Form("custom"),
    labels: str = Form("[]"),
    photo: UploadFile = File(...),
):
    import json as _json
    labels_list = _json.loads(labels)
    image_bytes = await photo.read()
    return await get_vec_svc().add_user_from_image(
        user_name, description, avatar_style, labels_list, image_bytes
    )
