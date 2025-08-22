from fastapi import APIRouter

from ..api.v1 import router as v1_router
from ..api.routers import router as aws_router

router = APIRouter(prefix="/api")
router.include_router(v1_router)
router.include_router(aws_router)
