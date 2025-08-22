from fastapi import APIRouter

from .aws import router as aws_router

router = APIRouter(prefix="/aws")
router.include_router(aws_router)
