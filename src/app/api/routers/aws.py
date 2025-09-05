"""
AWS S3 Upload Router

Provides FastAPI endpoints to:
1. Upload files to an S3 bucket with gzip compression.
2. Get the size of a specified S3 bucket.

Features:
- Asynchronous operations using FastAPI and asyncio.
- Compression is offloaded to a worker thread to avoid blocking the event loop.
- Handles AWS ClientErrors with proper HTTP responses.
"""

import asyncio
import os
from io import BytesIO
from typing import Annotated, Any

from botocore.exceptions import ClientError
from fastapi import APIRouter, Depends, HTTPException, UploadFile

from ..dependencies import (
    _compress_bytes,
    get_bucket_size,
    get_s3_client,
    upload_file_to_s3,
)

router = APIRouter(tags=["aws_router"])


@router.post(
    "/{bucket_name}",
    dependencies=[Depends(get_s3_client)],
    status_code=201,
    summary="Upload a file to S3",
    description="Uploads a file to the specified S3 bucket. "
    "The file is compressed with gzip, original extension is removed, "
    "and '.gz' is appended.",
)
async def upload_s3(
    bucket_name: str,
    file: UploadFile,
    file_path: str,
    s3_client: Annotated[Any, Depends(get_s3_client)],
) -> dict[str, str]:
    try:
        # Read file into memory (async)
        file_bytes: bytes = await file.read()

        # Offload compression to a worker thread
        compressed_buffer: BytesIO = await asyncio.to_thread(
            _compress_bytes, file_bytes
        )

        # Strip extension → append .gz
        base_filename, _ = os.path.splitext(file.filename)
        base_file_path, _ = os.path.splitext(file_path)

        compressed_file = UploadFile(
            filename=f"{base_filename}.gz",
            file=compressed_buffer,
            headers=file.headers,
        )

        # Upload to S3
        return await upload_file_to_s3(
            file=compressed_file,
            bucket_name=bucket_name,
            s3_client=s3_client,
            object_name=f"{base_file_path}.gz",
        )
    except ClientError as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to upload file to S3: {str(e)}"
        ) from e


@router.get(
    "/{bucket_name}/size",
    dependencies=[Depends(get_s3_client)],
    status_code=201,
    summary="Get S3 bucket size",
    description="Returns the total size of the specified S3 bucket in bytes.",
)
async def bucket_size(
    bucket_name: str,
    s3_client: Annotated[Any, Depends(get_s3_client)],
) -> dict[str, float]:
    try:
        return await get_bucket_size(bucket_name=bucket_name, s3_client=s3_client)
    except ClientError as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get bucket size: {str(e)}"
        ) from e
