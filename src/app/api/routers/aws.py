from typing import Annotated, Any

from botocore.exceptions import ClientError
from fastapi import APIRouter, Depends, HTTPException, UploadFile

from ..dependencies import get_bucket_size, get_s3_client, upload_file_to_s3

router = APIRouter(tags=["aws_router"])


# f"{API_GATEWAY_URL}{BUCKET_NAME}/{os.path.basename(csv_path)}"
@router.post(
    "/{bucket_name}/{file_path}", dependencies=[Depends(get_s3_client)], status_code=201
)
async def upload_s3(
    bucket_name: str,
    file: UploadFile,
    s3_client: Annotated[Any, Depends(get_s3_client)],
) -> dict[str, str]:
    try:
        return await upload_file_to_s3(
            file=file, bucket_name=bucket_name, s3_client=s3_client
        )
    except ClientError as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to upload file to S3: {str(e)}"
        ) from e


@router.get(
    "/{bucket_name}/size", dependencies=[Depends(get_s3_client)], status_code=201
)
async def bucket_size(
    bucket_name: str,
    s3_client: Annotated[Any, Depends(get_s3_client)],
) -> dict[str, float]:
    try:
        return await get_bucket_size(bucket_name=bucket_name, s3_client=s3_client)
    except ClientError as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to upload file to S3: {str(e)}"
        ) from e
