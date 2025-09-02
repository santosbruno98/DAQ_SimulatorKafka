import mimetypes
import os
from typing import Annotated, Any, cast

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from fastapi import Depends, HTTPException, Request, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from ..core.config import settings
from ..core.db.database import async_get_db
from ..core.exceptions.http_exceptions import (
    ForbiddenException,
    RateLimitException,
    UnauthorizedException,
)
from ..core.logger import logging
from ..core.security import TokenType, oauth2_scheme, verify_token
from ..core.utils.rate_limit import rate_limiter
from ..crud.crud_rate_limit import crud_rate_limits
from ..crud.crud_tier import crud_tiers
from ..crud.crud_users import crud_users
from ..schemas.rate_limit import RateLimitRead, sanitize_path
from ..schemas.tier import TierRead

logger = logging.getLogger(__name__)

DEFAULT_LIMIT = settings.DEFAULT_RATE_LIMIT_LIMIT
DEFAULT_PERIOD = settings.DEFAULT_RATE_LIMIT_PERIOD


async def get_current_user(
    token: Annotated[str, Depends(oauth2_scheme)],
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> dict[str, Any] | None:
    token_data = await verify_token(token, TokenType.ACCESS, db)
    if token_data is None:
        raise UnauthorizedException("User not authenticated.")

    if "@" in token_data.username_or_email:
        user = await crud_users.get(
            db=db, email=token_data.username_or_email, is_deleted=False
        )
    else:
        user = await crud_users.get(
            db=db, username=token_data.username_or_email, is_deleted=False
        )

    if user:
        return cast(dict[str, Any], user)

    raise UnauthorizedException("User not authenticated.")


async def get_optional_user(
    request: Request, db: AsyncSession = Depends(async_get_db)
) -> dict | None:
    token = request.headers.get("Authorization")
    if not token:
        return None

    try:
        token_type, _, token_value = token.partition(" ")
        if token_type.lower() != "bearer" or not token_value:
            return None

        token_data = await verify_token(token_value, TokenType.ACCESS, db)
        if token_data is None:
            return None

        return await get_current_user(token_value, db=db)

    except HTTPException as http_exc:
        if http_exc.status_code != 401:
            logger.error(
                f"Unexpected HTTPException in get_optional_user: {http_exc.detail}"
            )
        return None

    except Exception as exc:
        logger.error(f"Unexpected error in get_optional_user: {exc}")
        return None


async def get_current_superuser(
    current_user: Annotated[dict, Depends(get_current_user)],
) -> dict:
    if not current_user["is_superuser"]:
        raise ForbiddenException("You do not have enough privileges.")

    return current_user


async def rate_limiter_dependency(
    request: Request,
    db: Annotated[AsyncSession, Depends(async_get_db)],
    user: dict | None = Depends(get_optional_user),
) -> None:
    if hasattr(request.app.state, "initialization_complete"):
        await request.app.state.initialization_complete.wait()

    path = sanitize_path(request.url.path)
    if user:
        user_id = user["id"]
        tier = await crud_tiers.get(db, id=user["tier_id"], schema_to_select=TierRead)
        if tier:
            tier = cast(TierRead, tier)
            rate_limit = await crud_rate_limits.get(
                db=db, tier_id=tier.id, path=path, schema_to_select=RateLimitRead
            )
            if rate_limit:
                rate_limit = cast(RateLimitRead, rate_limit)
                limit, period = rate_limit.limit, rate_limit.period
            else:
                logger.warning(
                    f"User {user_id} with tier '{tier.name}' has no specific rate limit for path '{path}'. \
                        Applying default rate limit."
                )
                limit, period = DEFAULT_LIMIT, DEFAULT_PERIOD
        else:
            logger.warning(
                f"User {user_id} has no assigned tier. Applying default rate limit."
            )
            limit, period = DEFAULT_LIMIT, DEFAULT_PERIOD
    else:
        user_id = request.client.host if request.client else "unknown"
        limit, period = DEFAULT_LIMIT, DEFAULT_PERIOD

    is_limited = await rate_limiter.is_rate_limited(
        db=db, user_id=user_id, path=path, limit=limit, period=period
    )
    if is_limited:
        raise RateLimitException("Rate limit exceeded.")


# add a dependency for the aws upload
async def get_s3_client():
    """
    Dependency to provide an valid s3 client
    """
    try:
        load_dotenv()
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION"),
        )
        return s3_client
    except (BotoCoreError, ClientError) as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to initialize S3 client: {str(e)}"
        ) from e


async def upload_file_to_s3(
    file: UploadFile,
    bucket_name: str,
    s3_client: Depends(get_s3_client),
    object_name: str | None = None,
) -> dict[str, str]:
    """Uploads a file to an S3 bucket."""
    if object_name is None:
        object_name = os.path.basename(file.filename)

    try:
        # reset pointer
        file.file.seek(0)

        content_type, _ = mimetypes.guess_type(file.filename)
        if not content_type:
            content_type = (
                "application/octet-stream"  # Default content type if not detected
            )

        s3_client.upload_fileobj(
            file.file,
            bucket_name,
            object_name,
            ExtraArgs={
                "ContentType": file.content_type,
            },
        )
        return {"bucket": bucket_name, "object": object_name, "status": "uploaded"}
    except ClientError as e:
        raise HTTPException(
            status_code=501, detail=f"Failed to upload file to S3: {str(e)}"
        ) from e
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to upload file to S3: {str(e)}"
        ) from e


async def get_bucket_size(
    bucket_name: str, s3_client: Depends(get_s3_client)
) -> dict[str, float]:
    """Returns the size of the bucket in GB"""

    bucket_size: float = 0.0
    try:
        allbuckets = s3_client.list_buckets()
        print(
            "------------------------BUCKET LISTING--------------------\n %s",
            allbuckets,
        )
        if bucket_name not in [bucket["Name"] for bucket in allbuckets["Buckets"]]:
            raise HTTPException(
                status_code=404, detail=f"Bucket {bucket_name} not found."
            )
        s3 = boto3.resource(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION"),
        )

        bucket = s3.Bucket(bucket_name)
        for bucket_obj in bucket.objects.all():
            bucket_size += bucket_obj.size

        return {"size_gb": bucket_size / 1024 / 1024 / 1024}

    except ClientError as e:
        raise HTTPException(
            status_code=501, detail=f"Failed to get bucket size: {str(e)}"
        ) from e
