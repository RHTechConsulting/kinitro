"""
Authentication system for Kinitro Backend API.
"""

import hashlib
import secrets
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from fastapi import Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader
from sqlalchemy import select

from backend.models import ApiKey
from core.log import get_logger

logger = get_logger(__name__)

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


class UserRole(str, Enum):
    """User roles for access control."""

    ADMIN = "admin"
    VALIDATOR = "validator"
    VIEWER = "viewer"


def generate_api_key() -> str:
    """Generate a secure random API key."""
    return secrets.token_urlsafe(32)


def hash_api_key(api_key: str) -> str:
    """Hash an API key for secure storage."""
    return hashlib.sha256(api_key.encode()).hexdigest()


async def get_api_key_from_db(
    api_key: str,
    backend_service,
) -> Optional["ApiKey"]:
    """
    Look up an API key in the database.
    """
    if not backend_service.async_session:
        return None

    # Hash the provided key and look it up
    key_hash = hash_api_key(api_key)

    async with backend_service.async_session() as session:
        result = await session.execute(
            select(ApiKey).where(ApiKey.key_hash == key_hash, ApiKey.is_active)
        )
        api_key_obj = result.scalar_one_or_none()

        if api_key_obj:
            # Check expiration
            if api_key_obj.expires_at and api_key_obj.expires_at < datetime.now(
                timezone.utc
            ):
                return None

            # Update last used timestamp
            api_key_obj.last_used_at = datetime.now(timezone.utc)
            await session.commit()
            await session.refresh(api_key_obj)

        return api_key_obj


def create_auth_dependency(backend_service):
    """
    Create an authentication dependency with access to the backend service.
    """

    async def get_current_user(
        api_key: Optional[str] = Security(api_key_header),
    ) -> Optional["ApiKey"]:
        """
        Validate API key and return the associated user.
        Returns None if no API key is provided (for public endpoints).
        """
        if not api_key:
            return None

        api_key_obj = await get_api_key_from_db(api_key, backend_service)

        if not api_key_obj:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid, expired, or inactive API key",
            )

        return api_key_obj

    return get_current_user


def create_role_dependencies(get_current_user):
    """
    Create role-based authentication dependencies.
    """

    async def require_admin(
        current_user: Optional["ApiKey"] = Depends(get_current_user),
    ) -> "ApiKey":
        """Require an authenticated admin user."""
        if not current_user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication required",
            )

        if current_user.role != UserRole.ADMIN:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required"
            )

        return current_user

    async def require_validator(
        current_user: Optional["ApiKey"] = Depends(get_current_user),
    ) -> "ApiKey":
        """Require an authenticated validator or admin user."""
        if not current_user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication required",
            )

        if current_user.role not in [UserRole.ADMIN, UserRole.VALIDATOR]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Validator or admin access required",
            )

        return current_user

    async def require_auth(
        current_user: Optional["ApiKey"] = Depends(get_current_user),
    ) -> "ApiKey":
        """Require any authenticated user."""
        if not current_user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication required",
            )

        return current_user

    return require_admin, require_validator, require_auth
