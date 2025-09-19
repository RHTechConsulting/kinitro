#!/usr/bin/env python3
"""
Setup script to create an initial admin API key.

This script should be run once after setting up the database to create
the first admin API key that can be used to manage other API keys.
"""

import asyncio
import sys
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

from backend.auth import generate_api_key, hash_api_key, UserRole
from backend.config import BackendConfig
from backend.models import ApiKey


async def create_initial_admin_key():
    """Create the initial admin API key."""
    config = BackendConfig()

    # Get database URL from config
    database_url = config.settings.get("database_url")

    if not database_url:
        print(
            "ERROR: No database URL configured. Please set --database-url or configure it in backend.toml"
        )
        return False

    # Create database engine
    engine = create_async_engine(database_url)

    try:
        async with AsyncSession(engine) as session:
            # Check if admin user already exists
            result = await session.execute(
                select(ApiKey).where(ApiKey.role == UserRole.ADMIN)
            )
            existing_admin = result.scalar_one_or_none()

            if existing_admin:
                print(
                    f"Admin API key already exists: {existing_admin.name} (ID: {existing_admin.id})"
                )
                print("Use the admin endpoints to create additional API keys.")
                return True

            # Generate new admin API key
            api_key = generate_api_key()
            key_hash = hash_api_key(api_key)

            # Create admin API key record
            admin_api_key = ApiKey(
                name="Initial Admin",
                description="Initial admin API key created during setup",
                key_hash=key_hash,
                role=UserRole.ADMIN,
                is_active=True,
            )

            session.add(admin_api_key)
            await session.commit()
            await session.refresh(admin_api_key)

            print("✅ Initial admin API key created successfully!")
            print("=" * 60)
            print(f"API Key ID: {admin_api_key.id}")
            print(f"Name: {admin_api_key.name}")
            print(f"Role: {admin_api_key.role}")
            print(f"Created: {admin_api_key.created_at}")
            print("=" * 60)
            print(f"🔑 API Key: {api_key}")
            print("=" * 60)
            print("⚠️  IMPORTANT: Save this API key securely!")
            print("   This is the only time it will be displayed.")
            print("   You will need this key to access admin endpoints.")
            print("=" * 60)

            return True

    except Exception as e:
        print(f"ERROR: Failed to create admin API key: {e}")
        return False
    finally:
        await engine.dispose()


def main():
    """Main entry point."""
    print("Kinitro Backend - Admin API Key Setup")
    print("=" * 40)

    if not asyncio.run(create_initial_admin_key()):
        sys.exit(1)

    print("\nNext steps:")
    print("1. Use the API key with the X-API-Key header")
    print("2. Access admin endpoints at /admin/api-keys")
    print("3. Create additional API keys as needed")


if __name__ == "__main__":
    main()
