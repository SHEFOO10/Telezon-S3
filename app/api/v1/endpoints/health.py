from fastapi import APIRouter, Depends
from motor.motor_asyncio import AsyncIOMotorClient

from app.core.config import DATABASE_NAME, MULTIPART_MODE, PROJECT_NAME
from app.db.mongodb import get_database

router = APIRouter(tags=["Health"])


@router.get("/health")
async def health_check(db: AsyncIOMotorClient = Depends(get_database)):
    mongo_ok = False
    try:
        if db:
            await db[DATABASE_NAME].command("ping")
            mongo_ok = True
    except Exception:
        mongo_ok = False

    return {
        "status": "healthy" if mongo_ok else "degraded",
        "project": PROJECT_NAME,
        "database": {
            "name": DATABASE_NAME,
            "connected": mongo_ok,
        },
        "multipart_mode": MULTIPART_MODE,
    }
