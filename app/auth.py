from fastapi import Security, HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from app.config import settings

security = HTTPBearer()


async def verify_api_key(credentials: HTTPAuthorizationCredentials = Security(security)):
    """
    Accept either:
    1. Static API key (for Android app / public endpoints)
    2. JWT token (for dashboard users)
    Both are sent as Bearer tokens.
    """
    token = credentials.credentials

    # Check static API key first
    if token == settings.API_KEY:
        return token

    # Try JWT validation
    try:
        from app.api.admin_users import decode_access_token
        payload = decode_access_token(token)
        return token
    except Exception:
        pass

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing API key / token"
    )