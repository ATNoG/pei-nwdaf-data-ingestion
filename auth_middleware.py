import logging
import os

import jwt
from jwt import PyJWKClient
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)

REQUIRED_ROLES = {"network_engineer", "debug_admin"}

SKIP_PATHS = {"/nef/notify"}

_keycloak_url = os.getenv("KEYCLOAK_URL", "http://keycloak:8080/auth")
_keycloak_realm = os.getenv("KEYCLOAK_REALM", "aion")
_jwks_url = f"{_keycloak_url}/realms/{_keycloak_realm}/protocol/openid-connect/certs"
_jwks_client = PyJWKClient(_jwks_url, cache_keys=True)


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if os.getenv("DEV_MODE", "").lower() == "true":
            return await call_next(request)

        if request.url.path in SKIP_PATHS:
            return await call_next(request)

        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return JSONResponse({"detail": "Missing authorization token"}, status_code=401)

        token = auth_header[7:]
        try:
            signing_key = _jwks_client.get_signing_key_from_jwt(token)
            payload = jwt.decode(
                token,
                signing_key.key,
                algorithms=["RS256"],
                options={"verify_aud": False},
            )
        except jwt.ExpiredSignatureError:
            return JSONResponse({"detail": "Token expired"}, status_code=401)
        except Exception:
            logger.warning("JWT validation failed", exc_info=True)
            return JSONResponse({"detail": "Invalid token"}, status_code=401)

        roles = set(payload.get("realm_access", {}).get("roles", []))
        if not roles & REQUIRED_ROLES:
            return JSONResponse({"detail": "Insufficient permissions"}, status_code=403)

        request.state.user = {
            "username": payload.get("preferred_username"),
            "name": payload.get("name"),
            "roles": list(roles),
        }

        return await call_next(request)
