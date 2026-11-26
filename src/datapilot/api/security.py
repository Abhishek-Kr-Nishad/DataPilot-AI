"""API key authentication dependencies for DataPilot AI control-plane endpoints."""

from __future__ import annotations

import hmac

from fastapi import HTTPException, Request, status

from datapilot.dependencies.container import get_container


def _extract_presented_key(request: Request) -> str | None:
    header_key = request.headers.get("X-API-Key")
    if header_key and header_key.strip():
        return header_key.strip()
    auth_header = request.headers.get("Authorization")
    if auth_header:
        parts = auth_header.strip().split(None, 1)
        if len(parts) == 2 and parts[0].lower() == "bearer" and parts[1].strip():
            return parts[1].strip()
    return None


def require_api_auth(request: Request) -> str:
    """Enforce X-API-Key or Bearer authentication when enabled in settings."""
    container = get_container(request)
    settings = container.settings
    if not settings.auth_enabled:
        return "local-dev-unauthenticated"

    expected = (
        settings.api_key.get_secret_value().strip()
        if settings.api_key is not None
        else ""
    )
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "API authentication is required (DATAPILOT_REQUIRE_AUTH=true) "
                "but DATAPILOT_API_KEY is not configured."
            ),
        )

    presented = _extract_presented_key(request)
    if not presented or not hmac.compare_digest(presented, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key. Provide X-API-Key or Authorization: Bearer token.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return "api-key"
