"""Tests for API endpoints."""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_health_check(client: AsyncClient):
    """Test health check endpoint."""
    response = await client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "version" in data


@pytest.mark.asyncio
async def test_get_trending_movies_no_data(client: AsyncClient):
    """Test trending movies endpoint with no data."""
    response = await client.get("/v1/trends/movies")
    # Expect 404 when no data
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_movie_trend_not_found(client: AsyncClient):
    """Test movie trend endpoint with non-existent movie."""
    response = await client.get("/v1/trends/movies/99999")
    assert response.status_code == 404
    data = response.json()
    assert "detail" in data


@pytest.mark.asyncio
async def test_compare_trends_invalid_ids(client: AsyncClient):
    """Test compare endpoint with invalid IDs."""
    response = await client.get("/v1/trends/compare?ids=abc,def")
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_compare_trends_too_many_ids(client: AsyncClient):
    """Test compare endpoint with too many IDs."""
    ids = ",".join(str(i) for i in range(1, 12))
    response = await client.get(f"/v1/trends/compare?ids={ids}")
    assert response.status_code == 400
