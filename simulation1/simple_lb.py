# simple_lb.py - Simple Round-Robin Load Balancer using FastAPI
# For testing - use Nginx in production

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, Response
import httpx
import random
from typing import List

app = FastAPI(title="Simple Load Balancer")

# Backend servers
BACKENDS = [
    "http://localhost:8001",
    "http://localhost:8002", 
    "http://localhost:8003",
]

current_index = 0


def get_next_backend() -> str:
    """Round-robin selection"""
    global current_index
    backend = BACKENDS[current_index % len(BACKENDS)]
    current_index += 1
    return backend


@app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy(full_path: str, request: Request):
    """
    Forward requests to backend servers
    """
    # Select backend using round-robin
    backend = get_next_backend()
    url = f"{backend}/{full_path}"
    
    # Get query params
    query_params = dict(request.query_params)
    
    # Get request body
    body = await request.body()
    
    # Forward headers (except Host)
    headers = dict(request.headers)
    headers.pop("host", None)
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.request(
                method=request.method,
                url=url,
                params=query_params,
                headers=headers,
                content=body,
            )
        
        # Return response from backend
        return Response(
            content=response.content,
            status_code=response.status_code,
            headers=dict(response.headers),
        )
        
    except httpx.ConnectError:
        raise HTTPException(status_code=503, detail=f"Backend {backend} unreachable")
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Backend timeout")


@app.get("/lb/health")
async def lb_health():
    """Load balancer health check"""
    # Check if backends are healthy
    healthy_backends = []
    
    async with httpx.AsyncClient(timeout=5.0) as client:
        for backend in BACKENDS:
            try:
                resp = await client.get(f"{backend}/health")
                if resp.status_code == 200:
                    healthy_backends.append(backend)
            except:
                pass
    
    return {
        "status": "ok" if healthy_backends else "degraded",
        "backends_total": len(BACKENDS),
        "backends_healthy": len(healthy_backends),
        "healthy_urls": healthy_backends,
    }


# Run with: uvicorn simple_lb:app --port 8000

