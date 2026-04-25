"""
order-api · 한밭푸드 주문 조회 서비스 (시즌 2)

주요 흐름:
  GET /api/orders/{order_id}
    → payment-api 에 동기 호출 (httpx AsyncClient)
    → 주문 + 결제 정보를 합쳐서 응답

Phase 2 상태: Timeout(2초) + Retry(503/Timeout, 최대 3회) + Circuit Breaker(5회 실패 시 Open)
"""

import os
import time

import httpx
import pybreaker
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

app = FastAPI(title="order-api", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# 설정
# ---------------------------------------------------------------------------
PAYMENT_API_URL: str = os.getenv("PAYMENT_API_URL", "http://payment-api:8080")

# ---------------------------------------------------------------------------
# Circuit Breaker
# fail_max=5  : 연속 5회 실패 시 Open (요청 차단)
# reset_timeout=10 : 10초 후 Half-Open (복구 테스트)
# ---------------------------------------------------------------------------
payment_breaker = pybreaker.CircuitBreaker(
    fail_max=5,
    reset_timeout=10,
)

# ---------------------------------------------------------------------------
# 샘플 주문 데이터
# ---------------------------------------------------------------------------
ORDERS: dict[str, dict] = {
    "ORD-001": {"order_id": "ORD-001", "user_id": 3030, "product_name": "유기농 쌀 10kg", "amount": 32000, "status": "배송완료", "ordered_at": "2025-04-01"},
    "ORD-002": {"order_id": "ORD-002", "user_id": 3030, "product_name": "한우 등심 500g", "amount": 15500, "status": "배송중",   "ordered_at": "2025-04-02"},
    "ORD-003": {"order_id": "ORD-003", "user_id": 2020, "product_name": "제주 흑돼지 1kg",  "amount": 58000, "status": "결제완료", "ordered_at": "2025-04-03"},
    "ORD-004": {"order_id": "ORD-004", "user_id": 2020, "product_name": "국내산 달걀 30구", "amount": 4500,  "status": "배송완료", "ordered_at": "2025-04-04"},
    "ORD-005": {"order_id": "ORD-005", "user_id": 1010, "product_name": "친환경 방울토마토 2kg", "amount": 12000, "status": "배송중", "ordered_at": "2025-04-05"},
}


@app.get("/health")
async def health():
    return {"status": "ok", "service": "order-api"}


# ---------------------------------------------------------------------------
# Retry 조건 — Timeout 또는 503일 때만 재시도
# ---------------------------------------------------------------------------
def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TimeoutException):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code == 503
    return False


@retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=4),
    reraise=True,
)
async def _fetch_payment_raw(order_id: str) -> dict:
    """payment-api 단순 호출 (Retry가 감싸는 실제 함수)"""
    async with httpx.AsyncClient(timeout=2.0) as client:
        resp = await client.get(f"{PAYMENT_API_URL}/api/payments/{order_id}")
        resp.raise_for_status()
        return resp.json()


async def _fetch_payment(order_id: str) -> dict:
    """Circuit Breaker + Retry 래핑"""
    return await payment_breaker.call_async(_fetch_payment_raw, order_id)


@app.get("/api/orders/{order_id}")
async def get_order(order_id: str):
    """주문 + 결제 정보 통합 조회 (Phase 2: Timeout + Retry + Circuit Breaker 적용)"""
    order = ORDERS.get(order_id)
    if not order:
        raise HTTPException(
            status_code=404,
            detail={"error": "ORDER_NOT_FOUND", "order_id": order_id},
        )

    start = time.monotonic()

    try:
        payment = await _fetch_payment(order_id)
    except pybreaker.CircuitBreakerError:
        # Circuit Breaker Open — 결제 서비스 차단 중, Fallback 응답
        payment = {
            "status": "조회불가",
            "message": "결제 서비스 일시 중단 — 잠시 후 다시 시도해주세요",
        }
    except httpx.TimeoutException:
        raise HTTPException(
            status_code=504,
            detail={"error": "PAYMENT_API_TIMEOUT", "order_id": order_id},
        )
    except httpx.HTTPStatusError as e:
        raise HTTPException(
            status_code=502,
            detail={"error": "PAYMENT_API_ERROR", "upstream_status": e.response.status_code},
        )
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=502,
            detail={"error": "PAYMENT_API_UNREACHABLE", "detail": str(e)},
        )

    elapsed_ms = int((time.monotonic() - start) * 1000)

    return {
        **order,
        "payment": payment,
        "total_response_time_ms": elapsed_ms,
    }
