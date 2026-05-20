"""
order-api · 한밭푸드 주문 조회 서비스 (시즌 2)

주요 흐름:
  GET /api/orders/{order_id}
    → payment-api 에 동기 호출 (httpx AsyncClient)
    → 주문 + 결제 정보를 합쳐서 응답

Phase 1 상태: 타임아웃 설정 없음 (의도적 — 장애 전파 체험용)
Phase 2에서 학생이 Retry + Circuit Breaker 를 추가할 예정
"""

import os
import time

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware


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


@app.get("/api/orders/{order_id}")
async def get_order(order_id: str):
    order = ORDERS.get(order_id)
    if not order:
        raise HTTPException(
            status_code=404,
            detail={"error": "ORDER_NOT_FOUND", "order_id": order_id},
        )

    start = time.monotonic()

    # ⚠️  Phase 1: timeout=None (타임아웃 없음) — 이것이 연쇄 장애의 원인!
    #
    # 💡 Phase 2 TODO:
    #    [STEP 1] timeout 추가:
    #      async with httpx.AsyncClient(timeout=2.0) as client:
    #
    #    [STEP 2] tenacity 로 Retry 래핑
    #
    #    [STEP 3] circuitbreaker 로 Circuit Breaker 래핑
    async with httpx.AsyncClient(timeout=None) as client:  # ← Phase 2에서 수정
        try:
            resp = await client.get(f"{PAYMENT_API_URL}/api/payments/{order_id}")
            resp.raise_for_status()
            payment = resp.json()
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
