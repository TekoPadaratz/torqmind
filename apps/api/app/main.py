from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routes_auth import router as auth_router
from app.routes_dashboard import router as dashboard_router
from app.routes_ingest import router as ingest_router
from app.routes_etl import router as etl_router
from app.routes_bi import router as bi_router

app = FastAPI(title="TorqMind API", version="0.2.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"]
)

app.include_router(auth_router)
app.include_router(dashboard_router)
app.include_router(bi_router)
app.include_router(etl_router)
app.include_router(ingest_router)


@app.get("/")
def root():
    # PT-BR: Ajuda a evitar confusão ao abrir localhost:8000 no browser.
    # EN: Prevents confusion when opening localhost:8000 in the browser.
    return {"ok": True, "service": "torqmind-api", "docs": "/docs", "health": "/health"}


@app.get("/health")
def health():
    return {"ok": True}
