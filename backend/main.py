# backend/main.py
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.routers import auth, rules, notifications, fragments, crawler

app = FastAPI(
    title="DPKB API",
    description="Dynamic Payer Knowledge Base — NY Medicaid Facility Billing Rules",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router,          prefix="/api/auth",          tags=["auth"])
app.include_router(rules.router,         prefix="/api/rules",         tags=["rules"])
app.include_router(notifications.router, prefix="/api/notifications", tags=["notifications"])
app.include_router(fragments.router,     prefix="/api/fragments",     tags=["fragments"])
app.include_router(crawler.router,       prefix="/api/crawler",       tags=["crawler"])


@app.get("/api/health")
def health_check():
    return {"status": "ok"}