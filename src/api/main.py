from fastapi import FastAPI
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Setup Kafka consumer here later
    yield
    # Cleanup Kafka consumer here later

app = FastAPI(title="AIOps API", lifespan=lifespan)

@app.get("/health")
async def health_check():
    return {"status": "ok"}
