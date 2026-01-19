from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.api.routes import weather, risk

app = FastAPI(
    title="Debris Flow Digital Twin API",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(weather.router)
app.include_router(risk.router)


@app.get("/")
async def root():
    return {
        "message": "Debris Flow Digital Twin API",
        "version": "1.0.0",
        "endpoints": {
            "weather": "/weather",
            "risk": "/risk"
        }
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}