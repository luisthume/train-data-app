from fastapi import FastAPI
from controllers.train_controller import router as train_router
from controllers.health_controller import router as health_router

app = FastAPI(title="Digitraffic Kafka Pipeline")

app.include_router(train_router, prefix="/v1")
app.include_router(health_router, prefix="/v1")
