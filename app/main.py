from fastapi import FastAPI
from controllers.train_controller import router as train_router
from controllers.health_controler import router as health_router

app = FastAPI(title="Digitraffic Kafka Pipeline")

app.include_router(train_router, prefix="/train", tags=["Train Data"])
app.include_router(health_router, tags=["Health Check"])
