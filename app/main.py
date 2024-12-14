from fastapi import FastAPI
from controllers.train_controller import router as train_router

app = FastAPI(title="Digitraffic Kafka Pipeline")

app.include_router(train_router, prefix="/train", tags=["Train Data"])
