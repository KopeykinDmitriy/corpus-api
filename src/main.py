from fastapi import FastAPI
from services.data_collector import DataCollectorService

app = FastAPI(
    title="API",
    version="1.0.0",
    docs_url="/docs"
)

data_collector_service = DataCollectorService()

app.include_router(data_collector_service.router)