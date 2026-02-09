from fastapi import APIRouter, UploadFile, File, Query, HTTPException
from fastapi.responses import FileResponse
import tempfile
import json
from datetime import datetime
import uuid

from interfaces import IDataCollector
from models import Document, ApiCollectRequest, WebCollectRequest, FileCollectRequest
from services.api_collector import ApiCollector
from services.web_collector import WebCollector
from services.file_collector import FileCollector
from utils import get_full_error

class DataCollectorService:
    def __init__(self):
        self.router = APIRouter()
        self._setup_routes()
        
        self.api_collector = ApiCollector()
        self.web_collector = WebCollector()
        self.file_collector = FileCollector()

    def _setup_routes(self):
        self.router.add_api_route(
            "/collect/api",
            self.collect_api,
            methods=["POST"],
            tags=["Сбор данных"],
            response_description="JSONL файл с документами"
        )
        
        self.router.add_api_route(
            "/collect/web",
            self.collect_web,
            methods=["POST"],
            tags=["Сбор данных"],
            response_description="JSONL файл с текстом веб-страницы"
        )
        
        self.router.add_api_route(
            "/collect/file",
            self.collect_file,
            methods=["POST"],
            tags=["Сбор данных"],
            response_description="JSONL файл с содержимым файла"
        )

    async def collect_api(
        self,
        url: str = Query(..., description="URL внешнего API", example="https://jsonplaceholder.typicode.com/posts"),
        limit: int = Query(50, description="Максимальное количество документов", ge=1)
    ):
        try:
            documents = self.api_collector.collect(url=url, limit=limit)
            return self._create_jsonl_response(documents, "api")
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Ошибка обработки: {str(e)}")

    async def collect_web(
        self,
        url: str = Query(..., description="URL веб-страницы", example="https://example.com"),
        max_pages: int = Query(5, description="Максимальное количество страниц для обработки", ge=1, le=2000)
    ):
        try:
            documents = self.web_collector.collect(url=url, max_pages=max_pages)
            return self._create_jsonl_response(documents, "web")
        except Exception as e:
            raise HTTPException(status_code=500, detail=get_full_error())

    async def collect_file(
        self,
        file: UploadFile = File(..., description="Загрузите файл для обработки", example="test.txt или data.json"),
        max_documents: int = Query(100, description="Максимальное количество документов из файла", ge=1, le=1000)
    ):
        try:
            content_bytes = await file.read()
            content = content_bytes.decode('utf-8', errors='ignore')
            
            documents = self.file_collector.process(
                content=content,
                filename=file.filename,
                max_documents=max_documents
            )
            return self._create_jsonl_response(documents, "file")
        except Exception as e:
            raise HTTPException(status_code=500, detail=get_full_error())

    def _create_jsonl_response(self, documents: list, source: str):
        temp_file = tempfile.NamedTemporaryFile(
            mode='w', 
            encoding='utf-8',
            suffix='.jsonl', 
            delete=False
        )
        
        for doc in documents:
            temp_file.write(json.dumps(doc, ensure_ascii=False) + '\n')
        
        temp_file.close()
        
        filename = f"corpus_{source}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        return FileResponse(
            path=temp_file.name,
            filename=filename,
            media_type='application/json',
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )