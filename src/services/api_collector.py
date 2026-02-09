import requests
import json
from datetime import datetime
import uuid
from typing import List, Dict, Any
from fastapi import HTTPException

from interfaces import IDataCollector

class ApiCollector(IDataCollector):
    def collect(self, **kwargs) -> List[Dict[str, Any]]:
        url = kwargs.get('url')
        limit = kwargs.get('limit', 50)
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            documents = []
            
            try:
                data = response.json()
                documents = self._process_json_data(data, url, limit)
            except json.JSONDecodeError:
                doc = self._create_document(
                    source="api",
                    url=url,
                    title="API Response",
                    text=response.text,
                    lang="ru"
                )
                documents.append(doc)
            
            if not documents:
                raise HTTPException(status_code=400, detail="Не удалось извлечь данные из API ответа")
            
            return documents
            
        except requests.exceptions.RequestException as e:
            raise HTTPException(status_code=400, detail=f"Ошибка запроса к API: {str(e)}")

    def _process_json_data(self, data, url: str, limit: int) -> List[Dict[str, Any]]:
        documents = []
        
        if isinstance(data, list):
            for i, item in enumerate(data[:limit]):
                if isinstance(item, dict):
                    doc = self._create_document(
                        source="api",
                        url=url,
                        title=item.get("title", f"Документ {i+1}"),
                        text=json.dumps(item, ensure_ascii=False, indent=2),
                        lang=item.get("lang", "ru")
                    )
                    documents.append(doc)
        
        elif isinstance(data, dict):
            if "items" in data or "results" in data or "articles" in data:
                items = data.get("items") or data.get("results") or data.get("articles") or []
                for i, item in enumerate(items[:limit]):
                    if isinstance(item, dict):
                        doc = self._create_document(
                            source="api",
                            url=url,
                            title=item.get("title", item.get("name", f"Документ {i+1}")),
                            text=json.dumps(item, ensure_ascii=False, indent=2),
                            lang=item.get("language", item.get("lang", "ru"))
                        )
                        documents.append(doc)
            else:
                doc = self._create_document(
                    source="api",
                    url=url,
                    title=data.get("title", "API Response"),
                    text=json.dumps(data, ensure_ascii=False, indent=2),
                    lang=data.get("lang", "ru")
                )
                documents.append(doc)
        else:
            doc = self._create_document(
                source="api",
                url=url,
                title="API Response",
                text=str(data),
                lang="ru"
            )
            documents.append(doc)
        
        return documents

    def _create_document(self, source: str, url: str, title: str, text: str, lang: str = "ru") -> Dict[str, Any]:
        return {
            "id": str(uuid.uuid4()),
            "source": source,
            "url": url,
            "title": title,
            "text": text,
            "lang": lang,
            "date": datetime.now().isoformat()
        }