import json
import csv
import io
from datetime import datetime
import uuid
from typing import List, Dict, Any
from fastapi import HTTPException

from interfaces import IFileProcessor

class FileProcessor(IFileProcessor):
    def process(self, content: str, filename: str, **kwargs) -> List[Dict[str, Any]]:
        max_documents = kwargs.get('max_documents', 100)
        filename_lower = filename.lower()
        
        if filename_lower.endswith('.txt'):
            return self._process_txt(content, filename)
        elif filename_lower.endswith('.json'):
            return self._process_json(content, filename, max_documents)
        elif filename_lower.endswith('.jsonl'):
            return self._process_jsonl(content, filename, max_documents)
        elif filename_lower.endswith('.csv'):
            return self._process_csv(content, filename, max_documents)
        else:
            return self._process_generic(content, filename)

    def _process_txt(self, content: str, filename: str) -> List[Dict[str, Any]]:
        doc = {
            "id": str(uuid.uuid4()),
            "source": "file",
            "url": filename,
            "title": filename,
            "text": content,
            "lang": "ru",
            "date": datetime.now().isoformat()
        }
        return [doc]

    def _process_json(self, content: str, filename: str, max_documents: int) -> List[Dict[str, Any]]:
        try:
            data = json.loads(content)
            documents = []
            
            if isinstance(data, list):
                for i, item in enumerate(data[:max_documents]):
                    if isinstance(item, dict):
                        doc = {
                            "id": str(uuid.uuid4()),
                            "source": "file",
                            "url": filename,
                            "title": item.get("title", f"Документ {i+1}"),
                            "text": json.dumps(item, ensure_ascii=False, indent=2),
                            "lang": item.get("lang", "ru"),
                            "date": datetime.now().isoformat()
                        }
                        documents.append(doc)
            
            elif isinstance(data, dict):
                doc = {
                    "id": str(uuid.uuid4()),
                    "source": "file",
                    "url": filename,
                    "title": data.get("title", "JSON Document"),
                    "text": json.dumps(data, ensure_ascii=False, indent=2),
                    "lang": data.get("lang", "ru"),
                    "date": datetime.now().isoformat()
                }
                documents.append(doc)
            
            return documents
                
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"Неверный формат JSON: {str(e)}")

    def _process_jsonl(self, content: str, filename: str, max_documents: int) -> List[Dict[str, Any]]:
        documents = []
        lines = content.strip().split('\n')
        
        for i, line in enumerate(lines[:max_documents]):
            if not line.strip():
                continue
                
            try:
                item = json.loads(line.strip())
                
                if isinstance(item, dict):
                    doc = {
                        "id": str(uuid.uuid4()),
                        "source": "file",
                        "url": filename,
                        "title": item.get("title", f"Документ {i+1}"),
                        "text": json.dumps(item, ensure_ascii=False, indent=2),
                        "lang": item.get("lang", "ru"),
                        "date": datetime.now().isoformat()
                    }
                    documents.append(doc)
                    
            except json.JSONDecodeError:
                doc = {
                    "id": str(uuid.uuid4()),
                    "source": "file",
                    "url": filename,
                    "title": f"Текстовая строка {i+1}",
                    "text": line.strip(),
                    "lang": "ru",
                    "date": datetime.now().isoformat()
                }
                documents.append(doc)
        
        return documents

    def _process_csv(self, content: str, filename: str, max_documents: int) -> List[Dict[str, Any]]:
        documents = []
        csv_file = io.StringIO(content)
        csv_reader = csv.DictReader(csv_file)
        
        for i, row in enumerate(csv_reader):
            if i >= max_documents:
                break
                
            doc = {
                "id": str(uuid.uuid4()),
                "source": "file",
                "url": filename,
                "title": f"CSV строка {i+1}",
                "text": json.dumps(row, ensure_ascii=False, indent=2),
                "lang": "ru",
                "date": datetime.now().isoformat()
            }
            documents.append(doc)
        
        return documents

    def _process_generic(self, content: str, filename: str) -> List[Dict[str, Any]]:
        doc = {
            "id": str(uuid.uuid4()),
            "source": "file",
            "url": filename,
            "title": filename,
            "text": content,
            "lang": "ru",
            "date": datetime.now().isoformat()
        }
        return [doc]

class FileCollector:
    def __init__(self):
        self.processor = FileProcessor()
    
    def process(self, content: str, filename: str, **kwargs) -> List[Dict[str, Any]]:
        return self.processor.process(content, filename, **kwargs)