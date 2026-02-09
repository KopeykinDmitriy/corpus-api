from pydantic import BaseModel
from typing import Optional

class Document(BaseModel):
    id: str
    source: str
    url: str
    title: str
    text: str
    lang: Optional[str] = "ru"
    date: str

class ApiCollectRequest(BaseModel):
    url: str
    limit: Optional[int] = 50

class WebCollectRequest(BaseModel):
    url: str
    max_pages: Optional[int] = 5

class FileCollectRequest(BaseModel):
    max_documents: Optional[int] = 100