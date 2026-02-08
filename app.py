from fastapi import FastAPI, HTTPException, UploadFile, File, Query
from fastapi.responses import FileResponse
import requests
from bs4 import BeautifulSoup
import json
import tempfile
from datetime import datetime
import uuid
from typing import Optional
import traceback

def get_full_error():
    return traceback.format_exc()

app = FastAPI(
    title="API",
    version="1.0.0",
    docs_url="/docs"
)

@app.post("/collect/api", tags=["Сбор данных"], response_description="JSONL файл с документами")
async def collect_api(
    url: str = Query(
        ...,
        description="URL внешнего API",
        example="https://jsonplaceholder.typicode.com/posts"
    ),
    limit: Optional[int] = Query(
        50,
        description="Максимальное количество документов",
        ge=1
    )
):
    """
    Сбор данных из внешнего API
    
    - Отправляет GET запрос к указанному URL
    - Пытается распарсить ответ как JSON
    - Создает документы из полученных данных
    - Возвращает файл в формате JSONL
    
    **Примеры использования:**
    - Тестовые данные: `https://jsonplaceholder.typicode.com/posts`
    """
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        documents = []
        
        try:
            data = response.json()
            
            if isinstance(data, list):
                for i, item in enumerate(data[:limit]):
                    if isinstance(item, dict):
                        doc = {
                            "id": str(uuid.uuid4()),
                            "source": "api",
                            "url": url,
                            "title": item.get("title", f"Документ {i+1}"),
                            "text": json.dumps(item, ensure_ascii=False, indent=2),
                            "lang": item.get("lang", "ru"),
                            "date": datetime.now().isoformat()
                        }
                        documents.append(doc)
            
            elif isinstance(data, dict):
                if "items" in data or "results" in data or "articles" in data:
                    items = data.get("items") or data.get("results") or data.get("articles") or []
                    for i, item in enumerate(items[:limit]):
                        if isinstance(item, dict):
                            doc = {
                                "id": str(uuid.uuid4()),
                                "source": "api",
                                "url": url,
                                "title": item.get("title", item.get("name", f"Документ {i+1}")),
                                "text": json.dumps(item, ensure_ascii=False, indent=2),
                                "lang": item.get("language", item.get("lang", "ru")),
                                "date": datetime.now().isoformat()
                            }
                            documents.append(doc)
                else:
                    doc = {
                        "id": str(uuid.uuid4()),
                        "source": "api",
                        "url": url,
                        "title": data.get("title", "API Response"),
                        "text": json.dumps(data, ensure_ascii=False, indent=2),
                        "lang": data.get("lang", "ru"),
                        "date": datetime.now().isoformat()
                    }
                    documents.append(doc)
            else:
                doc = {
                    "id": str(uuid.uuid4()),
                    "source": "api",
                    "url": url,
                    "title": "API Response",
                    "text": str(data),
                    "lang": "ru",
                    "date": datetime.now().isoformat()
                }
                documents.append(doc)
                
        except json.JSONDecodeError:
            doc = {
                "id": str(uuid.uuid4()),
                "source": "api",
                "url": url,
                "title": "API Response",
                "text": response.text,
                "lang": "ru",
                "date": datetime.now().isoformat()
            }
            documents.append(doc)
        
        if not documents:
            raise HTTPException(status_code=400, detail="Не удалось извлечь данные из API ответа")
        
        temp_file = tempfile.NamedTemporaryFile(
            mode='w', 
            encoding='utf-8',
            suffix='.jsonl', 
            delete=False
        )
        
        for doc in documents:
            temp_file.write(json.dumps(doc, ensure_ascii=False) + '\n')
        
        temp_file.close()
        
        return FileResponse(
            path=temp_file.name,
            filename=f"corpus_api_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl",
            media_type='application/json',
            headers={
                "Content-Disposition": f"attachment; filename=corpus_api_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
            }
        )
        
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=400, detail=f"Ошибка запроса к API: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка обработки: {str(e)}")

@app.post("/collect/web", tags=["Сбор данных"], response_description="JSONL файл с текстом веб-страницы")
async def collect_web(
    url: str = Query(
        ...,
        description="URL веб-страницы",
        example="https://example.com"
    )
):
    """
    Сбор текста с веб-страницы
    
    - Загружает HTML страницу
    - Извлекает заголовок и основной текст
    - Очищает от HTML тегов и скриптов
    - Возвращает файл в формате JSONL
    """
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        documents = []
        urls_to_process = [url]
        processed_urls = set()
        
        for i in range(1):
            if not urls_to_process:
                break
                
            current_url = urls_to_process.pop(0)
            
            if current_url in processed_urls:
                continue
                
            processed_urls.add(current_url)
            
            try:
                response = requests.get(current_url, headers=headers, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside', 'form']):
                    tag.decompose()
                
                title = soup.title.string if soup.title else ""
                
                text_content = ""
                
                content_selectors = [
                    'article',
                    'main',
                    '.content',
                    '.article',
                    '.post',
                    '.text',
                    '#content',
                    '[role="main"]'
                ]
                
                for selector in content_selectors:
                    element = soup.select_one(selector)
                    if element:
                        text_content = element.get_text(separator=' ', strip=True)
                        break
                
                if not text_content:
                    body = soup.body
                    if body:
                        text_content = body.get_text(separator=' ', strip=True)
                
                text_content = ' '.join(text_content.split())
                
                if text_content:
                    lang = "ru" if any(cyr in text_content.lower() for cyr in ['а', 'б', 'в', 'г', 'д', 'е', 'ё', 'ж', 'з', 'и', 'й', 'к', 'л', 'м', 'н', 'о', 'п', 'р', 'с', 'т', 'у', 'ф', 'х', 'ц', 'ч', 'ш', 'щ', 'ъ', 'ы', 'ь', 'э', 'ю', 'я']) else "en"
                    
                    doc = {
                        "id": str(uuid.uuid4()),
                        "source": "web",
                        "url": current_url,
                        "title": title if title else "Web Page",
                        "text": text_content,
                        "lang": lang,
                        "date": datetime.now().isoformat()
                    }
                    documents.append(doc)
                            
            except Exception as e:
                print(f"Ошибка при обработке {current_url}: {e}")
                continue
        
        if not documents:
            raise HTTPException(status_code=400, detail="Не удалось извлечь текст со страницы")
        
        temp_file = tempfile.NamedTemporaryFile(
            mode='w', 
            encoding='utf-8',
            suffix='.jsonl', 
            delete=False
        )
        
        for doc in documents:
            temp_file.write(json.dumps(doc, ensure_ascii=False) + '\n')
        
        temp_file.close()
        
        return FileResponse(
            path=temp_file.name,
            filename=f"corpus_web_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl",
            media_type='application/json',
            headers={
                "Content-Disposition": f"attachment; filename=corpus_web_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
            }
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=get_full_error())

@app.post("/collect/file", tags=["Сбор данных"], response_description="JSONL файл с содержимым файла")
async def collect_file(
    file: UploadFile = File(
        ...,
        description="Загрузите файл для обработки",
        example="test.txt или data.json"
    ),
    max_documents: Optional[int] = Query(
        100,
        description="Максимальное количество документов из файла",
        ge=1,
        le=1000
    )
):
    """
    Сбор данных из файла
    
    **Поддерживаемые форматы:**
    - **.txt** - текстовый файл (создается один документ)
    - **.json** - JSON файл (массив объектов или одиночный объект)
    - **.jsonl** - JSON Lines (каждая строка - отдельный JSON объект)
    - **.csv** - CSV файл (конвертируется в JSON)
    - Любой другой - обрабатывается как текст
    """
    
    try:
        content_bytes = await file.read()
        content = content_bytes.decode('utf-8', errors='ignore')
        
        documents = []
        filename = file.filename.lower()
        
        if filename.endswith('.txt'):
            doc = {
                "id": str(uuid.uuid4()),
                "source": "file",
                "url": file.filename,
                "title": file.filename,
                "text": content,
                "lang": "ru",
                "date": datetime.now().isoformat()
            }
            documents.append(doc)
        
        elif filename.endswith('.json'):
            try:
                data = json.loads(content)
                
                if isinstance(data, list):
                    for i, item in enumerate(data[:max_documents]):
                        if isinstance(item, dict):
                            doc = {
                                "id": str(uuid.uuid4()),
                                "source": "file",
                                "url": file.filename,
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
                        "url": file.filename,
                        "title": data.get("title", "JSON Document"),
                        "text": json.dumps(data, ensure_ascii=False, indent=2),
                        "lang": data.get("lang", "ru"),
                        "date": datetime.now().isoformat()
                    }
                    documents.append(doc)
                    
            except json.JSONDecodeError as e:
                raise HTTPException(status_code=400, detail=f"Неверный формат JSON: {str(e)}")
        
        elif filename.endswith('.jsonl'):
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
                            "url": file.filename,
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
                        "url": file.filename,
                        "title": f"Текстовая строка {i+1}",
                        "text": line.strip(),
                        "lang": "ru",
                        "date": datetime.now().isoformat()
                    }
                    documents.append(doc)
        
        elif filename.endswith('.csv'):
            import csv
            import io
            
            csv_file = io.StringIO(content)
            csv_reader = csv.DictReader(csv_file)
            
            for i, row in enumerate(csv_reader):
                if i >= max_documents:
                    break
                    
                doc = {
                    "id": str(uuid.uuid4()),
                    "source": "file",
                    "url": file.filename,
                    "title": f"CSV строка {i+1}",
                    "text": json.dumps(row, ensure_ascii=False, indent=2),
                    "lang": "ru",
                    "date": datetime.now().isoformat()
                }
                documents.append(doc)
        
        else:
            doc = {
                "id": str(uuid.uuid4()),
                "source": "file",
                "url": file.filename,
                "title": file.filename,
                "text": content,
                "lang": "ru",
                "date": datetime.now().isoformat()
            }
            documents.append(doc)
        
        temp_file = tempfile.NamedTemporaryFile(
            mode='w', 
            encoding='utf-8',
            suffix='.jsonl', 
            delete=False
        )
        
        for doc in documents:
            temp_file.write(json.dumps(doc, ensure_ascii=False) + '\n')
        
        temp_file.close()
        
        return FileResponse(
            path=temp_file.name,
            filename=f"corpus_file_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl",
            media_type='application/json',
            headers={
                "Content-Disposition": f"attachment; filename=corpus_file_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
            }
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=get_full_error())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        reload=True
    )