import requests
from bs4 import BeautifulSoup
from datetime import datetime
import uuid
from typing import List, Dict, Any
from fastapi import HTTPException

from interfaces import IDataCollector

class WebCollector(IDataCollector):
    def collect(self, **kwargs) -> List[Dict[str, Any]]:
        url = kwargs.get('url')
        max_pages = kwargs.get('max_pages', 5)
        
        headers = {'User-Agent': 'Mozilla/5.0'}
        base_url = url.rstrip('/')
        
        documents = []
        urls_to_process = [url]
        processed_urls = set()
        processed_count = 0
        
        while urls_to_process and processed_count < max_pages:
            current_url = urls_to_process.pop(0)
            
            if current_url in processed_urls:
                continue
                
            processed_urls.add(current_url)
            processed_count += 1
            
            try:
                response = requests.get(current_url, headers=headers, timeout=10)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                for tag in soup(['script', 'style']):
                    tag.decompose()
                
                title = soup.title.string if soup.title else current_url
                
                text_content = ""
                main_content = soup.find('article') or soup.find('main') or soup.find('div', class_='content')
                
                if main_content:
                    text_content = main_content.get_text(separator=' ', strip=True)
                else:
                    body = soup.find('body')
                    if body:
                        text_content = body.get_text(separator=' ', strip=True)
                
                text_content = ' '.join(text_content.split())
                
                if text_content:
                    doc = {
                        "id": str(uuid.uuid4()),
                        "source": "web",
                        "url": current_url,
                        "title": title,
                        "text": text_content,
                        "date": datetime.now().isoformat()
                    }
                    documents.append(doc)
                    
                    if processed_count < max_pages:
                        for link in soup.find_all('a', href=True):
                            href = link['href']
                            
                            if href.startswith('/'):
                                full_url = base_url + href
                            elif href.startswith('./'):
                                full_url = base_url + href[1:]
                            elif not href.startswith(('http://', 'https://')):
                                full_url = base_url + '/' + href.lstrip('/')
                            else:
                                full_url = href
                            
                            if full_url.startswith(base_url) and full_url not in processed_urls:
                                if full_url not in urls_to_process:
                                    urls_to_process.append(full_url)
            
            except Exception:
                continue
        
        if not documents:
            raise HTTPException(status_code=400, detail="Не удалось извлечь текст ни с одной страницы")
        
        return documents