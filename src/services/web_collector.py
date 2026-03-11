import time
import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup
from datetime import datetime
import uuid
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse
from fastapi import HTTPException

from interfaces import IDataCollector

class WebCollector(IDataCollector):
    def __init__(self, rate_delay: float = 0.5, max_retries: int = 3):
        self.rate_delay = rate_delay
        self.session = requests.Session()
        retries = Retry(total=max_retries, backoff_factor=0.5,
                        status_forcelist=(429, 500, 502, 503, 504))
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; WebCollector/1.0; +https://example.com/bot)"
        })

    # --- вспомогательные для Википедии ---
    def _is_wikipedia(self, parsed_netloc: str) -> bool:
        return parsed_netloc.endswith("wikipedia.org")

    def _is_valid_wiki_link(self, href: str) -> bool:
        if not href or not href.startswith("/wiki/"):
            return False
        invalid_prefixes = ("/wiki/Special:", "/wiki/Help:", "/wiki/Talk:", "/wiki/File:",
                            "/wiki/Category:", "/wiki/Portal:", "/wiki/Template:", "/wiki/Template_talk:",
                            "/wiki/Файл:", "/wiki/Обсуждение:")
        if any(href.startswith(p) for p in invalid_prefixes):
            return False
        if ':' in href.split('/wiki/')[-1]:
            return False
        return True

    def _extract_last_modified(self, soup: BeautifulSoup) -> Optional[str]:
        meta = soup.find("meta", {"property": "article:modified_time"}) or soup.find("meta", {"name": "last-modified"})
        if meta and meta.get("content"):
            return meta["content"]
        footer = soup.find(id="footer-info-lastmod")
        if footer:
            return footer.get_text(strip=True)
        return None

    def _get_main_text_wiki(self, soup: BeautifulSoup) -> str:
        content = soup.find(id="mw-content-text")
        if not content:
            content = soup.find('article') or soup.find('body')
        for selector in ["table", "sup.reference", "div.reflist", "ol.references", "script", "style", "aside", "nav"]:
            for el in content.select(selector):
                el.decompose()
        paragraphs = []
        for p in content.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'li']):
            text = p.get_text(separator=' ', strip=True)
            if text:
                paragraphs.append(text)
        return ' '.join(paragraphs)

    def _split_blocks(self, text: str, max_chars: int = 3000, overlap: int = 300) -> List[str]:
        if not text:
            return []
        parts = []
        tokens = text.split('. ')
        cur = ""
        for tok in tokens:
            piece = (tok + '. ') if not tok.endswith('. ') else tok
            if len(cur) + len(piece) <= max_chars:
                cur += piece
            else:
                if cur:
                    parts.append(cur.strip())
                cur = piece
        if cur:
            parts.append(cur.strip())
        if overlap and len(parts) > 1:
            overlapped = []
            for i, p in enumerate(parts):
                if i == 0:
                    overlapped.append(p)
                else:
                    prev = overlapped[-1]
                    overlap_chunk = prev[-overlap:] if len(prev) > overlap else prev
                    merged = overlap_chunk + " " + p
                    overlapped.append(merged.strip())
            return overlapped
        return parts

    def collect(self, **kwargs) -> List[Dict[str, Any]]:
        url = kwargs.get('url')
        if not url:
            raise HTTPException(status_code=400, detail="url is required")
        max_pages = int(kwargs.get('max_pages', 5))

        parsed_start = urlparse(url)
        base_domain = parsed_start.netloc
        base_scheme = parsed_start.scheme or "https"
        base_origin = f"{base_scheme}://{base_domain}"
        headers = {'User-Agent': self.session.headers.get("User-Agent", "Mozilla/5.0")}

        documents = []
        urls_to_process = [url]
        processed_urls = set()
        processed_count = 0

        is_wiki = self._is_wikipedia(base_domain)

        while urls_to_process and processed_count < max_pages:
            current_url = urls_to_process.pop(0)

            if current_url in processed_urls:
                continue

            processed_urls.add(current_url)
            processed_count += 1

            try:
                # use session for retries for wiki and non-wiki
                resp = self.session.get(current_url, timeout=10)
                resp.raise_for_status()
                time.sleep(self.rate_delay)

                soup = BeautifulSoup(resp.text, 'html.parser')

                for tag in soup(['script', 'style']):
                    tag.decompose()

                if is_wiki:
                    # Википедия-ветка: лучшее извлечение и фильтрация ссылок
                    title_tag = soup.find(id="firstHeading") or soup.title
                    title = title_tag.get_text(strip=True) if title_tag else current_url

                    text_content = self._get_main_text_wiki(soup)
                    last_mod = self._extract_last_modified(soup)

                    text_content = ' '.join(text_content.split())

                    if text_content:
                        blocks = self._split_blocks(text_content, max_chars=3000, overlap=300)
                        for i, block in enumerate(blocks):
                            doc = {
                                "id": str(uuid.uuid4()),
                                "source": "wikipedia",
                                "url": current_url,
                                "title": title,
                                "text": block,
                                "date": datetime.now().isoformat(),
                            }
                            documents.append(doc)

                    # enqueue internal wiki links
                    for a in soup.find_all('a', href=True):
                        href = a['href']
                        if href.startswith('#'):
                            continue
                        if self._is_valid_wiki_link(href):
                            full_url = urljoin(base_origin, href)
                            parsed_full = urlparse(full_url)
                            if parsed_full.netloc == base_domain and full_url not in processed_urls:
                                if full_url not in urls_to_process:
                                    urls_to_process.append(full_url)

                else:
                    # Ваша оригинальная ветка (минимально изменённая)
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
                                    full_url = base_origin + href
                                elif href.startswith('./'):
                                    full_url = base_origin + href[1:]
                                elif not href.startswith(('http://', 'https://')):
                                    full_url = base_origin + '/' + href.lstrip('/')
                                else:
                                    full_url = href

                                if full_url.startswith(base_origin) and full_url not in processed_urls:
                                    if full_url not in urls_to_process:
                                        urls_to_process.append(full_url)

            except Exception:
                continue

        if not documents:
            raise HTTPException(status_code=400, detail="Не удалось извлечь текст ни с одной страницы")

        return documents
