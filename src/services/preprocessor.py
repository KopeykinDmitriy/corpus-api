import re
import json
from typing import List, Dict
from collections import Counter
from datetime import datetime
import uuid
from pymorphy3 import MorphAnalyzer as PMorph


class Preprocessor:
    def __init__(self):
        self.morph = PMorph()
        self.token_pattern = re.compile(r"[а-яА-ЯёЁa-zA-Z0-9]+", re.UNICODE)
        self.stopwords = set([
            'и', 'в', 'во', 'не', 'что', 'он', 'на', 'я', 'с', 'со', 'как', 'а', 'то',
            'все', 'она', 'так', 'его', 'но', 'да', 'ты', 'к', 'у', 'же', 'вы', 'за',
            'бы', 'по', 'только', 'ее', 'мне', 'было', 'вот', 'от', 'меня', 'еще', 'нет',
            'о', 'из', 'ему', 'теперь', 'когда', 'даже', 'ну', 'вдруг', 'ли', 'если', 'уже',
            'или', 'быть', 'был', 'него', 'до', 'вас', 'ни', 'при', 'свою', 'эти', 'такой'
        ])

    def clean_text(self, text: str) -> str:
        if not isinstance(text, str):
            text = json.dumps(text, ensure_ascii=False)

        text = re.sub(r"<[^>]+>", " ", text)
        text = text.replace('\xa0', ' ')
        text = text.lower()
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def tokenize(self, text: str) -> List[str]:
        return self.token_pattern.findall(text)

    def lemmatize(self, tokens: List[str]) -> List[str]:
        lemmas = []
        for t in tokens:
            try:
                parsed = self.morph.parse(t)
                if parsed:
                    p = parsed[0]
                    if hasattr(p, 'normal_form'):
                        lemmas.append(p.normal_form)
                    elif hasattr(p, 'lemma'):
                        lemmas.append(p.lemma)
                    else:
                        try:
                            l = self.morph.lemmatize(t)
                            lemmas.append(l[0] if l else t)
                        except Exception:
                            lemmas.append(t)
                else:
                    lemmas.append(t)
            except Exception:
                lemmas.append(t)
        return lemmas

    def filter_tokens(self, lemmas: List[str]) -> List[str]:
        out = []
        for l in lemmas:
            if l in self.stopwords:
                continue
            if len(l) < 2:
                continue
            if any(ch.isdigit() for ch in l):
                continue
            out.append(l)
        return out

    def process_jsonl(self, content: str, max_documents: int = 1000, top_n: int = 10) -> List[Dict]:
        documents = []
        lemma_counter = Counter()

        lines = content.strip().split('\n')
        for i, line in enumerate(lines[:max_documents]):
            if not line.strip():
                continue

            try:
                item = json.loads(line)
            except Exception:
                item = {"text": line.strip()}

            text = item.get('text') if isinstance(item, dict) and item.get('text') is not None else json.dumps(item, ensure_ascii=False)

            clean = self.clean_text(text)
            tokens = self.tokenize(clean)
            lemmas = self.lemmatize(tokens)
            filtered = self.filter_tokens(lemmas)

            lemma_counter.update(filtered)

            out = {
                "original": item,
                "clean_text": clean,
                "tokens": tokens,
                "lemmas": filtered
            }

            documents.append(out)

        top = lemma_counter.most_common(top_n)
        if top:
            print(f"Top {top_n} lemmas:")
            for lemma, cnt in top:
                print(f"{lemma}: {cnt}")

        return documents
