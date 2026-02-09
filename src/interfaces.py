from abc import ABC, abstractmethod
from typing import List, Dict, Any

class IDataCollector(ABC):
    @abstractmethod
    def collect(self, **kwargs) -> List[Dict[str, Any]]:
        pass

class IFileProcessor(ABC):
    @abstractmethod
    def process(self, content: str, filename: str, **kwargs) -> List[Dict[str, Any]]:
        pass