from typing import Any, Dict, Optional

from tgedr.dataops.sink import Sink
from tgedr.dataops.source import Source


class ASink(Sink):
    def put(self, context: Optional[Dict[str, Any]] = None) -> Any:
        pass


class ASource(Source):
    def get(self, context: Optional[Dict[str, Any]] = None) -> Any:
        pass


class NotASource:
    def getX(self, context: Optional[Dict[str, Any]] = None) -> Any:
        pass
