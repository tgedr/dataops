import logging
import shutil
from typing import Any, Dict, Optional
from tgedr.dataops.sink.sink import Sink, SinkException


logger = logging.getLogger(__name__)


class LocalFsFileSink(Sink):
    CONTEXT_SOURCE_PATH = "source"
    CONTEXT_TARGET_PATH = "target"

    def put(self, context: Optional[Dict[str, Any]] = None) -> Any:
        logger.info(f"[put|in] ({context})")

        if self.CONTEXT_SOURCE_PATH not in context:
            raise SinkException(f"you must provide context for {self.CONTEXT_SOURCE_PATH}")
        if self.CONTEXT_TARGET_PATH not in context:
            raise SinkException(f"you must provide context for {self.CONTEXT_TARGET_PATH}")

        source = context[self.CONTEXT_SOURCE_PATH]
        target = context[self.CONTEXT_TARGET_PATH]

        shutil.copy(source, target)
        logger.info("[put|out]")
