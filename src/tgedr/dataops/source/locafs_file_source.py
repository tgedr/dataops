import logging
import os
import shutil
from typing import Any, Dict, List, Optional
from tgedr.dataops.source.source import Source, SourceException


logger = logging.getLogger(__name__)


class LocalFsFileSource(Source):
    CONTEXT_KEY_SOURCE = "source"
    CONTEXT_KEY_TARGET = "target"
    CONTEXT_KEY_SUFFIX = "file_suffix"
    __DEFAULT_SUFFIX = ".txt"

    def get(self, context: Optional[Dict[str, Any]] = None) -> Any:
        logger.info(f"[get|in] ({context})")

        if self.CONTEXT_KEY_SOURCE not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_SOURCE}")

        source = context[self.CONTEXT_KEY_SOURCE]
        suffix = self.__DEFAULT_SUFFIX
        if self.CONTEXT_KEY_SUFFIX in context:
            suffix = context[self.CONTEXT_KEY_SUFFIX]

        files: List[str] = [os.path.join(source, file) for file in os.listdir(source) if file.endswith(suffix)]
        result: List[str] = []

        if self.CONTEXT_KEY_TARGET in context:
            target = context[self.CONTEXT_KEY_TARGET]
            for file in files:
                basename = os.path.basename(file)
                new_file = os.path.join(target, basename)
                shutil.move(file, new_file)
                result.append(new_file)
        else:
            result = files

        logger.info("[get|out] => {result}")
        return result
