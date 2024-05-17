import logging
import os
import shutil
from typing import Any, Dict, List, Optional

from tgedr.dataops.source.source import Source, SourceException


logger = logging.getLogger(__name__)


class LocalFsFileSource(Source):
    """source class used to retrieve local objects/files to a another local fs location"""

    CONTEXT_KEY_SOURCE = "source"
    CONTEXT_KEY_TARGET = "target"
    CONTEXT_KEY_SUFFIX = "suffix"
    CONTEXT_KEY_FILES = "files"

    def list(self, context: Optional[Dict[str, Any]] = None) -> List[str]:
        logger.info(f"[list|in] ({context})")
        result: List[str] = []
        if self.CONTEXT_KEY_SOURCE not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_SOURCE}")

        source = context[self.CONTEXT_KEY_SOURCE]
        if os.path.isdir(source):
            suffix = None
            if self.CONTEXT_KEY_SUFFIX in context:
                suffix = context[self.CONTEXT_KEY_SUFFIX]
                result: List[str] = [os.path.join(source, file) for file in os.listdir(source) if file.endswith(suffix)]
            else:
                result: List[str] = [os.path.join(source, file) for file in os.listdir(source)]
        elif os.path.isfile(source):
            result: List[str] = [source]

        logger.debug(f"[list|out] => {result}")
        logger.info(f"[list|out] => result len: {len(result)}")
        return result

    def get(self, context: Optional[Dict[str, Any]] = None) -> Any:
        logger.info(f"[get|in] ({context})")

        if self.CONTEXT_KEY_FILES not in context or self.CONTEXT_KEY_TARGET not in context:
            raise SourceException(f"{self.CONTEXT_KEY_FILES} and {self.CONTEXT_KEY_TARGET} must be provided in config")
        files = context[self.CONTEXT_KEY_FILES]
        target = context[self.CONTEXT_KEY_TARGET]

        if "list" != type(files).__name__:
            if "string" == type(files).__name__:
                files = [files]
            else:
                raise SourceException("files argument must be a list of strings or a string")

        target_is_dir: bool = False
        if os.path.isdir(target):
            target_is_dir = True

        result: List[str] = []

        for file in files:
            basename = os.path.basename(file)
            if target_is_dir:
                new_file = os.path.join(target, basename)
            else:
                new_file = target
            shutil.copy(file, new_file)
            result.append(new_file)

        logger.info("[get|out] => {result}")
        return result
