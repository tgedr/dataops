import logging
from typing import Any, Dict, Optional
from tgedr.dataops.commons.s3_connector import S3Connector
from tgedr.dataops.source.source import Source, SourceException


logger = logging.getLogger(__name__)


class S3FileSource(Source, S3Connector):
    CONTEXT_KEY_SOURCE = "source"
    CONTEXT_KEY_TARGET = "target"

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        Source.__init__(self, config=config)
        S3Connector.__init__(self)

    def get(self, context: Optional[Dict[str, Any]] = None) -> Any:
        logger.info(f"[get|in] ({context})")

        if self.CONTEXT_KEY_SOURCE not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_SOURCE}")
        if self.CONTEXT_KEY_TARGET not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_TARGET}")

        path = context[self.CONTEXT_KEY_SOURCE]
        path_elements = path.split("/")
        bucket = path_elements[0]
        key = "/".join(path_elements[1:])
        local_file_path = context[self.CONTEXT_KEY_TARGET]

        self._client.download_file(Bucket=bucket, Key=key, Filename=local_file_path)
        logger.info("[get|out]")
