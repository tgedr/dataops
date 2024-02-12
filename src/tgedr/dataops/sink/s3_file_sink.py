import logging
from typing import Any, Dict, Optional
from tgedr.dataops.commons.s3_connector import S3Connector
from tgedr.dataops.sink.sink import Sink, SinkException


logger = logging.getLogger(__name__)


class S3FileSink(Sink, S3Connector):
    CONTEXT_SOURCE_PATH = "source"
    CONTEXT_TARGET_PATH = "target"

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        Sink.__init__(self, config=config)
        S3Connector.__init__(self)

    def put(self, context: Optional[Dict[str, Any]] = None) -> Any:
        logger.info(f"[put|in] ({context})")

        if self.CONTEXT_SOURCE_PATH not in context:
            raise SinkException(f"you must provide context for {self.CONTEXT_SOURCE_PATH}")
        if self.CONTEXT_TARGET_PATH not in context:
            raise SinkException(f"you must provide context for {self.CONTEXT_TARGET_PATH}")

        path = context[self.CONTEXT_TARGET_PATH]
        path_elements = path.split("/")
        bucket = path_elements[0]
        key = "/".join(path_elements[1:])
        local_file_path = context[self.CONTEXT_SOURCE_PATH]

        self._client.upload_file(Filename=local_file_path, Bucket=bucket, Key=key)
        logger.info("[put|out]")
