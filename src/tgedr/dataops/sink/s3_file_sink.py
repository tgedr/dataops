import logging
import os
from typing import Any, Dict, List, Optional
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

        target = context[self.CONTEXT_TARGET_PATH]
        target_elements = target.split("/")
        bucket = target_elements[0]
        key = "/".join(target_elements[1:])

        source = context[self.CONTEXT_SOURCE_PATH]

        if os.path.isdir(source):
            files: List[str] = [f for f in os.listdir(source) if not os.path.isdir(os.path.join(source, f))]
            for file in files:
                target_key = os.path.join(key, file)
                source_file = os.path.join(source, file)
                logger.info(f"[put] uploading file {source_file} to key {target_key}")
                self._client.upload_file(Filename=source_file, Bucket=bucket, Key=target_key)
        else:
            logger.info(f"[put] uploading single file {source} to key {key}")
            self._client.upload_file(Filename=source, Bucket=bucket, Key=key)

        logger.info("[put|out]")
