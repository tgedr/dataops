import logging
import os
from typing import Any, Dict, Optional
from tgedr.dataops.commons.s3_connector import S3Connector

from tgedr.dataops.sink.sink import Sink, SinkException
from tgedr.dataops.commons.utils_fs import remove_s3_protocol


logger = logging.getLogger(__name__)


class S3FileSink(Sink, S3Connector):
    """sink class used to save/persist a local object/file to an s3 bucket"""

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

        source = context[self.CONTEXT_SOURCE_PATH]
        if os.path.isdir(source):
            raise SinkException("source can't be a folder, must be a file")

        target = remove_s3_protocol(context[self.CONTEXT_TARGET_PATH])
        target_elements = target.split("/")
        target_bucket = target_elements[0]
        target_key = "/".join(target_elements[1:])

        if target_key.endswith("/"):
            target_file = os.path.basename(source)
            target_key = target_key + target_file

        logger.info(f"[put] uploading {source} to key: {target_key} in bucket: {target_bucket}")
        self._client.upload_file(Filename=source, Bucket=target_bucket, Key=target_key)

        logger.info("[put|out]")

    def delete(self, context: Optional[Dict[str, Any]] = None):
        logger.info(f"[delete|in] ({context})")

        if self.CONTEXT_TARGET_PATH not in context:
            raise SinkException(f"you must provide context for {self.CONTEXT_TARGET_PATH}")

        target = remove_s3_protocol(context[self.CONTEXT_TARGET_PATH])
        target_elements = target.split("/")
        bucket = target_elements[0]
        key = "/".join(target_elements[1:])

        response = self._client.delete_object(Bucket=bucket, Key=key)
        logger.info(f"[delete] response: {response}")
        logger.info("[delete|out]")
