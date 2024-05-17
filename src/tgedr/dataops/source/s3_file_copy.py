import logging
import os
from typing import Any, Dict, List, Optional
from tgedr.dataops.commons.s3_connector import S3Connector

from tgedr.dataops.source.source import Source, SourceException
from tgedr.dataops.commons.utils_fs import process_s3_path, resolve_s3_protocol


logger = logging.getLogger(__name__)


class S3FileCopy(Source, S3Connector):
    """class used to copy objects/files from an s3 bucket to another s3 bucket"""

    CONTEXT_KEY_SOURCE = "source"
    CONTEXT_KEY_TARGET = "target"
    CONTEXT_KEY_FILES = "files"
    CONTEXT_KEY_SUFFIX = "suffix"
    CONTEXT_KEY_PRESERVE_SOURCE_KEY = "preserve_source_key"

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        Source.__init__(self, config=config)
        S3Connector.__init__(self)

    def list(self, context: Optional[Dict[str, Any]] = None) -> List[str]:
        logger.info(f"[list|in] ({context})")

        result: List[str] = []
        if self.CONTEXT_KEY_SOURCE not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_SOURCE}")

        s3_protocol: str = resolve_s3_protocol(context[self.CONTEXT_KEY_SOURCE])
        protocol = "" if s3_protocol is None else s3_protocol

        bucket, key = process_s3_path(context[self.CONTEXT_KEY_SOURCE])

        objs = self._client.list_objects_v2(Bucket=bucket, Prefix=key)
        result = [
            (protocol + bucket + "/" + entry["Key"]) for entry in objs["Contents"] if not (entry["Key"]).endswith("/")
        ]

        if self.CONTEXT_KEY_SUFFIX in context:
            suffix: str = context[self.CONTEXT_KEY_SUFFIX]
            result = [f for f in result if f.endswith(suffix)]

        logger.debug(f"[list|out] => {result}")
        logger.info(f"[list|out] => result len: {len(result)}")
        return result

    def get(self, context: Optional[Dict[str, Any]] = None) -> Any:
        logger.info(f"[get|in] ({context})")

        result: List[str] = []
        if self.CONTEXT_KEY_FILES not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_FILES}")
        if self.CONTEXT_KEY_TARGET not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_TARGET}")

        preserve_source_key = False
        if self.CONTEXT_KEY_PRESERVE_SOURCE_KEY in context:
            preserve_source_key = (
                True if (str(context[self.CONTEXT_KEY_PRESERVE_SOURCE_KEY]).lower() in ["1", "true"]) else False
            )
        logger.info(f"[get] preserve_source_key: {preserve_source_key}")

        target_bucket, target_key = process_s3_path(context[self.CONTEXT_KEY_TARGET])
        s3_protocol: str = resolve_s3_protocol(context[self.CONTEXT_KEY_TARGET])
        protocol = "" if s3_protocol is None else s3_protocol

        files = context[self.CONTEXT_KEY_FILES]
        multiple_files: bool = True if (0 < len(files)) else False

        for file in files:
            src_bucket, src_key = process_s3_path(file)

            src = {"Bucket": src_bucket, "Key": src_key}

            if True == preserve_source_key:
                key = os.path.join(target_key, src_key)
            else:
                key = target_key
                if not multiple_files:
                    if target_key.endswith("/"):
                        src_key_leaf = os.path.basename(src_key)
                        key += src_key_leaf
                else:
                    src_key_leaf = os.path.basename(src_key)
                    key = os.path.join(key, src_key_leaf)

            logger.info(
                f"[get] copying... src bucket: {src_bucket}   src key: {src_key}   target bucket: {target_bucket}   target key: {key}"
            )
            self._client.copy(src, target_bucket, key)

            result.append(os.path.join(protocol, target_bucket, key))

        logger.info(f"[get|out] => {result}")
        return result
