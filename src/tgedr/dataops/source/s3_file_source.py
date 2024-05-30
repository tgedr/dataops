import logging
import os
from typing import Any, Dict, List, Optional
from tgedr.dataops.commons.s3_connector import S3Connector

from tgedr.dataops.source.source import Source, SourceException
from tgedr.dataops.commons.utils_fs import remove_s3_protocol, resolve_s3_protocol


logger = logging.getLogger(__name__)


class S3FileSource(Source, S3Connector):
    """class used to retrieve objects/files from s3 bucket to local fs location"""

    CONTEXT_KEY_SOURCE = "source"
    CONTEXT_KEY_TARGET = "target"
    CONTEXT_KEY_FILES = "files"
    CONTEXT_KEY_SUFFIX = "suffix"

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

        path = remove_s3_protocol(context[self.CONTEXT_KEY_SOURCE])
        path_elements = path.split("/")
        bucket = path_elements[0]
        key = "/".join(path_elements[1:])

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

        preserve_structure = True

        files = context[self.CONTEXT_KEY_FILES]
        target = context[self.CONTEXT_KEY_TARGET]

        target_is_dir: bool = False
        if os.path.isdir(target):
            target_is_dir = True

        for file in files:
            path_elements = remove_s3_protocol(file).split("/")
            bucket = path_elements[0]
            key = "/".join(path_elements[1:])
            filename = path_elements[-1]

            if target_is_dir:
                if preserve_structure:
                    local_file = os.path.join(target, key)
                else:
                    local_file = os.path.join(target, filename)
            else:
                local_file = target

            # assure we have that path there
            local_folder = os.path.dirname(local_file)
            if not os.path.isdir(local_folder):
                os.makedirs(local_folder)

            logger.info(f"[get] bucket: {bucket}   key: {key}   file: {file}   local_file: {local_file}")
            self._client.download_file(Bucket=bucket, Key=key, Filename=local_file)
            result.append(local_file)

        logger.info(f"[get|out] => {result}")
        return result
