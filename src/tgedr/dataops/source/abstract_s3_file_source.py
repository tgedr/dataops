from abc import ABC
import logging
from typing import Any, Dict, List, Optional

from tgedr.dataops.commons.s3_connector import S3Connector
from tgedr.dataops.commons.utils_fs import process_s3_url
from tgedr.dataops.source.source import Source, SourceException


logger = logging.getLogger()


class AbstractS3FileSource(Source, S3Connector, ABC):
    """abstract class used to read file sources from s3"""

    CONTEXT_KEY_URL = "url"
    CONTEXT_KEY_SUFFIX = "suffix"

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        Source.__init__(self, config=config)
        S3Connector.__init__(self)

    def list(self, context: Optional[Dict[str, Any]] = None) -> List[str]:
        logger.info(f"[list|in] ({context})")

        result: List[str] = []
        if self.CONTEXT_KEY_URL not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_URL}")

        protocol, bucket, key = process_s3_url(context[self.CONTEXT_KEY_URL])

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
