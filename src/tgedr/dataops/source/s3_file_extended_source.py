import logging
from typing import Any, Dict, Optional

from tgedr.dataops.source.s3_file_source import S3FileSource
from tgedr.dataops.source.source import SourceException
from tgedr.dataops.commons.utils_fs import process_s3_path


logger = logging.getLogger(__name__)


class S3FileExtendedSource(S3FileSource):
    """class used to retrieve objects/files from s3 bucket to local fs location"""

    METADATA_KEYS = ["LastModified", "ContentLength", "ETag", "VersionId", "ContentType"]

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config=config)

    def get_metadata(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        logger.info(f"[get_metadata|in] ({context})")

        result: Dict[str, Any] = {}
        if self.CONTEXT_KEY_SOURCE not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_SOURCE}")

        bucket, key = process_s3_path(context[self.CONTEXT_KEY_SOURCE])

        o = self._client.head_object(Bucket=bucket, Key=key)

        for key in list(o.keys()):
            if key in self.METADATA_KEYS:
                if key == "LastModified":
                    result[key] = int(o[key].timestamp())
                else:
                    result[key] = o[key]

        logger.info(f"[get_metadata|out] => result len: {result}")
        return result
