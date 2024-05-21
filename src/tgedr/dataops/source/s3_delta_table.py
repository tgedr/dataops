import logging
import re
from typing import Any, Dict, List, Optional

from tgedr.dataops.commons.s3_connector import S3Connector
from tgedr.dataops.commons.utils_fs import remove_s3_protocol, resolve_s3_protocol
from tgedr.dataops.source.delta_table_source import DeltaTableSource
from tgedr.dataops.source.source import SourceException


logger = logging.getLogger()


class S3DeltaTable(DeltaTableSource, S3Connector):
    """class used to read delta lake format datasets from s3 bucket with python only, pyspark not needed, returning a pandas dataframe"""

    CONFIG_KEY_AWS_ACCESS_KEY_ID: str = "AWS_ACCESS_KEY_ID"
    CONFIG_KEY_AWS_SECRET_ACCESS_KEY: str = "AWS_SECRET_ACCESS_KEY"
    CONFIG_KEY_AWS_SESSION_TOKEN: str = "AWS_SESSION_TOKEN"
    CONFIG_KEY_AWS_REGION: str = "AWS_REGION"

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        DeltaTableSource.__init__(self, config=config)
        S3Connector.__init__(self)

    @property
    def _storage_options(self):
        result = None
        if (self._config is not None) and all(
            element in list(self._config.keys())
            for element in [
                self.CONFIG_KEY_AWS_ACCESS_KEY_ID,
                self.CONFIG_KEY_AWS_SECRET_ACCESS_KEY,
                self.CONFIG_KEY_AWS_SESSION_TOKEN,
                self.CONFIG_KEY_AWS_REGION,
            ]
        ):
            result = {
                "AWS_ACCESS_KEY_ID": self._config[self.CONFIG_KEY_AWS_ACCESS_KEY_ID],
                "AWS_SECRET_ACCESS_KEY": self._config[self.CONFIG_KEY_AWS_SECRET_ACCESS_KEY],
                "AWS_SESSION_TOKEN": self._config[self.CONFIG_KEY_AWS_SESSION_TOKEN],
                "AWS_REGION": self._config[self.CONFIG_KEY_AWS_REGION],
            }

        return result

    def list(self, context: Optional[Dict[str, Any]] = None) -> List[str]:
        """lists the available delta lake datasets in the url provided"""
        logger.info(f"[list|in] ({context})")

        result: List[str] = []
        if self.CONTEXT_KEY_URL not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_URL}")

        s3_protocol: str = resolve_s3_protocol(context[self.CONTEXT_KEY_URL])
        protocol = "" if s3_protocol is None else s3_protocol

        path = remove_s3_protocol(context[self.CONTEXT_KEY_URL])
        path_elements = path.split("/")
        bucket = path_elements[0]
        key = "/".join(path_elements[1:])

        matches: set[str] = set()
        pattern: str = f".*{key}/(.*)/_delta_log/.*"
        for entry in self._client.list_objects_v2(Bucket=bucket, Prefix=key)["Contents"]:
            output_key: str = entry["Key"]
            match = re.search(pattern, output_key)
            if match:
                matches.add(f"{key}/{match.group(1)}")

        result = list(matches)

        logger.info(f"[list] result: {result}")
        logger.info(f"[list|out] => result len: {len(result)}")
        return result
