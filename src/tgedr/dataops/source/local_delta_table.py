import logging
import os
import re
from typing import Any, Dict, List, Optional
import glob

from tgedr.dataops.source.delta_table_source import DeltaTableSource
from tgedr.dataops.source.source import SourceException


logger = logging.getLogger()


class LocalDeltaTable(DeltaTableSource):
    """class used to read delta lake format datasets from local fs with python only, pyspark not needed, returning a pandas dataframe"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config=config)

    @property
    def _storage_options(self):
        return None

    def list(self, context: Optional[Dict[str, Any]] = None) -> List[str]:
        """lists the available delta lake datasets in the url provided"""
        logger.info(f"[list|in] ({context})")

        result: List[str] = []
        if self.CONTEXT_KEY_URL not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_URL}")

        url = context[self.CONTEXT_KEY_URL]
        if not os.path.isdir(url):
            raise SourceException(f"not a delta lake url: {url}")

        matches: set[str] = set()
        pattern: str = f".*{url}/(.*)/_delta_log/.*"
        for entry in glob.iglob(url + "**/**", recursive=True):
            match = re.search(pattern, entry)
            if match:
                matches.add(match.group(1))

        result = list(matches)

        logger.info(f"[list] result: {result}")
        logger.info(f"[list|out] => result len: {len(result)}")
        return result
