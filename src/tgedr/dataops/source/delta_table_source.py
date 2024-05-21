from abc import ABC, abstractmethod
import logging
from typing import Any, Dict, List, Optional
from pandas import DataFrame
from deltalake import DeltaTable

from tgedr.dataops.source.source import Source, SourceException


logger = logging.getLogger()


class DeltaTableSource(Source, ABC):
    """abstract class used to read delta lake format datasets returning a pandas dataframe"""

    CONTEXT_KEY_URL: str = "url"
    CONTEXT_KEY_COLUMNS: str = "columns"

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config=config)

    @property
    @abstractmethod
    def _storage_options(self):
        return None

    def get(self, context: Optional[Dict[str, Any]] = None) -> DataFrame:
        """retrieves a delta lake table"""
        logger.info(f"[get|in] ({context})")
        result: DataFrame = None

        if self.CONTEXT_KEY_URL not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_URL}")

        columns: List[str] = None
        if self.CONTEXT_KEY_COLUMNS in context:
            columns = context[self.CONTEXT_KEY_COLUMNS]

        delta_table = DeltaTable(
            table_uri=context[self.CONTEXT_KEY_URL], storage_options=self._storage_options, without_files=True
        )
        result = delta_table.to_pandas(columns=columns)

        logger.info(f"[get|out] => {result}")
        return result
