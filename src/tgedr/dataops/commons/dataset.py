from dataclasses import dataclass
import json
from pyspark.sql import DataFrame
from tgedr.dataops.commons.metadata import Metadata


@dataclass(frozen=True)
class Dataset:
    """
    utility immutable class to wrap up a dataframe along with metadata
    """

    __slots__ = ["metadata", "data"]
    metadata: Metadata
    data: DataFrame

    def as_dict(self) -> dict:
        """serialize the dataset as a dictionary"""
        return {"metadata": self.metadata.as_dict(), "data": str(self.data.__repr__)}

    def __str__(self) -> str:
        """serialize the dataset as a json string"""
        return json.dumps(self.as_dict())
