from typing import Any
from great_expectations.dataset.dataset import Dataset

from tgedr.dataops.validation.abs import DataValidation
from great_expectations.dataset.sparkdf_dataset import PandasDataset


class Impl(DataValidation):
    def _get_dataset(self, df: Any) -> Dataset:
        return PandasDataset(df)
