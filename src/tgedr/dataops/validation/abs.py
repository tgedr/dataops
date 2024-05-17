from abc import ABC, abstractmethod
import logging
from typing import Any

from great_expectations.core import ExpectationSuite
from great_expectations.dataset.dataset import Dataset

from tgedr.dataops.utils_reflection import UtilsReflection

logger = logging.getLogger(__name__)


class DataValidationException(Exception):
    pass


class DataValidation(ABC):
    @staticmethod
    def get_impl(name: str):
        logger.info(f"[get_impl|in] ({name})")
        result = None
        module = ".".join(__name__.split(".")[:-1]) + "." + name.lower()
        try:
            result = UtilsReflection.load_subclass_from_module(module, "Impl", DataValidation)()
        except Exception as x:
            raise DataValidationException(f"[get_impl] couldn't load implementation for {name}: {x}")
        logger.info(f"[get_impl|out] ({result})")
        return result

    @abstractmethod
    def _get_dataset(self, df: Any) -> Dataset:
        raise NotImplementedError("DataValidation")

    def validate(self, df: Any, expectations: dict) -> None:
        logger.info(f"[validate|in] ({df}, {expectations})")

        try:
            dataset = self._get_dataset(df)

            validation = dataset.validate(ExpectationSuite(**expectations), only_return_failures=True)
            result = validation.to_json_dict()
        except Exception as x:
            raise DataValidationException(f"[validate] failed data expectations", x)

        logger.info(f"[validate|out] => {result['success']}")
        return result
