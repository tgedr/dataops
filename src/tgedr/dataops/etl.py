from abc import ABC, abstractmethod
import inspect
import logging
from typing import Any, Dict, Optional


logger = logging.getLogger(__name__)


class EtlException(Exception):
    pass


"""
ETL is an abstract base class that should be extended when you want to run an ETL-like task/job
A subclass of ETL has extract, transform and load methods.

The ETL class has static utility methods that serve as an outline for the class. Use example below:

```python
class MyEtl(Etl):
    @Etl.inject_configuration
    def extract(self, MY_PARAM) -> None:
        # "MY_PARAM" should be supplied in 'configuration' dict otherwise an exception will be raised

    @Etl.inject_configuration
    def load(self, NOT_IN_CONFIG=123) -> None:
        # If you try to inject a configuration key that is NOT on the configuration dictionary
        # supplied to the constructor, it will not throw an error as long as you set a default
        # value in the method you wish to decorate
        assert NOT_IN_CONFIG == 123, "This will be ok"

```
"""


class Etl(ABC):
    def __init__(self, configuration: Optional[Dict[str, Any]] = None) -> None:
        """Initialize a new instance of ETL.

        Parameters
        ----------
        configuration : Dict[str, Any]
            source for configuration injection
        """
        self._configuration = configuration

    @abstractmethod
    def extract(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    def transform(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    def load(self) -> None:
        raise NotImplementedError()

    def validate_extract(self):
        """
        Optional extra checks for extract step.
        """

    def validate_transform(self):
        """
        Optional extra checks for transform step.
        """

    def run(self) -> Any:
        logger.info("[run|in]")

        self.extract()
        self.validate_extract()

        self.transform()
        self.validate_transform()

        self.load()

        logger.info("[run|out]")

    @staticmethod
    def inject_configuration(f):
        def decorator(self):
            signature = inspect.signature(f)

            missing_params = []
            params = {}
            for param in [parameter for parameter in signature.parameters if parameter != "self"]:
                if signature.parameters[param].default != inspect._empty:
                    params[param] = signature.parameters[param].default
                else:
                    params[param] = None
                    if self._configuration is None or param not in self._configuration:
                        missing_params.append(param)

                if self._configuration is not None and param in self._configuration:
                    params[param] = self._configuration[param]

            if 0 < len(missing_params):
                raise EtlException(
                    f"{type(self).__name__}.{f.__name__}: missing required configuration parameters: {missing_params}"
                )

            return f(
                self,
                *[params[argument] for argument in params],
            )

        return decorator
