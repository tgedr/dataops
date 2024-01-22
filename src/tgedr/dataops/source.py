import abc
from typing import Any, Dict, Optional

from tgedr.dataops.chain import Chain


class SourceException(Exception):
    pass


class SourceInterface(metaclass=abc.ABCMeta):
    """
    def get(self, context: Optional[Dict[str, Any]] = None) -> Any:
        raise NotImplementedError()
    """

    @classmethod
    def __subclasshook__(cls, subclass):
        return hasattr(subclass, "get") and callable(subclass.get) or NotImplemented


@SourceInterface.register
class Source(abc.ABC):
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self._config = config

    @abc.abstractmethod
    def get(self, context: Optional[Dict[str, Any]] = None) -> Any:
        raise NotImplementedError()


@SourceInterface.register
class SourceChain(Chain, abc.ABC):
    def execute(self, context: Optional[Dict[str, Any]] = None) -> Any:
        return self.get(context=context)
