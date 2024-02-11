import abc
from typing import Any, Dict, Optional

from tgedr.dataops.chain import Chain


class SinkException(Exception):
    pass


class SinkInterface(metaclass=abc.ABCMeta):
    """
    def put(self, context: Optional[Dict[str, Any]] = None) -> Any:
        raise NotImplementedError()
    """

    @classmethod
    def __subclasshook__(cls, subclass):
        return hasattr(subclass, "put") and callable(subclass.put) or NotImplemented


@SinkInterface.register
class Sink(abc.ABC):
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self._config = config

    @abc.abstractmethod
    def put(self, context: Optional[Dict[str, Any]] = None) -> Any:
        raise NotImplementedError()


@SinkInterface.register
class SinkChain(Chain, abc.ABC):
    def execute(self, context: Optional[Dict[str, Any]] = None) -> Any:
        return self.put(context=context)
