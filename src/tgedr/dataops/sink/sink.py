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
        return (
            hasattr(subclass, "put")
            and callable(subclass.put)
            and hasattr(subclass, "delete")
            and callable(subclass.delete)
        ) or NotImplemented


@SinkInterface.register
class Sink(abc.ABC):
    """abstract class defining methods ('put' and 'delete') to manage persistence of data somewhere as defined by implementing classes"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self._config = config

    @abc.abstractmethod
    def put(self, context: Optional[Dict[str, Any]] = None) -> Any:
        raise NotImplementedError()

    @abc.abstractmethod
    def delete(self, context: Optional[Dict[str, Any]] = None):
        raise NotImplementedError()


@SinkInterface.register
class SinkChain(Chain, abc.ABC):
    def execute(self, context: Optional[Dict[str, Any]] = None) -> Any:
        return self.put(context=context)
