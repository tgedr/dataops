import abc
from typing import Any, Dict, Optional

from tgedr.dataops.processor import Processor


class ChainException(Exception):
    pass


class ChainInterface(metaclass=abc.ABCMeta):
    @classmethod
    def __subclasshook__(cls, subclass):
        return (
            hasattr(subclass, "next")
            and callable(subclass.next)
            and hasattr(subclass, "execute")
            and callable(subclass.execute)
        ) or NotImplemented


class ChainMixin(abc.ABC):
    def next(self, handler: "ChainMixin") -> "ChainMixin":
        if "_next" not in self.__dict__ or self._next is None:
            self._next: "ChainMixin" = handler
        else:
            self._next.next(handler)
        return self

    @abc.abstractmethod
    def execute(self, context: Optional[Dict[str, Any]] = None) -> Any:
        raise NotImplementedError()


class ProcessorChainMixin(ChainMixin):
    def execute(self, context: Optional[Dict[str, Any]] = None) -> Any:
        self.process(context=context)
        if "_next" in self.__dict__ and self._next is not None:
            self._next.execute(context=context)


@ChainInterface.register
class ProcessorChain(ProcessorChainMixin, Processor):
    pass


@ChainInterface.register
class Chain(ChainMixin, abc.ABC):
    @abc.abstractmethod
    def execute(self, context: Optional[Dict[str, Any]] = None) -> Any:
        raise NotImplementedError()
