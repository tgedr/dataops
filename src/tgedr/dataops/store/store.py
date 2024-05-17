import abc
from typing import Any, Dict, Optional


class StoreException(Exception):
    pass


class NoStoreException(StoreException):
    pass


class StoreInterface(metaclass=abc.ABCMeta):
    @classmethod
    def __subclasshook__(cls, subclass):
        return (
            hasattr(subclass, "get")
            and callable(subclass.get)
            and hasattr(subclass, "delete")
            and callable(subclass.delete)
            and hasattr(subclass, "save")
            and callable(subclass.save)
            and hasattr(subclass, "update")
            and callable(subclass.update)
        ) or NotImplemented


@StoreInterface.register
class Store(abc.ABC):
    """abstract class used to manage persistence, defining CRUD-like (CreateReadUpdateDelete) methods"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self._config = config

    @abc.abstractmethod
    def get(self, key: str, **kwargs) -> Any:
        raise NotImplementedError()

    @abc.abstractmethod
    def delete(self, key: str, **kwargs) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def save(self, df: Any, key: str, **kwargs):
        raise NotImplementedError()

    @abc.abstractmethod
    def update(self, df: Any, key: str, **kwargs):
        raise NotImplementedError()
