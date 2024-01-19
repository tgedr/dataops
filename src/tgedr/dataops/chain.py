from typing import Any, Dict, Optional


class ProcessorChainMixin:
    def next(self, handler: "ProcessorChainMixin") -> "ProcessorChainMixin":
        if "_next" not in self.__dict__ or self._next is None:
            self._next: "ProcessorChainMixin" = handler
        else:
            self._next.next(handler)
        return self

    def execute(self, context: Optional[Dict[str, Any]] = None) -> None:
        self.process(context=context)
        if "_next" in self.__dict__ and self._next is not None:
            self._next.execute(context=context)
