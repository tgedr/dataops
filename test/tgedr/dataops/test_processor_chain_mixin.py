from typing import Any, Dict, Optional

from tgedr.dataops.chain import ProcessorChainMixin
from tgedr.dataops.processor import ProcessorInterface


@ProcessorInterface.register
class StartCount(ProcessorChainMixin):
    def process(self, context: Optional[Dict[str, Any]] = None) -> Any:
        context["state"] = 2


@ProcessorInterface.register
class AddOne(ProcessorChainMixin):
    def process(self, context: Optional[Dict[str, Any]] = None) -> None:
        context["state"] = context["state"] + 1


@ProcessorInterface.register
class ShowCount(ProcessorChainMixin):
    def process(self, context: Optional[Dict[str, Any]] = None) -> None:
        print(f"count: {context['state']}")


def test_handling():
    chain = StartCount().next(AddOne()).next(ShowCount())

    context = {}
    chain.execute(context)

    assert 3 == (context["state"])
