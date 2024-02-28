from typing import Any, Dict, Optional

from tgedr.dataops.chain import ProcessorChain


class StartCount(ProcessorChain):
    def process(self, context: Optional[Dict[str, Any]] = None) -> Any:
        context["state"] = 2


class AddOne(ProcessorChain):
    def process(self, context: Optional[Dict[str, Any]] = None) -> None:
        context["state"] = context["state"] + 1


class ShowCount(ProcessorChain):
    def process(self, context: Optional[Dict[str, Any]] = None) -> None:
        print(f"count: {context['state']}")


def test_handling():
    chain = StartCount().next(AddOne()).next(ShowCount())

    context = {}
    chain.execute(context)

    assert 3 == (context["state"])
