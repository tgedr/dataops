from dataclasses import dataclass
import json
from typing import Dict, List, Optional, Union


@dataclass(frozen=True)
class FieldFrame:
    """
    class depicting a field values range, to be used in metadata

    Parameters:
        field (str): the name of the field
        lower (Union[int, str, float]): field lower bound
        upper (Union[int, str, float]): field upper bound
    """

    __slots__ = ["field", "lower", "upper"]
    field: str
    lower: Union[int, str, float]
    upper: Union[int, str, float]

    def as_dict(self) -> Dict[str, Union[int, str, float]]:
        return {"field": self.field, "lower": self.lower, "upper": self.upper}

    @staticmethod
    def from_str(src: str) -> "FieldFrame":
        r = json.loads(src)
        field = r["field"]
        lower = r["lower"]
        upper = r["upper"]
        return FieldFrame(field=field, lower=lower, upper=upper)

    def __str__(self) -> str:
        return json.dumps(self.as_dict())

    def __eq__(self, other):
        return self.field == other.field and self.lower == other.lower and self.upper == other.upper

    def __gt__(self, other):
        return self.field > other.field or (
            self.field == other.field
            and (self.lower > other.lower or (self.lower == other.lower and self.upper > other.upper))
        )

    def __ne__(self, other):
        return not other == self

    def __ge__(self, other):
        return other == self or self > other

    def __le__(self, other):
        return other == self or self < other

    def __lt__(self, other):
        return other > self


@dataclass(frozen=True)
class Metadata:
    """immutable class depicting dataset metadata

    Parameters:
        name (str): the name of the dataset
        version (Optional[str]): version of this dataset, if available
        framing (Optional[List[FieldFrame]]): multiple field frames
        sources (Optional[List["Metadata"]]): metadatas related to the datasets sourcing this one
    """

    __slots__ = ["name", "version", "framing", "sources"]
    name: str
    version: Optional[str]
    framing: Optional[List[FieldFrame]]
    sources: Optional[List["Metadata"]]

    def as_dict(self) -> dict:
        result = {"name": self.name}
        if self.version is not None:
            result["version"] = self.version
        if self.framing is not None:
            result["framing"] = []
            for f in self.framing:
                (result["framing"]).append(f.as_dict())
        if self.sources is not None:
            result["sources"] = []
            for source in self.sources:
                (result["sources"]).append(source.as_dict())

        return result

    def __str__(self):
        return json.dumps(self.as_dict())

    def __eq__(self, other: object) -> bool:
        return (
            self.name == other.name
            and (
                (self.version is None and other.version is None)
                or ((self.version is not None and other.version is not None) and self.version == other.version)
            )
            and (
                (self.framing is None and other.framing is None)
                or (
                    (self.framing is not None and other.framing is not None)
                    and sorted(self.framing) == sorted(other.framing)
                )
            )
            and (
                (self.sources is None and other.sources is None)
                or (
                    (self.sources is not None and other.sources is not None)
                    and sorted(self.sources) == sorted(other.sources)
                )
            )
        )

    def __gt__(self, other):
        return self.name > other.name or (
            self.name == other.name
            and (
                ((self.version is not None and other.version is not None) and (self.version > other.version))
                or (self.version is not None and other.version is None)
                or (
                    (
                        (self.framing is not None and other.framing is not None)
                        and (sorted(self.framing) > sorted(other.framing))
                    )
                    or (self.framing is not None and other.framing is None)
                    or (
                        (
                            (self.sources is not None and other.sources is not None)
                            and (sorted(self.sources) > sorted(other.sources))
                        )
                        or (self.sources is not None and other.sources is None)
                    )
                )
            )
        )

    def __ne__(self, other):
        return not other == self

    def __ge__(self, other):
        return other == self or self > other

    def __le__(self, other):
        return other == self or self < other

    def __lt__(self, other):
        return other > self

    @staticmethod
    def from_str(src: str) -> "Metadata":
        r = json.loads(src)
        name = r["name"]
        version = None if "version" not in r else r["version"]

        framing = None
        framing_entries = None if "framing" not in r else r["framing"]
        if framing_entries is not None:
            framing = []
            for framing_entry in framing_entries:
                framing.append(FieldFrame.from_str(json.dumps(framing_entry)))

        sources = None
        sources_entries = None if "sources" not in r else r["sources"]
        if sources_entries is not None:
            sources = []
            for source_entry in sources_entries:
                source_entry_as_str = json.dumps(source_entry)
                sources.append(Metadata.from_str(source_entry_as_str))

        return Metadata(name=name, version=version, framing=framing, sources=sources)
