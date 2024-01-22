import logging
import os

import pytest

from tgedr.dataops.sink import Sink
from tgedr.dataops.source import Source
from tgedr.dataops.utils_reflection import UtilsReflection

logger = logging.getLogger(__name__)

MODULE = "test.tgedr.dataops.impls"


def test_load_subclass_from_module():
    assert UtilsReflection.load_subclass_from_module(MODULE, "ASource", Source) is not None


def test_load_subclass_from_module_attribute_error():
    with pytest.raises(AttributeError):
        UtilsReflection.load_subclass_from_module(MODULE, "SourceImplMissing", Source)


def test_load_class_from_module_type_error1():
    with pytest.raises(TypeError):
        UtilsReflection.load_subclass_from_module(MODULE, "ASink", Source)


def test_load_class_from_module_type_error2():
    with pytest.raises(TypeError):
        UtilsReflection.load_subclass_from_module(MODULE, "NotASource", Source)


def test_get_type():
    assert UtilsReflection.get_type(MODULE, "ASink") is not None


def test_is_subclass_of():
    o = UtilsReflection.get_type(MODULE, "ASink")
    assert UtilsReflection.is_subclass_of(o, Sink)


def test_is_subclass_of_not():
    o = UtilsReflection.get_type(MODULE, "ASink")
    assert not UtilsReflection.is_subclass_of(o, Source)


def test_find_module_classes():
    assert any(
        elem in [x.__name__ for x in UtilsReflection.find_module_classes("tgedr.dataops.processor")]
        for elem in ["ProcessorException", "ProcessorInterface", "Processor"]
    )


def test_find_module_classes_and_create_one():
    the_class = None
    classes = UtilsReflection.find_module_classes("tgedr.dataops.processor")
    for clazz in classes:
        if clazz.__name__ == "ProcessorException":
            the_class = clazz
            break
    assert UtilsReflection.is_subclass_of(the_class, Exception)
    dummy = the_class()
    assert dummy is not None


def test_find_package_folder():
    from tgedr.dataops.processor import __file__ as f

    expected = os.path.dirname(os.path.abspath(f))
    assert expected == UtilsReflection.find_package_path("tgedr.dataops")


def test_find_class_implementations():
    assert 1 == len(UtilsReflection.find_class_implementations(packages="test.tgedr.dataops", clazz=Source))
