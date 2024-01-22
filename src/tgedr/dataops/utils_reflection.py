import importlib
import inspect
import logging
import os
import sys
from importlib import import_module
from typing import Dict, Any, List


logger = logging.getLogger(__name__)


class UtilsReflectionException(Exception):
    pass


class UtilsReflection:
    __MODULE_EXTENSIONS = (".py", ".pyc", ".pyo")

    @staticmethod
    def load_subclass_from_module(module: str, clazz: str, super_clazz: type) -> Any:
        logger.info(f"[load_subclass_from_module|in] (module={module}, clazz={clazz}, super_clazz={super_clazz})")
        result = getattr(import_module(module), clazz)

        if not callable(result):
            raise TypeError(f"Object {clazz} in {module} is not callable.")

        if super_clazz and (not issubclass(result, super_clazz)):
            raise TypeError(f"Wrong class type, it is not a subclass of {super_clazz.__name__}")

        logger.info(f"[load_subclass_from_module|out] => {result}")
        return result

    @staticmethod
    def get_type(module: str, _type: str) -> type:
        logger.info(f"[get_type|in] (module={module}, _type={_type})")
        result = None

        result = getattr(import_module(module), _type)

        logger.info(f"[get_type|out] => {result}")
        return result

    @staticmethod
    def is_subclass_of(sub_class: type, super_class: type) -> bool:
        logger.info(f"[is_subclass_of|in] ({sub_class}, {super_class})")
        result = False

        if callable(sub_class) and issubclass(sub_class, super_class):
            result = True

        logger.info(f"[is_subclass_of|out] => {result}")
        return result

    @staticmethod
    def find_module_classes(module: str) -> List[Any]:
        logger.info(f"[find_module_classes|in] ({module})")
        result = []
        for name, obj in inspect.getmembers(sys.modules[module]):
            if inspect.isclass(obj):
                result.append(obj)
        logger.info(f"[find_module_classes|out] => {result}")
        return result

    @staticmethod
    def find_class_implementations_in_package(package_name: str, super_class: type) -> Dict[str, type]:
        logger.info(f"[find_class_implementations_in_package|in] ({package_name}, {super_class})")
        result = {}

        the_package = importlib.import_module(package_name)
        pkg_path = the_package.__path__[0]
        modules = [
            package_name + "." + module.split(".")[0]
            for module in os.listdir(pkg_path)
            if module.endswith(UtilsReflection.__MODULE_EXTENSIONS) and module != "__init__.py"
        ]

        logger.info(f"[find_class_implementations_in_package] found modules: {modules}")

        for _module in modules:
            if _module not in sys.modules:
                importlib.import_module(_module)

            for _class in UtilsReflection.find_module_classes(_module):
                if UtilsReflection.is_subclass_of(_class, super_class) and _class != super_class:
                    result[_module] = _class

        logger.info(f"[find_class_implementations_in_package|out] => {result}")
        return result

    @staticmethod
    def find_package_path(package_name: str) -> str:
        logger.info(f"[find_package_path|in] ({package_name})")
        the_package = importlib.import_module(package_name)
        result = the_package.__path__[0]
        logger.info(f"[find_package_path|out] => {result}")
        return result

    @staticmethod
    def find_class_implementations(packages: str, clazz: Any) -> Dict[str, Any]:
        """
        throws UtilsReflectionException
        """
        logger.info(f"[find_class_implementations|in] ({packages}, {clazz})")
        result = {}
        _packages = [a.strip() for a in packages.split(",")]

        # find classes that extend clazz
        for pack_name in _packages:
            module_class_map = UtilsReflection.find_class_implementations_in_package(pack_name, clazz)
            for mod, _clazz in module_class_map.items():
                impl = mod.split(".")[-1]
                result[impl] = _clazz

        logger.info(f"[find_class_implementations|out] => {result}")
        return result
