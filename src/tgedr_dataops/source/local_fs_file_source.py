"""Local filesystem source implementation for file operations.

This module provides:
- LocalFsFileSource: A source class for retrieving and listing local files.
"""
import logging
import shutil
from pathlib import Path
from typing import Any

from tgedr_dataops_abs.source import Source, SourceException


logger = logging.getLogger(__name__)


class LocalFsFileSource(Source):
    """source class used to retrieve local objects/files to a another local fs location."""

    CONTEXT_KEY_SOURCE = "source"
    CONTEXT_KEY_TARGET = "target"
    CONTEXT_KEY_SUFFIX = "suffix"
    CONTEXT_KEY_FILES = "files"

    def list(self, context: dict[str, Any] | None = None) -> list[str]:
        """List files in the local filesystem directory.

        Parameters
        ----------
        context : dict[str, Any], optional
            Context dictionary containing 'source' path.

        Returns
        -------
        list[str]
            List of file paths in the source directory.

        Raises
        ------
        SourceException
            If source context is missing or if source path is not a directory.
        """
        logger.info(f"[list|in] ({context})")
        result: list[str] = []
        if self.CONTEXT_KEY_SOURCE not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_SOURCE}")

        source = context[self.CONTEXT_KEY_SOURCE]
        source_path = Path(source)
        if source_path.is_dir():
            suffix = None
            if self.CONTEXT_KEY_SUFFIX in context:
                suffix = context[self.CONTEXT_KEY_SUFFIX]
                result: list[str] = [str(file) for file in source_path.iterdir() if file.name.endswith(suffix)]
            else:
                result: list[str] = [str(file) for file in source_path.iterdir()]
        elif source_path.is_file():
            result: list[str] = [source]

        logger.debug(f"[list|out] => {result}")
        logger.info(f"[list|out] => result len: {len(result)}")
        return result

    def get(self, context: dict[str, Any] | None = None) -> Any:
        """Retrieve file(s) from local filesystem.

        Parameters
        ----------
        context : dict[str, Any], optional
            Context dictionary containing 'source' path.

        Returns
        -------
        Any
            List of file paths if source is a directory, or single file path if source is a file.

        Raises
        ------
        SourceException
            If source context is missing.
        """
        logger.info(f"[get|in] ({context})")

        if self.CONTEXT_KEY_FILES not in context or self.CONTEXT_KEY_TARGET not in context:
            raise SourceException(f"{self.CONTEXT_KEY_FILES} and {self.CONTEXT_KEY_TARGET} must be provided in config")
        files = context[self.CONTEXT_KEY_FILES]
        target = context[self.CONTEXT_KEY_TARGET]

        if "list" != type(files).__name__:
            if "str" == type(files).__name__:
                files = [files]
            else:
                raise SourceException("files argument must be a list of strings or a string")

        target_is_dir: bool = False
        if Path(target).is_dir():
            target_is_dir = True

        result: list[str] = []

        for file in files:
            basename = Path(file).name
            new_file = str(Path(target) / basename) if target_is_dir else target
            shutil.copy(file, new_file)
            result.append(new_file)

        logger.info("[get|out] => {result}")
        return result
