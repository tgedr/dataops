"""Local filesystem sink implementation for persisting files.

This module provides:
- LocalFsFileSink: sink class for saving/persisting files to local filesystem.
"""

import logging
import shutil
from pathlib import Path
from typing import Any

from tgedr_dataops_abs.sink import Sink, SinkException


logger = logging.getLogger(__name__)


class LocalFsFileSink(Sink):
    """sink class used to save/persist an object/file to a local fs location."""

    CONTEXT_SOURCE_PATH = "source"
    CONTEXT_TARGET_PATH = "target"

    def put(self, context: dict[str, Any] | None = None) -> Any:
        """Copy a file from source to target on local filesystem.

        Parameters
        ----------
        context : dict[str, Any], optional
            Context dictionary containing 'source' and 'target' paths.

        Raises
        ------
        SinkException
            If source or target context is missing.
        """
        logger.info(f"[put|in] ({context})")

        if self.CONTEXT_SOURCE_PATH not in context:
            raise SinkException(f"you must provide context for {self.CONTEXT_SOURCE_PATH}")
        if self.CONTEXT_TARGET_PATH not in context:
            raise SinkException(f"you must provide context for {self.CONTEXT_TARGET_PATH}")

        source = context[self.CONTEXT_SOURCE_PATH]
        target = context[self.CONTEXT_TARGET_PATH]

        shutil.copy(source, target)
        logger.info("[put|out]")

    def delete(self, context: dict[str, Any] | None = None) -> None:
        """Delete a file or directory from local filesystem.

        Parameters
        ----------
        context : dict[str, Any], optional
            Context dictionary containing 'target' path.

        Raises
        ------
        SinkException
            If target context is missing or target is neither file nor directory.
        """
        logger.info(f"[delete|in] ({context})")

        if self.CONTEXT_TARGET_PATH not in context:
            raise SinkException(f"you must provide context for {self.CONTEXT_TARGET_PATH}")

        target = Path(context[self.CONTEXT_TARGET_PATH])

        if target.is_file():
            target.unlink()
        elif target.is_dir():
            shutil.rmtree(target)
        else:
            raise SinkException(f"[delete] is it a dir or a folder? {target}")

        logger.info("[delete|out]")
