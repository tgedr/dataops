"""Filesystem utility functions for file and path operations.

This module provides utilities for:
- Creating temporary files and directories
- Parsing and processing S3 URLs and paths
- Extracting URL protocols
- Generating file hashes
"""
import tempfile
import re
import hashlib
from pathlib import Path


def temp_dir(root: str | None = None, suffix: str | None = None, prefix: str | None = None) -> str:
    """Create a temporary directory and return its path.

    Parameters
    ----------
    root : str | None
        Directory where the temporary directory will be created.
    suffix : str | None
        Suffix for the temporary directory name.
    prefix : str | None
        Prefix for the temporary directory name.

    Returns
    -------
    str
        Path to the created temporary directory.
    """
    return tempfile.mkdtemp(suffix=suffix, prefix=prefix, dir=root)


def temp_file(
    root: str | None = None, suffix: str | None = None, prefix: str | None = None,
    discard_handle: bool = True
) -> str | tuple[int, str]:
    """Create a temporary file and return its path or handle and path.

    Parameters
    ----------
    root : str | None
        Directory where the temporary file will be created.
    suffix : str | None
        Suffix for the temporary file name.
    prefix : str | None
        Prefix for the temporary file name.
    discard_handle : bool
        If True, return only the file path. If False, return tuple of (handle, path).

    Returns
    -------
    str | tuple[int, str]
        File path if discard_handle is True, otherwise tuple of (file handle, file path).
    """
    h, f = tempfile.mkstemp(suffix=suffix, prefix=prefix, dir=root)
    if discard_handle:
        return f
    return (h, f)


def resolve_url_protocol(url: str) -> str:
    """Extract the protocol from a URL.

    Parameters
    ----------
    url : str
        The URL to extract the protocol from.

    Returns
    -------
    str
        The protocol (e.g., 'http://', 'https://') or None if no protocol is found.
    """
    result = None
    group_match = re.search("(.*://).*", url)
    if group_match is not None:
        result = group_match.group(1)
    return result


def resolve_s3_protocol(url: str) -> str:
    """Extract the S3 protocol from a URL.

    Parameters
    ----------
    url : str
        The URL to extract the S3 protocol from.

    Returns
    -------
    str
        The S3 protocol (e.g., 's3://', 's3a://') or None if no S3 protocol is found.
    """
    result = None
    group_match = re.search("(s3[a]?://).*", url)
    if group_match is not None:
        result = group_match.group(1)
    return result


def remove_s3_protocol(url: str) -> str:
    """Remove the S3 protocol prefix from a URL.

    Parameters
    ----------
    url : str
        The S3 URL to remove the protocol from.

    Returns
    -------
    str
        The URL without the S3 protocol prefix (s3:// or s3a://).
    """
    if url.startswith("s3://"):
        result = url[5:]
    elif url.startswith("s3a://"):
        result = url[6:]
    else:
        result = url
    return result


def process_s3_path(path: str) -> tuple[str, str]:
    """Extract bucket and key from an S3 path.

    Parameters
    ----------
    path : str
        The S3 path to process (with or without protocol).

    Returns
    -------
    tuple[str, str]
        A tuple containing (bucket, key).
    """
    no_protocol_path = remove_s3_protocol(path)
    path_elements = no_protocol_path.split("/")
    bucket = path_elements[0]
    key = "/".join(path_elements[1:])
    return (bucket, key)


def process_s3_url(url: str) -> tuple[str, str, str]:
    """Extract protocol, bucket, and key from an S3 URL.

    Parameters
    ----------
    url : str
        The S3 URL to process.

    Returns
    -------
    tuple[str, str, str]
        A tuple containing (protocol, bucket, key).
    """
    protocol = resolve_s3_protocol(url)
    no_protocol_url = remove_s3_protocol(url)
    path_elements = no_protocol_url.split("/")
    bucket = path_elements[0]
    key = "/".join(path_elements[1:])
    return ("" if protocol is None else protocol, bucket, key)


def hash_file(filepath: str, hash_func=hashlib.sha256) -> str:  # noqa: ANN001
    """Generate a hash for a file.

    Args:
        filepath (str): The path to the file.
        hash_func: A hashlib hash function, e.g., hashlib.md5().

    Returns:
        str: The hexadecimal hash string of the file.
    """
    # Initialize the hash object
    hasher = hash_func()

    # Open the file in binary read mode
    with Path(filepath).open("rb") as file:
        # Read the file in chunks to avoid using too much memory
        chunk_size = 8192
        while chunk := file.read(chunk_size):
            hasher.update(chunk)

    # Return the hexadecimal digest of the hash
    return hasher.hexdigest()
