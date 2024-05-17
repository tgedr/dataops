import tempfile
import re
from typing import Tuple, Union, AnyStr
import hashlib


def temp_dir(root: str = None, suffix: str = None, prefix: str = None) -> str:
    return tempfile.mkdtemp(suffix=suffix, prefix=prefix, dir=root)


def temp_file(
    root: str = None, suffix: str = None, prefix: str = None, discard_handle: bool = True
) -> Union[str, Tuple[int, str]]:
    h, f = tempfile.mkstemp(suffix=suffix, prefix=prefix, dir=root)
    if discard_handle:
        return f
    else:
        return (h, f)


def resolve_url_protocol(url: str) -> str:
    result = None
    group_match = re.search("(.*://).*", url)
    if group_match is not None:
        result = group_match.group(1)
    return result


def resolve_s3_protocol(url: str) -> str:
    result = None
    group_match = re.search("(s3[a]?://).*", url)
    if group_match is not None:
        result = group_match.group(1)
    return result


def remove_s3_protocol(url: str) -> str:
    if url.startswith("s3://"):
        result = url[5:]
    elif url.startswith("s3a://"):
        result = url[6:]
    else:
        result = url
    return result


def process_s3_path(path: str) -> Tuple[str, str]:
    no_protocol_path = remove_s3_protocol(path)
    path_elements = no_protocol_path.split("/")
    bucket = path_elements[0]
    key = "/".join(path_elements[1:])
    return (bucket, key)


def hash_file(filepath, hash_func=hashlib.sha256) -> AnyStr:
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
    with open(filepath, "rb") as file:
        # Read the file in chunks to avoid using too much memory
        chunk_size = 8192
        while chunk := file.read(chunk_size):
            hasher.update(chunk)

    # Return the hexadecimal digest of the hash
    return hasher.hexdigest()
