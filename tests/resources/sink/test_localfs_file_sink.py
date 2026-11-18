import os

import pytest

from tgedr_dataops.commons.utils_fs import hash_file, temp_dir, temp_file
from tgedr_dataops.sink.local_fs_file_sink import LocalFsFileSink
from tgedr_dataops_abs.sink import SinkException


def test_put_file_in_target_file_and_delete():
    src_file = temp_file()
    hash = hash_file(src_file)

    dst_folder = temp_dir()
    dst_file = os.path.join(dst_folder, "dummy.txt")

    s = LocalFsFileSink()
    s.put(context={"source": src_file, "target": dst_file})

    actual = [os.path.join(dst_folder, f) for f in os.listdir(dst_folder)]
    assert 1 == len(actual)
    assert hash == hash_file(actual[0])

    s.delete({"target": actual[0]})
    actual = [f for f in os.listdir(dst_folder)]
    assert 0 == len(actual)


def test_put_file_in_target_folder_and_delete():
    src_file = temp_file()
    hash = hash_file(src_file)

    dst_folder = temp_dir()

    s = LocalFsFileSink()
    s.put(context={"source": src_file, "target": dst_folder})

    actual = [os.path.join(dst_folder, f) for f in os.listdir(dst_folder)]
    assert 1 == len(actual)
    assert hash == hash_file(actual[0])

    s.delete({"target": actual[0]})
    actual = [f for f in os.listdir(dst_folder)]
    assert 0 == len(actual)

    """
    files_found = o.list({"source": dst_folder, "file_suffix": ".txt"})
    files = o.get({"files": files_found, "target": dst_folder2})

    assert 1 == len(files_found)
    assert hash == hash_file(files[0])

    s.delete({"target": files[0]})
    folder3 = tempfile.TemporaryDirectory("+wb")
    files = o.list({"source": folder2.name, "file_suffix": ".txt"})
    files = o.get({"files": files, "target": folder3.name})
    assert 0 == len(files)

    folder.cleanup()
    """


def test_delete_directory():
    dst_folder = temp_dir()
    temp_file(root=dst_folder)  # create file inside
    
    s = LocalFsFileSink()
    s.delete({"target": dst_folder})
    
    assert not os.path.exists(dst_folder)


def test_put_missing_source_context():
    s = LocalFsFileSink()
    
    with pytest.raises(SinkException, match="you must provide context for source"):
        s.put(context={"target": "/tmp/test"})


def test_put_missing_target_context():
    s = LocalFsFileSink()
    
    with pytest.raises(SinkException, match="you must provide context for target"):
        s.put(context={"source": "/tmp/test"})


def test_delete_missing_target_context():
    s = LocalFsFileSink()
    
    with pytest.raises(SinkException, match="you must provide context for target"):
        s.delete(context={})


def test_delete_invalid_path():
    s = LocalFsFileSink()
    
    with pytest.raises(SinkException, match="is it a dir or a folder"):
        s.delete({"target": "/nonexistent/path/that/does/not/exist"})

