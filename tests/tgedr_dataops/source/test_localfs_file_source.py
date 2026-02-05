import shutil

import pytest

from tgedr_dataops.commons.utils_fs import hash_file, temp_dir, temp_file
from tgedr_dataops.source.local_fs_file_source import LocalFsFileSource
from tgedr_dataops_abs.source import SourceException


def test_list():
    src_folder = temp_dir()
    src_file_1 = temp_file(root=src_folder)
    shutil.move(src_file_1, src_file_1 + ".xml")
    src_file_1 += ".xml"
    temp_file(root=src_folder)

    o = LocalFsFileSource()
    files = o.list(context={"source": src_folder})
    assert 2 == len(files)

    files = o.list(context={"source": src_folder, "suffix": ".xml"})
    assert 1 == len(files)


def test_sourcing_one_file():
    src_file_1 = temp_file()
    hash = hash_file(src_file_1)

    o = LocalFsFileSource()
    files = o.list(context={"source": src_file_1})
    assert 1 == len(files)

    target_folder = temp_dir()
    actual = o.get({"files": files, "target": target_folder})
    assert 1 == len(actual)
    assert hash == hash_file(actual[0])


def test_sourcing_multiple_files():
    src_folder = temp_dir()
    src_file_1 = temp_file(root=src_folder)
    src_file_2 = temp_file(root=src_folder)
    hash = hash_file(src_file_1)

    o = LocalFsFileSource()
    files = o.list(context={"source": src_folder})
    assert 2 == len(files)

    target_folder = temp_dir()
    actual = o.get({"files": files, "target": target_folder})
    assert 2 == len(actual)
    assert hash == hash_file(actual[0])


def test_list_missing_source_context():
    o = LocalFsFileSource()
    
    with pytest.raises(SourceException, match="you must provide context for source"):
        o.list(context={})


def test_get_missing_files_context():
    o = LocalFsFileSource()
    
    with pytest.raises(SourceException, match="files and target must be provided"):
        o.get(context={"target": "/tmp"})


def test_get_missing_target_context():
    o = LocalFsFileSource()
    
    with pytest.raises(SourceException, match="files and target must be provided"):
        o.get(context={"files": []})


def test_get_with_single_string_file():
    src_file = temp_file()
    hash = hash_file(src_file)
    
    o = LocalFsFileSource()
    target_folder = temp_dir()
    actual = o.get({"files": src_file, "target": target_folder})
    assert 1 == len(actual)
    assert hash == hash_file(actual[0])


def test_get_with_invalid_files_type():
    o = LocalFsFileSource()
    
    with pytest.raises(SourceException, match="files argument must be a list of strings or a string"):
        o.get({"files": 123, "target": "/tmp"})


def test_get_with_target_file():
    src_file = temp_file()
    hash = hash_file(src_file)
    
    o = LocalFsFileSource()
    target_file = temp_file()
    actual = o.get({"files": [src_file], "target": target_file})
    assert 1 == len(actual)
    assert hash == hash_file(actual[0])

