import shutil

from tgedr.dataops.commons.utils_fs import hash_file, temp_dir, temp_file
from tgedr.dataops.source.local_fs_file_source import LocalFsFileSource


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
