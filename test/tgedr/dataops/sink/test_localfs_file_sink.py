import os

from tgedr.dataops.commons.utils_fs import hash_file, temp_dir, temp_file
from tgedr.dataops.sink.local_fs_file_sink import LocalFsFileSink


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
