import os
import tempfile
from test.conftest import hash_file

from tgedr.dataops.sink.localfs_file_sink import LocalFsFileSink
from tgedr.dataops.source.locafs_file_source import LocalFsFileSource


def test_put():
    file = tempfile.NamedTemporaryFile()
    hash = hash_file(file.name)
    folder = tempfile.TemporaryDirectory("+wb")
    target_file = os.path.join(folder.name, "dummy.txt")
    o = LocalFsFileSink()
    o.put(context={"source": file.name, "target": target_file})

    folder2 = tempfile.TemporaryDirectory("+wb")
    o = LocalFsFileSource()
    files = o.get({"source": folder.name, "file_suffix": ".txt", "target": folder2.name})

    assert 1 == len(files)
    assert hash == hash_file(files[0])
    os.remove(file.name)
    folder.cleanup()
