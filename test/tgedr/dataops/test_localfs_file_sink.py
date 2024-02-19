import os
import tempfile
from test.conftest import hash_file

from tgedr.dataops.sink.localfs_file_sink import LocalFsFileSink
from tgedr.dataops.source.locafs_filelist_source import LocalFsFileListSource


# @mock_aws
def test_put():
    file = tempfile.NamedTemporaryFile()
    hash = hash_file(file.name)
    folder = tempfile.TemporaryDirectory("+wb")
    target_file = os.path.join(folder.name, "dummy.txt")
    o = LocalFsFileSink()
    o.put(context={"source": file.name, "target": target_file})

    o = LocalFsFileListSource()
    files = o.get({"source": folder.name, "file_suffix": ".txt"})

    assert 1 == len(files)
    assert hash == hash_file(files[0])
    os.remove(file.name)
    folder.cleanup()
