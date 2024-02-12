import os
import tempfile
from test.conftest import hash_file

from tgedr.dataops.sink.localfs_file_sink import LocalFsFileSink


# @mock_aws
def test_put():
    file = tempfile.NamedTemporaryFile()
    hash = hash_file(file.name)
    folder = tempfile.TemporaryDirectory("+wb")
    target_file = os.path.join(folder.name, "dummy.txt")
    o = LocalFsFileSink()
    o.put(context={"source": file.name, "target": target_file})
    assert hash == hash_file(target_file)
    os.remove(file.name)
    folder.cleanup()
