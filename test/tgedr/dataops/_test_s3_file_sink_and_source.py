import os
import tempfile
from test.conftest import hash_file

from tgedr.dataops.sink.s3_file_sink import S3FileSink
from tgedr.dataops.source.s3_file_source import S3FileSource


def test_put(resources_folder):
    local_sink_file = tempfile.NamedTemporaryFile().name
    source_file = os.path.join(resources_folder, "s3_files", "dummy.txt")
    hash = hash_file(source_file)

    o = S3FileSink()
    o.put(context={"target": "faersdataset-dev-landing/tmp/dummy", "source": source_file})

    u = S3FileSource()
    u.get(context={"source": "faersdataset-dev-landing/tmp/dummy", "target": local_sink_file})

    assert hash == hash_file(local_sink_file)
