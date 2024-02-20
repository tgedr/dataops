import os
import tempfile
from test.conftest import hash_file

from tgedr.dataops.sink.s3_file_sink import S3FileSink
from tgedr.dataops.source.s3_file_source import S3FileSource


def test_put_file_get_file(resources_folder):
    tmp_folder = tempfile.TemporaryDirectory("+wb").name
    if not os.path.exists(tmp_folder):
        os.mkdir(tmp_folder)
    local_sink_file = os.path.join(tmp_folder, "dummy.txt")

    source_file = os.path.join(resources_folder, "s3_files", "dummy.txt")
    hash = hash_file(source_file)

    target_file = "faersdataset-dev-landing/tmp/dummy.txt"

    o = S3FileSink()
    o.put(context={"source": source_file, "target": target_file})

    u = S3FileSource()
    u.get(context={"source": target_file, "target": local_sink_file})

    assert hash == hash_file(local_sink_file)


def test_put_in_folder_get_to_file(resources_folder):
    tmp_folder = tempfile.TemporaryDirectory("+wb").name
    if not os.path.exists(tmp_folder):
        os.mkdir(tmp_folder)
    local_sink_file = os.path.join(tmp_folder, "dummy.txt")

    source_file = os.path.join(resources_folder, "s3_files", "dummy.txt")
    hash = hash_file(source_file)

    target_key = "faersdataset-dev-landing/tmp/"

    o = S3FileSink()
    o.put(context={"target": target_key, "source": source_file})

    u = S3FileSource()
    u.get(context={"source": target_key, "target": local_sink_file})

    assert hash == hash_file(local_sink_file)


def test_put_in_folder_get_in_folder(resources_folder):
    tmp_folder = tempfile.TemporaryDirectory("+wb").name
    if not os.path.exists(tmp_folder):
        os.mkdir(tmp_folder)
    local_sink_file = os.path.join(tmp_folder, "tmp", "dummy.txt")
    local_sink_file2 = os.path.join(tmp_folder, "tmp", "dummy2.txt")

    source_folder = os.path.join(resources_folder, "s3_files")
    hash = hash_file(os.path.join(source_folder, "dummy.txt"))
    hash2 = hash_file(os.path.join(source_folder, "dummy2.txt"))

    target_key = "faersdataset-dev-landing/tmp/"

    o = S3FileSink()
    o.put(context={"target": target_key, "source": source_folder})

    u = S3FileSource()
    u.get(context={"source": target_key, "target": tmp_folder})

    assert hash == hash_file(local_sink_file)
    assert hash2 == hash_file(local_sink_file2)
