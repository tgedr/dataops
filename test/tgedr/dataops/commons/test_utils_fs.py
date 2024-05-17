import os

from tgedr.dataops.commons.utils_fs import (
    hash_file,
    process_s3_path,
    remove_s3_protocol,
    resolve_s3_protocol,
    resolve_url_protocol,
    temp_dir,
    temp_file,
)


def test_temp_dir_with_no_root():
    o = temp_dir()
    assert os.path.isdir(o)


def test_temp_dir_with_root():
    o = temp_dir(root=(os.path.sep + "tmp"))
    assert o.startswith((os.path.sep + "tmp")) and o != (os.path.sep + "tmp")
    assert os.path.isdir(o)


def test_temp_dir_with_root_2():
    o = temp_dir(root="/tmp")
    assert o.startswith((os.path.sep + "tmp")) and o != (os.path.sep + "tmp")
    assert os.path.isdir(o)


def test_temp_dir_with_suffix():
    o = temp_dir(suffix=".tmp")
    assert o.endswith(".tmp") and o != (".tmp")
    assert os.path.isdir(o)


def test_temp_dir_with_suffix_2():
    o = temp_dir(suffix="tmp")
    assert o.endswith("tmp") and o != ("tmp")
    assert os.path.isdir(o)


def test_temp_dir_with_suffix_and_root():
    o = temp_dir(root=(os.path.sep + "tmp"), suffix="pmt")
    assert o.startswith((os.path.sep + "tmp")) and o.endswith("pmt") and o != ((os.path.sep + "tmp") + "pmt")
    assert os.path.isdir(o)


def test_temp_file():
    f = temp_file()
    assert os.path.isfile(f)


def test_resolve_url_protocol():
    assert "http://" == resolve_url_protocol("http://mybucket/mykey")
    assert "https://" == resolve_url_protocol("https://mybucket/mykey")
    assert resolve_url_protocol("mybucket/mykey") is None


def test_resolve_s3_protocol():
    assert "s3a://" == resolve_s3_protocol("s3a://mybucket/mykey")
    assert "s3://" == resolve_s3_protocol("s3://mybucket/mykey")
    assert resolve_s3_protocol("mybucket/mykey") is None


def test_remove_s3_protocol():
    assert "mybucket/mykey" == remove_s3_protocol("s3a://mybucket/mykey")
    assert "mybucket/mykey" == remove_s3_protocol("s3://mybucket/mykey")
    assert "mybucket/mykey" == remove_s3_protocol("mybucket/mykey")


def test_happy_path():
    assert ("bucket", "rootkey/keyone") == process_s3_path("s3://bucket/rootkey/keyone")


def test_no_key_path():
    assert ("bucket", "") == process_s3_path("s3://bucket")


def test_no_protocol_no_key_path():
    assert "bucket", "" == process_s3_path("bucket")


def test_hash_file():
    # files created are equal in content so we can test its hash
    assert hash_file(temp_file()) == hash_file(temp_file())
