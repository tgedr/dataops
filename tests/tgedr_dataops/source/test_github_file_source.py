from pathlib import Path

import pytest
import requests

from tgedr_dataops.commons.utils_fs import hash_file, temp_dir, temp_file
from tgedr_dataops.commons.utils_github import RAW_HOST
from tgedr_dataops.source.github_file_source import GithubFileSource
from tgedr_dataops_abs.source import NoSourceException, SourceException

MOCK_GET = "tgedr_dataops.source.github_file_source.requests.get"


class FakeResponse:
    def __init__(self, status_code=200, payload=None, chunks=(b"abc",), error=None):
        self.status_code = status_code
        self.headers = {}
        self._payload = payload
        self._chunks = chunks
        self._error = error

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def json(self):
        return self._payload

    def iter_content(self, chunk_size=8192):  # noqa: ARG002
        if self._error is not None:
            raise self._error
        yield from self._chunks


def file_entry(name: str, folder: str = "folder") -> dict:
    return {"name": name, "path": f"{folder}/{name}", "type": "file", "download_url": None}


def dir_entry(name: str = "sub") -> dict:
    return {"name": name, "path": f"folder/{name}", "type": "dir", "download_url": None}


def test_init_defaults():
    o = GithubFileSource()
    assert o.CONFIG_KEY_AUTH == "token"
    assert o.DEFAULT_BRANCH == "main"
    assert o._timeout == GithubFileSource.DEFAULT_TIMEOUT


def test_init_accepts_int_timeout():
    assert 5.0 == GithubFileSource(config={"timeout": 5})._timeout


@pytest.mark.parametrize("timeout", ["30", 0, -1])
def test_init_invalid_timeout(timeout):
    with pytest.raises(ValueError, match="must be a positive number"):
        GithubFileSource(config={"timeout": timeout})


def test_auth_prefers_config(mocker, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "from_env")
    get = mocker.patch(MOCK_GET, return_value=FakeResponse(payload=[]))

    o = GithubFileSource(config={"token": "from_config", "timeout": 5})
    o.list(context={"source": "owner/repo"})
    assert f"{GithubFileSource.AUTH_SCHEME} from_config" == get.call_args.kwargs["headers"]["Authorization"]


def test_auth_from_github_token_env(mocker, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "from_env")
    monkeypatch.delenv("GH_TOKEN", raising=False)
    get = mocker.patch(MOCK_GET, return_value=FakeResponse(payload=[]))

    o = GithubFileSource()
    o.list(context={"source": "owner/repo"})
    assert f"{GithubFileSource.AUTH_SCHEME} from_env" == get.call_args.kwargs["headers"]["Authorization"]


def test_auth_from_gh_token_env(mocker, monkeypatch):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.setenv("GH_TOKEN", "from_gh_env")
    get = mocker.patch(MOCK_GET, return_value=FakeResponse(payload=[]))

    o = GithubFileSource()
    o.list(context={"source": "owner/repo"})
    assert f"{GithubFileSource.AUTH_SCHEME} from_gh_env" == get.call_args.kwargs["headers"]["Authorization"]


def test_auth_absent(mocker, monkeypatch):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    get = mocker.patch(MOCK_GET, return_value=FakeResponse(payload=[]))

    o = GithubFileSource()
    o.list(context={"source": "owner/repo"})
    assert "Authorization" not in get.call_args.kwargs["headers"]


def test_list_missing_source_context():
    with pytest.raises(SourceException, match="you must provide context for source"):
        GithubFileSource().list(context={})


def test_list_invalid_source():
    with pytest.raises(SourceException, match="unsupported github source"):
        GithubFileSource().list(context={"source": "https://github.com/owner/repo/blob/main/f.csv"})


def test_list_repository_root(mocker):
    get = mocker.patch(
        MOCK_GET,
        return_value=FakeResponse(payload=[file_entry("a.csv"), dir_entry(), file_entry("b.xml")]),
    )

    actual = GithubFileSource().list(context={"source": "owner/repo"})
    assert actual == ["owner/repo/folder/a.csv", "owner/repo/folder/b.xml"]
    assert get.call_args.args[0] == "https://api.github.com/repos/owner/repo/contents"
    assert get.call_args.kwargs["params"] == {"ref": "main"}


def test_list_with_ref_and_path(mocker):
    get = mocker.patch(MOCK_GET, return_value=FakeResponse(payload=[file_entry("a.csv")]))

    GithubFileSource().list(context={"source": "owner/repo", "ref": "v1.2.3", "path": "/data/raw/"})
    assert get.call_args.args[0] == "https://api.github.com/repos/owner/repo/contents/data/raw"
    assert get.call_args.kwargs["params"] == {"ref": "v1.2.3"}


def test_list_accepts_branch_context_key(mocker):
    get = mocker.patch(MOCK_GET, return_value=FakeResponse(payload=[file_entry("a.csv")]))

    GithubFileSource().list(context={"source": "owner/repo", "branch": "dev"})
    assert get.call_args.kwargs["params"] == {"ref": "dev"}


def test_list_raw_url_sets_ref_and_path(mocker):
    get = mocker.patch(MOCK_GET, return_value=FakeResponse(payload=[file_entry("a.csv")]))

    GithubFileSource().list(context={"source": f"https://{RAW_HOST}/owner/repo/feature-x/data"})
    assert get.call_args.args[0] == "https://api.github.com/repos/owner/repo/contents/data"
    assert get.call_args.kwargs["params"] == {"ref": "feature-x"}


def test_list_with_suffix(mocker):
    mocker.patch(
        MOCK_GET,
        return_value=FakeResponse(payload=[file_entry("a.csv"), file_entry("b.xml"), file_entry("c.xml")]),
    )

    actual = GithubFileSource().list(context={"source": "owner/repo", "suffix": ".xml"})
    assert ["owner/repo/folder/b.xml", "owner/repo/folder/c.xml"] == actual


def test_list_uses_reference_token(mocker, monkeypatch):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    get = mocker.patch(MOCK_GET, return_value=FakeResponse(payload=[]))

    o = GithubFileSource()
    o.list(context={"source": f"https://secret_token@{RAW_HOST}/owner/repo/main/data"})
    assert f"{GithubFileSource.AUTH_SCHEME} secret_token" == get.call_args.kwargs["headers"]["Authorization"]


def test_list_reference_token_is_not_persisted(mocker, monkeypatch):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    get = mocker.patch(MOCK_GET, return_value=FakeResponse(payload=[]))

    o = GithubFileSource()
    o.list(context={"source": f"https://secret_token@{RAW_HOST}/owner/repo/main/data"})
    o.list(context={"source": "owner/repo"})
    assert "Authorization" not in get.call_args.kwargs["headers"]


def test_list_error_label_mentions_ref_and_path(mocker):
    mocker.patch(MOCK_GET, return_value=FakeResponse(status_code=404))

    with pytest.raises(NoSourceException, match=r"owner/repo@main/data$"):
        GithubFileSource().list(context={"source": "owner/repo", "ref": "main", "path": "data"})


def test_list_single_file_payload(mocker):
    payload = {"name": "a.csv", "path": "data/a.csv", "type": "file"}
    mocker.patch(MOCK_GET, return_value=FakeResponse(payload=payload))

    actual = GithubFileSource().list(context={"source": "owner/repo", "path": "data/a.csv"})
    assert actual == ["owner/repo/data/a.csv"]


def test_list_request_exception(mocker):
    mocker.patch(MOCK_GET, side_effect=requests.RequestException("boom"))

    with pytest.raises(SourceException, match="could not list owner/repo"):
        GithubFileSource().list(context={"source": "owner/repo"})


def test_list_not_found(mocker):
    mocker.patch(MOCK_GET, return_value=FakeResponse(status_code=404))

    with pytest.raises(NoSourceException, match="could not find"):
        GithubFileSource().list(context={"source": "owner/repo", "ref": "main"})


def test_list_forbidden_mentions_token(mocker):
    mocker.patch(MOCK_GET, return_value=FakeResponse(status_code=403))

    with pytest.raises(SourceException, match="provide a token"):
        GithubFileSource().list(context={"source": "owner/repo"})


def test_list_unexpected_status(mocker):
    mocker.patch(MOCK_GET, return_value=FakeResponse(status_code=500))

    with pytest.raises(SourceException, match="status code: 500"):
        GithubFileSource().list(context={"source": "owner/repo"})


def test_get_missing_files_context():
    with pytest.raises(SourceException, match="you must provide context for files"):
        GithubFileSource().get(context={"target": "/tmp"})


def test_get_missing_target_context():
    with pytest.raises(SourceException, match="you must provide context for target"):
        GithubFileSource().get(context={"files": []})


def test_get_invalid_files_type():
    with pytest.raises(SourceException, match="files argument must be a list of strings or a string"):
        GithubFileSource().get({"files": 123, "target": "/tmp"})


def test_get_rejects_malformed_coordinate():
    with pytest.raises(SourceException, match="unsupported github file"):
        GithubFileSource().get({"files": "https://example.com/f.csv", "target": temp_dir()})


def test_get_rejects_coordinate_without_path():
    with pytest.raises(SourceException, match="missing a path"):
        GithubFileSource().get({"files": "owner/repo", "target": temp_dir()})


def test_get_refuses_several_files_into_a_file_target():
    with pytest.raises(SourceException, match="target must be a folder"):
        GithubFileSource().get({"files": ["owner/repo/a.csv", "owner/repo/b.csv"], "target": temp_file()})


def test_get_one_file_into_folder(mocker):
    content = b"col1,col2\n1,2\n"
    get = mocker.patch(MOCK_GET, return_value=FakeResponse(chunks=(content,)))

    target = temp_dir()
    actual = GithubFileSource().get({"files": "owner/repo/data/a.csv", "target": target})

    assert 1 == len(actual)
    assert actual[0] == f"{target}/a.csv"
    with Path(actual[0]).open("rb") as handle:
        assert content == handle.read()
    assert get.call_args.args[0] == f"https://{RAW_HOST}/owner/repo/main/data/a.csv"
    assert get.call_args.kwargs["stream"] is True


def test_get_defaults_to_main_branch(mocker):
    get = mocker.patch(MOCK_GET, return_value=FakeResponse(chunks=(b"payload",)))

    GithubFileSource().get({"files": "owner/repo/a.csv", "target": temp_dir()})
    assert get.call_args.args[0] == f"https://{RAW_HOST}/owner/repo/main/a.csv"


def test_get_uses_branch_context_key(mocker):
    get = mocker.patch(MOCK_GET, return_value=FakeResponse(chunks=(b"payload",)))

    GithubFileSource().get({"files": "owner/repo/a.csv", "branch": "v1.2.3", "target": temp_dir()})
    assert get.call_args.args[0] == f"https://{RAW_HOST}/owner/repo/v1.2.3/a.csv"


def test_get_accepts_branch_with_slashes(mocker):
    get = mocker.patch(MOCK_GET, return_value=FakeResponse(chunks=(b"payload",)))

    GithubFileSource().get({"files": "owner/repo/data/a.csv", "branch": "feature/x", "target": temp_dir()})
    assert get.call_args.args[0] == f"https://{RAW_HOST}/owner/repo/feature%2Fx/data/a.csv"


def test_get_accepts_nested_path(mocker):
    get = mocker.patch(MOCK_GET, return_value=FakeResponse(chunks=(b"payload",)))

    actual = GithubFileSource().get({"files": "owner/repo/a/b/c.csv", "branch": "dev", "target": temp_dir()})

    assert get.call_args.args[0] == f"https://{RAW_HOST}/owner/repo/dev/a/b/c.csv"
    assert Path(actual[0]).name == "c.csv"


def test_get_accepts_a_list_of_files(mocker):
    get = mocker.patch(MOCK_GET, return_value=FakeResponse(chunks=(b"payload",)))

    target = temp_dir()
    actual = GithubFileSource().get({"files": ["owner/repo/a.csv", "owner/repo/b.csv"], "target": target})

    assert 2 == len(actual)
    assert 2 == get.call_count


def test_get_creates_missing_parent_folders(mocker):
    mocker.patch(MOCK_GET, return_value=FakeResponse(chunks=(b"payload",)))

    target = f"{temp_dir()}/deep/folder/"
    actual = GithubFileSource().get({"files": "owner/repo/a.csv", "target": target})

    assert 1 == len(actual)
    assert hash_file(actual[0])


def test_get_uses_token_from_config(mocker):
    get = mocker.patch(MOCK_GET, return_value=FakeResponse(chunks=(b"payload",)))

    GithubFileSource(config={"token": "from_config"}).get({"files": "owner/repo/a.csv", "target": temp_dir()})

    assert f"{GithubFileSource.AUTH_SCHEME} from_config" == get.call_args.kwargs["headers"]["Authorization"]


def test_get_removes_partial_file_on_failure(mocker):
    mocker.patch(MOCK_GET, return_value=FakeResponse(error=requests.RequestException("boom")))
    target = temp_dir()

    with pytest.raises(SourceException, match="could not download"):
        GithubFileSource().get({"files": "owner/repo/a.csv", "target": target})

    assert [] == list(Path(target).iterdir())


def test_get_raises_when_file_not_found(mocker):
    mocker.patch(MOCK_GET, return_value=FakeResponse(status_code=404))
    target = temp_dir()

    with pytest.raises(NoSourceException, match="could not find"):
        GithubFileSource().get({"files": "owner/repo/a.csv", "target": target})

    assert [] == list(Path(target).iterdir())


def test_list_then_get(mocker):
    mocker.patch(
        MOCK_GET,
        side_effect=[
            FakeResponse(payload=[file_entry("a.csv"), file_entry("b.csv")]),
            FakeResponse(chunks=(b"one",)),
            FakeResponse(chunks=(b"two",)),
        ],
    )

    o = GithubFileSource()
    files = o.list(context={"source": "owner/repo"})
    assert ["owner/repo/folder/a.csv", "owner/repo/folder/b.csv"] == files

    target = temp_dir()
    actual = o.get({"files": files, "target": target})
    assert 2 == len(actual)
    assert {f"{target}/a.csv", f"{target}/b.csv"} == set(actual)

def test_init_invalid_host():
    with pytest.raises(ValueError, match="invalid github host"):
        GithubFileSource(config={"host": "not a host"})


def test_public_host_uses_api_github_com(mocker):
    get = mocker.patch(MOCK_GET, return_value=FakeResponse(payload=[]))

    GithubFileSource().list(context={"source": "owner/repo"})
    assert get.call_args.args[0] == "https://api.github.com/repos/owner/repo/contents"


def test_enterprise_host_uses_dedicated_api_subdomain(mocker):
    get = mocker.patch(MOCK_GET, return_value=FakeResponse(payload=[]))

    GithubFileSource(config={"host": "octocorp.ghe.com"}).list(context={"source": "owner/repo"})
    assert get.call_args.args[0] == "https://api.octocorp.ghe.com/repos/owner/repo/contents"


def test_enterprise_host_from_source_url(mocker):
    get = mocker.patch(MOCK_GET, return_value=FakeResponse(payload=[]))

    GithubFileSource().list(context={"source": "https://octocorp.ghe.com/owner/repo"})
    assert get.call_args.args[0] == "https://api.octocorp.ghe.com/repos/owner/repo/contents"


def test_config_host_wins_over_source_url(mocker):
    get = mocker.patch(MOCK_GET, return_value=FakeResponse(payload=[]))

    GithubFileSource(config={"host": "other.ghe.com"}).list(context={"source": "https://octocorp.ghe.com/owner/repo"})
    assert get.call_args.args[0] == "https://api.other.ghe.com/repos/owner/repo/contents"


def test_list_returns_coordinates_for_enterprise(mocker):
    mocker.patch(MOCK_GET, return_value=FakeResponse(payload=[file_entry("a.csv")]))

    actual = GithubFileSource(config={"host": "octocorp.ghe.com"}).list(context={"source": "owner/repo"})
    assert actual == ["owner/repo/folder/a.csv"]


def test_public_get_uses_raw_host(mocker):
    get = mocker.patch(MOCK_GET, return_value=FakeResponse(chunks=(b"payload",)))

    GithubFileSource().get({"files": "owner/repo/data/a.csv", "branch": "dev", "target": temp_dir()})
    assert get.call_args.args[0] == f"https://{RAW_HOST}/owner/repo/dev/data/a.csv"
    assert get.call_args.kwargs["headers"]["Accept"] == "application/vnd.github+json"


def test_public_get_can_use_api_download(mocker):
    get = mocker.patch(MOCK_GET, return_value=FakeResponse(chunks=(b"payload",)))

    GithubFileSource(config={"use_api_download": True}).get(
        {"files": "owner/repo/data/a.csv", "branch": "dev", "target": temp_dir()}
    )
    assert get.call_args.args[0] == "https://api.github.com/repos/owner/repo/contents/data/a.csv?ref=dev"
    assert get.call_args.kwargs["headers"]["Accept"] == "application/vnd.github.raw"


def test_enterprise_get_uses_contents_api_raw_media_type(mocker):
    get = mocker.patch(MOCK_GET, return_value=FakeResponse(chunks=(b"payload",)))

    GithubFileSource(config={"host": "octocorp.ghe.com"}).get(
        {"files": "owner/repo/data/a.csv", "branch": "feature/x", "target": temp_dir()}
    )
    assert get.call_args.args[0] == "https://api.octocorp.ghe.com/repos/owner/repo/contents/data/a.csv?ref=feature%2Fx"
    assert get.call_args.kwargs["headers"]["Accept"] == "application/vnd.github.raw"
