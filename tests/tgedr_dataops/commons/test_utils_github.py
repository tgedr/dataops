import pytest

from tgedr_dataops.commons.utils_github import (
    RAW_HOST,
    GithubReference,
    parse_github_source,
    raw_file_url,
    split_credentials,
)


def test_split_credentials_with_token():
    url = "https://some_token@raw.githubusercontent.com/o/r/main/f.csv"
    actual_url, actual_token = split_credentials(url)
    assert actual_url == "https://raw.githubusercontent.com/o/r/main/f.csv"
    assert actual_token == "some_token"


def test_split_credentials_without_token():
    url = "https://raw.githubusercontent.com/o/r/main/f.csv"
    actual_url, actual_token = split_credentials(url)
    assert actual_url == url
    assert actual_token is None


def test_split_credentials_without_scheme():
    actual_url, actual_token = split_credentials("some_token@raw.githubusercontent.com/o/r/main/f.csv")
    assert actual_url == "raw.githubusercontent.com/o/r/main/f.csv"
    assert actual_token == "some_token"


def test_reference_str_without_ref_and_path():
    assert str(GithubReference(owner="owner", repo="repo")) == "owner/repo"


def test_reference_str_with_ref_and_path():
    ref = GithubReference(owner="owner", repo="repo", ref="main", path="folder/file.csv")
    assert str(ref) == "owner/repo@main/folder/file.csv"


def test_reference_does_not_expose_token():
    ref = GithubReference(owner="owner", repo="repo", token="top_secret")
    assert "top_secret" not in str(ref)
    assert "top_secret" not in repr(ref)


def test_parse_repository_coordinates():
    actual = parse_github_source("owner/repo")
    assert actual == GithubReference(owner="owner", repo="repo")


def test_parse_repository_coordinates_is_trimmed():
    actual = parse_github_source("  owner/repo  ")
    assert actual == GithubReference(owner="owner", repo="repo")


def test_parse_raw_url():
    actual = parse_github_source(f"https://{RAW_HOST}/owner/repo/main/folder/file.csv")
    assert actual == GithubReference(owner="owner", repo="repo", ref="main", path="folder/file.csv", host="github.com")


def test_parse_raw_url_without_scheme():
    actual = parse_github_source(f"{RAW_HOST}/owner/repo/main/file.csv")
    assert actual == GithubReference(owner="owner", repo="repo", ref="main", path="file.csv", host="github.com")


def test_parse_raw_url_with_token():
    actual = parse_github_source(f"https://top_secret@{RAW_HOST}/owner/repo/v1.0/file.csv")
    assert actual == GithubReference(
        owner="owner", repo="repo", ref="v1.0", path="file.csv", token="top_secret", host="github.com"
    )


def test_parse_raw_url_ignores_query_and_fragment():
    actual = parse_github_source(f"https://{RAW_HOST}/owner/repo/main/file.csv?raw=1#L1")
    assert actual == GithubReference(owner="owner", repo="repo", ref="main", path="file.csv", host="github.com")


def test_parse_empty_source():
    with pytest.raises(ValueError, match="cannot be empty"):
        parse_github_source("")


def test_parse_raw_url_without_path():
    with pytest.raises(ValueError, match="raw github source must be"):
        parse_github_source(f"https://{RAW_HOST}/owner/repo/main")


def test_parse_unsupported_source_does_not_leak_token():
    with pytest.raises(ValueError, match="unsupported github source") as error:
        parse_github_source("https://top_secret@github.com/owner/repo/blob/main/file.csv")
    assert "top_secret" not in str(error.value)


def test_raw_file_url():
    actual = raw_file_url("owner", "repo", "main", "folder/file.csv")
    assert actual == f"https://{RAW_HOST}/owner/repo/main/folder/file.csv"


def test_raw_file_url_quotes_segments():
    actual = raw_file_url("owner", "repo", "main", "folder with space/file #1.csv")
    assert actual == f"https://{RAW_HOST}/owner/repo/main/folder%20with%20space/file%20%231.csv"


@pytest.mark.parametrize(
    ("owner", "repo", "ref", "path", "message"),
    [
        ("", "repo", "main", "file.csv", "owner and repo"),
        ("owner", "", "main", "file.csv", "owner and repo"),
        ("owner", "repo", "", "file.csv", "ref must be provided"),
        ("owner", "repo", "main", "", "path must be provided"),
    ],
)
def test_raw_file_url_missing_arguments(owner, repo, ref, path, message):
    with pytest.raises(ValueError, match=message):
        raw_file_url(owner, repo, ref, path)

def test_parse_github_file():
    from tgedr_dataops.commons.utils_github import parse_github_file

    actual = parse_github_file("owner/repo/folder/file.csv")
    assert actual == GithubReference(owner="owner", repo="repo", path="folder/file.csv")


def test_parse_github_file_strips_leading_slash():
    from tgedr_dataops.commons.utils_github import parse_github_file

    assert parse_github_file("owner/repo//data/file.csv").path == "data/file.csv"


def test_parse_github_file_allows_spaces_in_path():
    from tgedr_dataops.commons.utils_github import parse_github_file

    assert parse_github_file("owner/repo/my folder/file.csv").path == "my folder/file.csv"


def test_parse_github_file_empty():
    from tgedr_dataops.commons.utils_github import parse_github_file

    with pytest.raises(ValueError, match="cannot be empty"):
        parse_github_file("")


def test_parse_github_file_missing_path():
    from tgedr_dataops.commons.utils_github import parse_github_file

    with pytest.raises(ValueError, match="missing a path"):
        parse_github_file("owner/repo/")


def test_parse_github_file_malformed():
    from tgedr_dataops.commons.utils_github import parse_github_file

    with pytest.raises(ValueError, match="missing a path"):
        parse_github_file("owner/repo")


def test_parse_github_file_unsupported():
    from tgedr_dataops.commons.utils_github import parse_github_file

    with pytest.raises(ValueError, match="unsupported github file"):
        parse_github_file("ow ner/repo/file.csv")


@pytest.mark.parametrize(
    ("given", "expected"),
    [
        (None, "github.com"),
        ("", "github.com"),
        ("github.com", "github.com"),
        ("  GitHub.com  ", "github.com"),
        ("https://github.com/owner/repo", "github.com"),
        ("https://token@github.com", "github.com"),
        ("octocorp.ghe.com", "octocorp.ghe.com"),
        ("https://api.octocorp.ghe.com/repos", "api.octocorp.ghe.com"),
    ],
)
def test_normalize_host(given, expected):
    from tgedr_dataops.commons.utils_github import normalize_host

    assert normalize_host(given) == expected


@pytest.mark.parametrize("host", ["not a host", "http://", "github", "has space.com"])
def test_normalize_host_invalid(host):
    from tgedr_dataops.commons.utils_github import normalize_host

    with pytest.raises(ValueError, match="invalid github host"):
        normalize_host(host)


@pytest.mark.parametrize(
    ("host", "expected"),
    [
        (None, None),
        ("github.com", None),
        ("octocorp.ghe.com", "octocorp"),
        ("https://octocorp.ghe.com/x", "octocorp"),
    ],
)
def test_enterprise_subdomain(host, expected):
    from tgedr_dataops.commons.utils_github import enterprise_subdomain

    assert enterprise_subdomain(host) == expected


def test_enterprise_subdomain_empty():
    from tgedr_dataops.commons.utils_github import enterprise_subdomain

    with pytest.raises(ValueError, match="invalid github host"):
        enterprise_subdomain(".ghe.com")


@pytest.mark.parametrize(
    ("host", "expected"),
    [
        (None, "https://api.github.com"),
        ("github.com", "https://api.github.com"),
        ("octocorp.ghe.com", "https://api.octocorp.ghe.com"),
        ("https://octocorp.ghe.com", "https://api.octocorp.ghe.com"),
    ],
)
def test_api_base_url(host, expected):
    from tgedr_dataops.commons.utils_github import api_base_url

    assert api_base_url(host) == expected


def test_contents_url_public():
    from tgedr_dataops.commons.utils_github import contents_url

    assert contents_url("owner", "repo", "") == "https://api.github.com/repos/owner/repo/contents"
    assert contents_url("owner", "repo", "a b/c.csv") == "https://api.github.com/repos/owner/repo/contents/a%20b/c.csv"


def test_contents_url_enterprise():
    from tgedr_dataops.commons.utils_github import contents_url

    actual = contents_url("owner", "repo", "/data/", host="octocorp.ghe.com")
    assert actual == "https://api.octocorp.ghe.com/repos/owner/repo/contents/data"


def test_contents_url_requires_owner_and_repo():
    from tgedr_dataops.commons.utils_github import contents_url

    with pytest.raises(ValueError, match="owner and repo"):
        contents_url("", "repo", "data")


def test_parse_ghe_com_source():
    from tgedr_dataops.commons.utils_github import parse_github_source

    actual = parse_github_source("https://octocorp.ghe.com/owner/repo")
    assert actual == GithubReference(owner="owner", repo="repo", host="octocorp.ghe.com")


def test_parse_ghe_com_source_with_token():
    from tgedr_dataops.commons.utils_github import parse_github_source

    actual = parse_github_source("https://secret@octocorp.ghe.com/owner/repo")
    assert actual == GithubReference(owner="owner", repo="repo", token="secret", host="octocorp.ghe.com")


def test_parse_ghe_com_source_without_repo():
    from tgedr_dataops.commons.utils_github import parse_github_source

    with pytest.raises(ValueError, match="ghe.com source must be"):
        parse_github_source("https://octocorp.ghe.com/owner")


class FakeListingResponse:
    def __init__(self, status_code=200, payload=None, link=None):
        self.status_code = status_code
        self.headers = {"Link": link} if link else {}
        self._payload = payload

    def json(self):
        return self._payload


def repo_entry(name: str, **overrides) -> dict:
    entry = {
        "name": name,
        "full_name": f"owner/{name}",
        "private": False,
        "archived": False,
        "disabled": False,
        "fork": False,
        "default_branch": "main",
        "html_url": f"https://github.com/owner/{name}",
        "clone_url": f"https://github.com/owner/{name}.git",
        "description": None,
        "size": 1,
        "stargazers_count": 0,
        "language": "Python",
        "topics": [],
        "pushed_at": "2024-01-01T00:00:00Z",
        "extra_unrelated_key": "dropped",
    }
    entry.update(overrides)
    return entry


@pytest.mark.parametrize(
    ("given", "expected"),
    [
        (None, None),
        ("", None),
        ("<https://api.github.com/x?page=2>; rel=\"next\"", "https://api.github.com/x?page=2"),
        (
            '<https://api.github.com/x?page=5>; rel="last", <https://api.github.com/x?page=2>; rel="next"',
            "https://api.github.com/x?page=2",
        ),
        ('<https://api.github.com/x?page=5>; rel="last"', None),
        ("garbage", None),
        ("; rel=\"next\"", None),
        ("no-brackets; rel=\"next\"", None),
    ],
)
def test_parse_next_link(given, expected):
    from tgedr_dataops.commons.utils_github import parse_next_link

    assert parse_next_link(given) == expected


def test_account_repositories_url_public():
    from tgedr_dataops.commons.utils_github import account_repositories_url

    assert account_repositories_url("owner") == "https://api.github.com/orgs/owner/repos"
    assert account_repositories_url("/owner/") == "https://api.github.com/orgs/owner/repos"
    assert account_repositories_url("owner", for_organizations=False) == "https://api.github.com/users/owner/repos"


def test_account_repositories_url_enterprise():
    from tgedr_dataops.commons.utils_github import account_repositories_url

    assert account_repositories_url("owner", "octocorp.ghe.com") == "https://api.octocorp.ghe.com/orgs/owner/repos"


@pytest.mark.parametrize("account", ["", "  ", "not a name", "has/slash/too/many"])
def test_account_repositories_url_invalid(account):
    from tgedr_dataops.commons.utils_github import account_repositories_url

    with pytest.raises(ValueError, match="invalid github account"):
        account_repositories_url(account)


def test_repository_filters_defaults():
    from tgedr_dataops.commons.utils_github import RepositoryFilters

    filters = RepositoryFilters()
    assert filters.include_archived is True
    assert filters.include_forks is True
    assert filters.include_private is True
    assert filters.include_disabled is False
    assert filters.suffix is None
    assert filters.keeps(repo_entry("a"))


@pytest.mark.parametrize(
    ("filters_kwargs", "entry_kwargs", "expected"),
    [
        ({"include_archived": False}, {"archived": True}, False),
        ({"include_archived": True}, {"archived": True}, True),
        ({"include_forks": False}, {"fork": True}, False),
        ({"include_private": False}, {"private": True}, False),
        ({"include_disabled": False}, {"disabled": True}, False),
        ({"include_disabled": True}, {"disabled": True}, True),
        ({"suffix": ".py"}, {}, True),
        ({"suffix": ".rst"}, {}, False),
    ],
)
def test_repository_filters_keeps(filters_kwargs, entry_kwargs, expected):
    from tgedr_dataops.commons.utils_github import RepositoryFilters

    assert RepositoryFilters(**filters_kwargs).keeps(repo_entry("thing.py", **entry_kwargs)) is expected


def test_list_account_repositories_reduces_metadata(mocker):
    from tgedr_dataops.commons.utils_github import REPOSITORY_METADATA_KEYS, list_account_repositories

    session = mocker.Mock()
    session.get.return_value = FakeListingResponse(payload=[repo_entry("a")])

    actual = list_account_repositories("owner", session=session)

    assert 1 == len(actual)
    assert set(REPOSITORY_METADATA_KEYS) == set(actual[0])
    assert "extra_unrelated_key" not in actual[0]
    assert session.get.call_args.args[0] == "https://api.github.com/orgs/owner/repos"


def test_list_account_repositories_sends_token(mocker):
    from tgedr_dataops.commons.utils_github import GITHUB_AUTH_SCHEME, list_account_repositories

    session = mocker.Mock()
    session.get.return_value = FakeListingResponse(payload=[repo_entry("a")])

    list_account_repositories("owner", token="secret", session=session)

    assert f"{GITHUB_AUTH_SCHEME} secret" == session.get.call_args.kwargs["headers"]["Authorization"]


def test_list_account_repositories_without_token(mocker):
    from tgedr_dataops.commons.utils_github import list_account_repositories

    session = mocker.Mock()
    session.get.return_value = FakeListingResponse(payload=[repo_entry("a")])

    list_account_repositories("owner", session=session)

    assert "Authorization" not in session.get.call_args.kwargs["headers"]


def test_list_account_repositories_follows_pages(mocker):
    from tgedr_dataops.commons.utils_github import list_account_repositories

    session = mocker.Mock()
    session.get.side_effect = [
        FakeListingResponse(
            payload=[repo_entry("a")],
            link='<https://api.github.com/orgs/owner/repos?page=2>; rel="next"',
        ),
        FakeListingResponse(payload=[repo_entry("b", full_name="owner/b")]),
    ]

    actual = list_account_repositories("owner", session=session)

    assert ["owner/a", "owner/b"] == [entry["full_name"] for entry in actual]
    assert 2 == session.get.call_count
    assert session.get.call_args_list[1].args[0] == "https://api.github.com/orgs/owner/repos?page=2"
    assert session.get.call_args_list[1].kwargs["params"] is None


def test_list_account_repositories_falls_back_to_users(mocker):
    from tgedr_dataops.commons.utils_github import list_account_repositories

    session = mocker.Mock()
    session.get.side_effect = [
        FakeListingResponse(status_code=404),
        FakeListingResponse(payload=[repo_entry("a")]),
    ]

    actual = list_account_repositories("someuser", session=session)

    assert ["owner/a"] == [entry["full_name"] for entry in actual]
    assert session.get.call_args_list[1].args[0] == "https://api.github.com/users/someuser/repos"


def test_list_account_repositories_not_found_on_both_endpoints(mocker):
    from tgedr_dataops.commons.utils_github import GithubAccountNotFoundException, list_account_repositories

    session = mocker.Mock()
    session.get.return_value = FakeListingResponse(status_code=404)

    with pytest.raises(GithubAccountNotFoundException, match="could not find"):
        list_account_repositories("ghost", session=session)


def test_list_account_repositories_forbidden(mocker):
    from tgedr_dataops.commons.utils_github import GithubApiException, list_account_repositories

    session = mocker.Mock()
    session.get.return_value = FakeListingResponse(status_code=403)

    with pytest.raises(GithubApiException, match="provide a token"):
        list_account_repositories("owner", session=session)


def test_list_account_repositories_unexpected_status(mocker):
    from tgedr_dataops.commons.utils_github import GithubApiException, list_account_repositories

    session = mocker.Mock()
    session.get.return_value = FakeListingResponse(status_code=500)

    with pytest.raises(GithubApiException, match="status code: 500"):
        list_account_repositories("owner", session=session)


def test_list_account_repositories_request_exception(mocker):
    import requests as requests_module

    from tgedr_dataops.commons.utils_github import GithubApiException, list_account_repositories

    session = mocker.Mock()
    session.get.side_effect = requests_module.RequestException("boom")

    with pytest.raises(GithubApiException, match="could not list"):
        list_account_repositories("owner", session=session)


def test_list_account_repositories_payload_not_a_list(mocker):
    from tgedr_dataops.commons.utils_github import list_account_repositories

    session = mocker.Mock()
    session.get.return_value = FakeListingResponse(payload={"message": "odd"})

    assert [] == list_account_repositories("owner", session=session)


def test_list_account_repositories_applies_filters(mocker):
    from tgedr_dataops.commons.utils_github import RepositoryFilters, list_account_repositories

    session = mocker.Mock()
    session.get.return_value = FakeListingResponse(
        payload=[
            repo_entry("keep"),
            repo_entry("archived", archived=True),
            repo_entry("forked", fork=True),
            repo_entry("private", private=True),
        ]
    )

    actual = list_account_repositories(
        "owner",
        filters=RepositoryFilters(include_archived=False, include_forks=False, include_private=False),
        session=session,
    )

    assert ["owner/keep"] == [entry["full_name"] for entry in actual]


def test_list_account_repositories_enterprise_host(mocker):
    from tgedr_dataops.commons.utils_github import list_account_repositories

    session = mocker.Mock()
    session.get.return_value = FakeListingResponse(payload=[repo_entry("a")])

    list_account_repositories("owner", host="octocorp.ghe.com", session=session)

    assert session.get.call_args.args[0] == "https://api.octocorp.ghe.com/orgs/owner/repos"


def test_list_account_repositories_uses_requests_by_default(mocker):
    from tgedr_dataops.commons.utils_github import list_account_repositories

    get = mocker.patch(
        "tgedr_dataops.commons.utils_github.requests.get",
        return_value=FakeListingResponse(payload=[repo_entry("a")]),
    )

    actual = list_account_repositories("owner")

    assert ["owner/a"] == [entry["full_name"] for entry in actual]
    assert get.call_args.kwargs["timeout"] == 30.0
