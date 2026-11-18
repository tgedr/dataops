"""GitHub utility functions for referencing files on public repositories.

This module provides utilities for:
- Parsing GitHub repository coordinates and raw file urls
- Building raw file urls and contents api urls
- Resolving github.com and ghe.com api hosts
- Listing the repositories of a github account or organization
- Splitting credentials out of urls so secrets are never logged
"""
import logging
import re
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote

import requests

logger = logging.getLogger(__name__)

RAW_HOST = "raw.githubusercontent.com"
PUBLIC_HOST = "github.com"
PUBLIC_API_HOST = "api.github.com"
GHE_SUFFIX = ".ghe.com"
RAW_SCHEME = "https://"
DEFAULT_REF = "HEAD"
GITHUB_AUTH_SCHEME = "Bearer"
REF_SUFFIX = "?ref="
_PER_PAGE = 100
_NOT_FOUND = 404
_FORBIDDEN = 403
_OK = 200


class GithubApiException(Exception):
    """Exception raised when a github api call fails."""


class GithubAccountNotFoundException(GithubApiException):
    """Exception raised when a github account or organization is not found."""

_URL_RE = re.compile(r"^(?:(?P<scheme>https?)://)?(?:(?P<userinfo>[^@/]+)@)?(?P<host>[^/]+)/(?P<rest>.*)$")
_REPOSITORY_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_SHORT_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_HOST_RE = re.compile(r"^(?:[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?\.)+[A-Za-z]{2,}$")


def normalize_host(host: str | None) -> str:
    """Normalize a github host.

    Accepts a bare host or a full url and returns the bare host, defaulting to the public
    github host when nothing is given.

    Parameters
    ----------
    host : str | None
        Host or url to normalize.

    Returns
    -------
    str
        Bare host, e.g. 'github.com' or '<organization>.ghe.com'.

    Raises
    ------
    ValueError
        If the host is not a valid domain name.
    """
    if not host:
        return PUBLIC_HOST

    candidate = host.strip().lower()
    candidate = candidate.split("://")[-1].split("/")[0].split("@")[-1]
    if not _HOST_RE.match(candidate):
        raise ValueError(f"invalid github host: {_scrub(candidate)}")

    return candidate


def enterprise_subdomain(host: str | None) -> str | None:
    """Extract the enterprise subdomain of a ghe.com host.

    Parameters
    ----------
    host : str | None
        Github host, e.g. '<organization>.ghe.com'.

    Returns
    -------
    str | None
        The enterprise subdomain, or None for the public github host.
    """
    normalized = normalize_host(host)
    if not normalized.endswith(GHE_SUFFIX):
        return None

    subdomain = normalized[: -len(GHE_SUFFIX)]
    return subdomain or None


def api_base_url(host: str | None = None) -> str:
    """Build the REST API base url for a github host.

    For github.com this is 'https://api.github.com'. For GitHub Enterprise Cloud with data
    residency it is 'https://api.<subdomain>.ghe.com'.

    Parameters
    ----------
    host : str | None
        Github host. Defaults to the public github host.

    Returns
    -------
    str
        REST API base url without a trailing slash.
    """
    subdomain = enterprise_subdomain(host)
    if subdomain is None:
        return f"{RAW_SCHEME}{PUBLIC_API_HOST}"
    return f"{RAW_SCHEME}api.{subdomain}{GHE_SUFFIX}"


@dataclass(frozen=True)
class GithubReference:
    """Reference to a file (or a repository) on github.

    Attributes
    ----------
    owner : str
        Repository owner (user or organization).
    repo : str
        Repository name.
    ref : str | None
        Branch, tag or commit sha. None means the repository default branch.
    path : str | None
        Path of the file inside the repository. None means the repository root.
    token : str | None
        Optional token extracted from an url userinfo. Never printed/represented.
    host : str | None
        Host the reference points at, e.g. 'github.com' or '<organization>.ghe.com'.
    """

    owner: str
    repo: str
    ref: str | None = None
    path: str | None = None
    token: str | None = field(default=None, repr=False)
    host: str | None = None

    def __str__(self) -> str:
        """Return a readable reference without any credential.

        Returns
        -------
        str
            Reference in the form 'owner/repo[@ref][/path]'.
        """
        ref = f"@{self.ref}" if self.ref else ""
        path = f"/{self.path}" if self.path else ""
        return f"{self.owner}/{self.repo}{ref}{path}"


def split_credentials(url: str) -> tuple[str, str | None]:
    """Split the userinfo (token) out of an url.

    Parameters
    ----------
    url : str
        Url that may embed a token as userinfo, e.g. 'https://token@host/path'.

    Returns
    -------
    tuple[str, str | None]
        A tuple containing (url without userinfo, token or None).
    """
    match = _URL_RE.match(url.strip())
    if match is None or match.group("userinfo") is None:
        return url, None

    scheme = f"{match.group('scheme')}://" if match.group("scheme") else ""
    return f"{scheme}{match.group('host')}/{match.group('rest')}", match.group("userinfo")


def _scrub(value: str) -> str:
    """Return the value without any embedded credential.

    Parameters
    ----------
    value : str
        Value to scrub.

    Returns
    -------
    str
        Value without userinfo.
    """
    return split_credentials(value)[0]


def parse_github_source(source: str) -> GithubReference:
    """Parse a github source into a reference.

    Accepted forms are:
    - 'owner/repo'
    - 'raw.githubusercontent.com/owner/repo/ref/path/to/file'
    - 'https://raw.githubusercontent.com/owner/repo/ref/path/to/file'
    - 'https://<token>@raw.githubusercontent.com/owner/repo/ref/path/to/file'

    Parameters
    ----------
    source : str
        Source string to parse.

    Returns
    -------
    GithubReference
        Parsed reference.

    Raises
    ------
    ValueError
        If the source does not match any accepted form.
    """
    value = (source or "").strip()
    value = value.split("#", 1)[0].split("?", 1)[0]
    if not value:
        raise ValueError("github source cannot be empty")

    match = _URL_RE.match(value)
    if match is not None:
        host = match.group("host").lower()
        segments = [segment for segment in match.group("rest").split("/") if segment]
        if host == RAW_HOST:
            if len(segments) < 4:
                raise ValueError(
                    f"raw github source must be '{RAW_HOST}/<owner>/<repo>/<ref>/<path>', got: {_scrub(value)}"
                )
            return GithubReference(
                owner=segments[0],
                repo=segments[1],
                ref=segments[2],
                path="/".join(segments[3:]),
                token=match.group("userinfo"),
                host=PUBLIC_HOST,
            )
        if host.endswith(GHE_SUFFIX):
            if len(segments) < 2:
                raise ValueError(f"ghe.com source must be '{host}/<owner>/<repo>', got: {_scrub(value)}")
            return GithubReference(
                owner=segments[0],
                repo=segments[1],
                token=match.group("userinfo"),
                host=normalize_host(host),
            )

    if _REPOSITORY_RE.match(value):
        owner, repo = value.split("/")
        return GithubReference(owner=owner, repo=repo)

    raise ValueError(
        f"unsupported github source: {_scrub(value)} - use '<owner>/<repo>' or "
        f"'{RAW_HOST}/<owner>/<repo>/<ref>/<path>' or '<organization>.ghe.com/<owner>/<repo>'"
    )


def format_github_file(owner: str, repo: str, path: str, ref: str | None = None) -> str:
    """Format a file coordinate, optionally pinned to a ref.

    The ref is appended as a '?ref=' suffix, so refs containing slashes stay unambiguous.

    Parameters
    ----------
    owner : str
        Repository owner (user or organization).
    repo : str
        Repository name.
    path : str
        Path of the file inside the repository.
    ref : str | None
        Optional branch, tag or commit sha.

    Returns
    -------
    str
        Coordinate in the form '<owner>/<repo>/<path>' or '<owner>/<repo>/<path>?ref=<ref>'.

    Raises
    ------
    ValueError
        If owner, repo or path are missing.
    """
    if not owner or not repo:
        raise ValueError("owner and repo must be provided")
    if not path:
        raise ValueError("path must be provided")

    clean_path = path.strip("/")
    if not clean_path:
        raise ValueError("path must be provided")

    pinned = f"{REF_SUFFIX}{ref.strip()}" if ref and ref.strip() else ""
    return f"{owner}/{repo}/{clean_path}{pinned}"


def parse_github_file(value: str) -> GithubReference:
    """Parse a file coordinate of the form 'owner/repo/path/to/file'.

    An optional '?ref=<ref>' suffix pins the coordinate to a branch, tag or commit sha. Keeping the
    ref out of the path keeps branch names containing slashes unambiguous.

    Parameters
    ----------
    value : str
        File coordinate to parse.

    Returns
    -------
    GithubReference
        Parsed reference, with 'ref' set when the coordinate carries one.

    Raises
    ------
    ValueError
        If the value is not a valid file coordinate.
    """
    candidate = (value or "").strip()
    if not candidate:
        raise ValueError("github file coordinate cannot be empty")

    coordinate, _, ref = candidate.partition(REF_SUFFIX)
    ref = ref.strip() or None
    if candidate.count(REF_SUFFIX) > 1 or (REF_SUFFIX in candidate and ref is None):
        raise ValueError(f"unsupported github file: {_scrub(candidate)} - use '<owner>/<repo>/<path>[?ref=<ref>]'")

    segments = coordinate.split("/", 2)
    if len(segments) < 2 or not _SHORT_NAME_RE.match(segments[0]) or not _SHORT_NAME_RE.match(segments[1]):
        raise ValueError(f"unsupported github file: {_scrub(candidate)} - use '<owner>/<repo>/<path>[?ref=<ref>]'")
    if len(segments) < 3:
        raise ValueError(f"github file coordinate is missing a path: {_scrub(candidate)}")

    owner, repo, path = segments[0], segments[1], segments[2].strip("/")
    if not path:
        raise ValueError(f"github file coordinate is missing a path: {_scrub(candidate)}")

    return GithubReference(owner=owner, repo=repo, ref=ref, path=path)


def contents_url(owner: str, repo: str, path: str, host: str | None = None) -> str:
    """Build the REST API contents url of a repository path.

    Parameters
    ----------
    owner : str
        Repository owner (user or organization).
    repo : str
        Repository name.
    path : str
        Path inside the repository, empty for the repository root.
    host : str | None
        Github host. Defaults to the public github host.

    Returns
    -------
    str
        Contents API url, without any credential.

    Raises
    ------
    ValueError
        If owner or repo are missing.
    """
    if not owner or not repo:
        raise ValueError("owner and repo must be provided")

    url = f"{api_base_url(host)}/repos/{quote(owner, safe='')}/{quote(repo, safe='')}/contents"
    if path:
        url = f"{url}/{quote(path.strip('/'), safe='/')}"
    return url


def raw_file_url(owner: str, repo: str, ref: str, path: str) -> str:
    """Build the raw url of a file in a github repository.

    Parameters
    ----------
    owner : str
        Repository owner (user or organization).
    repo : str
        Repository name.
    ref : str
        Branch, tag, commit sha or 'HEAD'.
    path : str
        Path of the file inside the repository.

    Returns
    -------
    str
        Raw url of the file, without any credential.

    Raises
    ------
    ValueError
        If owner, repo, ref or path are missing.
    """
    if not owner or not repo:
        raise ValueError("owner and repo must be provided")
    if not ref:
        raise ValueError("ref must be provided")
    if not path:
        raise ValueError("path must be provided")

    segments = [quote(segment, safe="") for segment in path.split("/") if segment]
    return (
        f"{RAW_SCHEME}{RAW_HOST}"
        f"/{quote(owner, safe='')}/{quote(repo, safe='')}/{quote(ref, safe='')}/{'/'.join(segments)}"
    )


REPOSITORY_METADATA_KEYS = (
    "full_name",
    "name",
    "private",
    "archived",
    "disabled",
    "fork",
    "default_branch",
    "html_url",
    "clone_url",
    "description",
    "size",
    "stargazers_count",
    "language",
    "topics",
    "pushed_at",
)


@dataclass(frozen=True)
class RepositoryFilters:
    """Filters applied when listing the repositories of a github account.

    Attributes
    ----------
    suffix : str | None
        Only keep repositories whose name ends with this suffix.
    include_archived : bool
        Keep archived repositories.
    include_forks : bool
        Keep forked repositories.
    include_private : bool
        Keep private repositories.
    include_disabled : bool
        Keep disabled repositories.
    """

    suffix: str | None = None
    include_archived: bool = True
    include_forks: bool = True
    include_private: bool = True
    include_disabled: bool = False

    def keeps(self, entry: dict[str, Any]) -> bool:
        """Check whether a repository entry passes the filters.

        Parameters
        ----------
        entry : dict[str, Any]
            Raw repository entry from the API.

        Returns
        -------
        bool
            True when the entry should be kept.
        """
        if not self.include_archived and entry.get("archived"):
            return False
        if not self.include_forks and entry.get("fork"):
            return False
        if not self.include_private and entry.get("private"):
            return False
        if not self.include_disabled and entry.get("disabled"):
            return False
        return self.suffix is None or str(entry.get("name", "")).endswith(self.suffix)


def parse_next_link(link_header: str | None) -> str | None:
    """Extract the 'next' url from a github Link header.

    Parameters
    ----------
    link_header : str | None
        Value of the 'Link' response header.

    Returns
    -------
    str | None
        The url of the next page, or None when there is no next page.
    """
    if not link_header:
        return None

    for part in link_header.split(","):
        segments = part.split(";")
        if len(segments) < 2:
            continue
        url = segments[0].strip()
        if not url.startswith("<") or not url.endswith(">"):
            continue
        if any(segment.strip() == 'rel="next"' for segment in segments[1:]):
            return url[1:-1]

    return None


def account_repositories_url(account: str, host: str | None = None, for_organizations: bool = True) -> str:
    """Build the url listing the repositories of a github account.

    Parameters
    ----------
    account : str
        Account or organization name.
    host : str | None
        Github host. Defaults to the public github host.
    for_organizations : bool
        Use the organization endpoint instead of the user endpoint. The organization endpoint is
        the only one returning private repositories when a token is provided.

    Returns
    -------
    str
        Repositories listing url, without any credential.

    Raises
    ------
    ValueError
        If the account name is empty or invalid.
    """
    candidate = (account or "").strip().strip("/")
    if not candidate or not _SHORT_NAME_RE.match(candidate):
        raise ValueError(f"invalid github account: {_scrub(candidate)}")

    kind = "orgs" if for_organizations else "users"
    return f"{api_base_url(host)}/{kind}/{quote(candidate, safe='')}/repos"


def list_account_repositories(
    account: str,
    token: str | None = None,
    host: str | None = None,
    timeout: float = 30.0,
    filters: RepositoryFilters | None = None,
    session: Any | None = None,
) -> list[dict[str, Any]]:
    """List the repositories of a github account or organization.

    The user and organization endpoints return the same '<owner>/<repo>' coordinates, so both are
    accepted. The organization endpoint is tried first because it is the only one that also returns
    private repositories when a token is provided; missing accounts fall back to the user endpoint.

    Every page is followed until the last one, and each entry is reduced to the keys in
    REPOSITORY_METADATA_KEYS.

    Parameters
    ----------
    account : str
        Account or organization name.
    token : str | None
        Token used as authorization header, if any.
    host : str | None
        Github host. Defaults to the public github host.
    timeout : float
        Request timeout in seconds.
    filters : RepositoryFilters | None
        Optional filters applied to the listed repositories.
    session : Any | None
        Optional object exposing a 'get' method, used to perform the calls (useful for testing).

    Returns
    -------
    list[dict[str, Any]]
        Repository metadata dictionaries, one per repository.

    Raises
    ------
    GithubAccountNotFoundException
        If the account or organization is not found.
    GithubApiException
        If the repositories cannot be listed.
    """
    active_filters = filters or RepositoryFilters()
    headers: dict[str, str] = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"{GITHUB_AUTH_SCHEME} {token}"

    try:
        result = _collect_repositories(
            account_repositories_url(account, host, for_organizations=True),
            headers,
            timeout,
            active_filters,
            session,
        )
    except GithubAccountNotFoundException:
        logger.info(f"[list_account_repositories] '{_scrub(account)}' is not an organization, trying as a user")
        result = _collect_repositories(
            account_repositories_url(account, host, for_organizations=False),
            headers,
            timeout,
            active_filters,
            session,
        )

    logger.info(f"[list_account_repositories|out] => result len: {len(result)}")
    return result


def _collect_repositories(
    url: str,
    headers: dict[str, str],
    timeout: float,
    filters: RepositoryFilters,
    session: Any | None,
) -> list[dict[str, Any]]:
    """Collect and filter repositories across all pages of a listing url.

    Parameters
    ----------
    url : str
        First page url.
    headers : dict[str, str]
        Request headers.
    timeout : float
        Request timeout in seconds.
    filters : RepositoryFilters
        Filters applied to the listed repositories.
    session : Any | None
        Optional object exposing a 'get' method, used to perform the calls.

    Returns
    -------
    list[dict[str, Any]]
        Repository metadata dictionaries.

    Raises
    ------
    GithubAccountNotFoundException
        If the listing url is not found.
    GithubApiException
        If the repositories cannot be listed.
    """
    http = session or requests
    result: list[dict[str, Any]] = []
    next_url: str | None = url
    params: dict[str, Any] | None = {"per_page": _PER_PAGE, "type": "all"}

    while next_url is not None:
        try:
            response = http.get(next_url, params=params, headers=headers, timeout=timeout)
        except requests.RequestException as e:
            raise GithubApiException(f"could not list {_scrub(next_url)}") from e

        _raise_for_listing_status(response, next_url)

        payload = response.json()
        entries = payload if isinstance(payload, list) else []
        result.extend(
            {key: entry[key] for key in REPOSITORY_METADATA_KEYS if key in entry}
            for entry in entries
            if filters.keeps(entry)
        )

        next_url = parse_next_link(response.headers.get("Link"))
        params = None

    return result


def _raise_for_listing_status(response: Any, url: str) -> None:
    """Raise the proper error when a repository listing response is not a success.

    Parameters
    ----------
    response : Any
        Response to check.
    url : str
        Listing url, used in error messages.

    Raises
    ------
    GithubAccountNotFoundException
        If the listing url is not found.
    GithubApiException
        If the repositories cannot be listed.
    """
    if response.status_code == _NOT_FOUND:
        raise GithubAccountNotFoundException(f"could not find: {_scrub(url)}")
    if response.status_code == _FORBIDDEN:
        raise GithubApiException(f"access forbidden to {_scrub(url)} - if this is a rate limit, provide a token")
    if response.status_code != _OK:
        raise GithubApiException(f"could not list {_scrub(url)} - status code: {response.status_code}")
