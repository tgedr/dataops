"""Github file source implementation for retrieving files from public repositories.

This module provides:
- GithubFileSource: A source class for listing and downloading files from github repositories.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, ClassVar
from urllib.parse import quote

import requests

from tgedr_dataops.commons.utils_github import (
    GithubReference,
    api_base_url,
    contents_url,
    enterprise_subdomain,
    normalize_host,
    format_github_file,
    parse_github_file,
    parse_github_source,
    raw_file_url,
)
from tgedr_dataops_abs.source import NoSourceException, Source, SourceException


logger = logging.getLogger(__name__)


class GithubFileSource(Source):
    """source class used to retrieve files from a public github repository to a local fs location."""

    CONTEXT_KEY_SOURCE = "source"
    CONTEXT_KEY_FILES = "files"
    CONTEXT_KEY_TARGET = "target"
    CONTEXT_KEY_SUFFIX = "suffix"
    CONTEXT_KEY_REF = "ref"
    CONTEXT_KEY_BRANCH = "branch"
    CONTEXT_KEY_PATH = "path"

    CONFIG_KEY_AUTH = "token"
    CONFIG_KEY_TIMEOUT = "timeout"
    CONFIG_KEY_HOST = "host"
    CONFIG_KEY_USE_API_DOWNLOAD = "use_api_download"

    ENV_GITHUB_AUTH: ClassVar[str] = "GITHUB_TOKEN"
    ENV_GH_AUTH: ClassVar[str] = "GH_TOKEN"

    AUTH_SCHEME: ClassVar[str] = "Bearer"
    DEFAULT_BRANCH: ClassVar[str] = "main"
    DEFAULT_TIMEOUT: ClassVar[float] = 30.0
    CHUNK_SIZE: ClassVar[int] = 8192

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the GithubFileSource.

        Parameters
        ----------
        config : dict[str, Any], optional
            Configuration dictionary accepting optional 'token', 'timeout' and 'host' keys. Use
            'host' to target GitHub Enterprise Cloud with data residency, e.g. '<organization>.ghe.com'.

        Raises
        ------
        ValueError
            If the configured timeout is not a positive number or the host is not a valid domain.
        """
        super().__init__(config=config)

        config = config or {}
        configured_timeout = config.get(self.CONFIG_KEY_TIMEOUT, self.DEFAULT_TIMEOUT)
        if not isinstance(configured_timeout, int | float) or configured_timeout <= 0:
            raise ValueError(f"[__init__] '{self.CONFIG_KEY_TIMEOUT}' must be a positive number, got: {configured_timeout!r}")

        self._auth: str | None = config.get(self.CONFIG_KEY_AUTH) or None
        self._timeout: float = float(configured_timeout)

        configured_host = config.get(self.CONFIG_KEY_HOST)
        self._host_is_configured: bool = isinstance(configured_host, str) and bool(configured_host.strip())
        self._config_host: str = normalize_host(configured_host if self._host_is_configured else None)

        self._use_api_download: bool = bool(config.get(self.CONFIG_KEY_USE_API_DOWNLOAD, False))

    @property
    def _api_base(self) -> str:
        """Resolve the REST API base url of the configured host.

        Returns
        -------
        str
            REST API base url.
        """
        return api_base_url(self._config_host)

    def _resolve_host(self, reference: GithubReference) -> str:
        """Resolve the host to use for a single operation.

        An explicitly configured host always wins. Otherwise the host carried by the source url is
        used, falling back to the default public host. The result is never stored on the instance,
        so a source url cannot leak its host into later calls.

        Parameters
        ----------
        reference : GithubReference
            Reference parsed from the context.

        Returns
        -------
        str
            Host to use for this operation.
        """
        if self._host_is_configured:
            return self._config_host
        return normalize_host(reference.host) if reference.host is not None else self._config_host

    @property
    def _effective_auth(self) -> str | None:
        """Resolve the token to use, falling back to the environment.

        Returns
        -------
        str | None
            Token from config or from the environment, None when unavailable.
        """
        if self._auth is not None:
            return self._auth
        return os.getenv(self.ENV_GITHUB_AUTH) or os.getenv(self.ENV_GH_AUTH) or None

    def list(self, context: dict[str, Any] | None = None) -> list[str]:
        """List files of a github repository folder.

        Parameters
        ----------
        context : dict[str, Any], optional
            Context dictionary containing 'source' and optionally 'ref', 'branch', 'path' and 'suffix'.

        Returns
        -------
        list[str]
            List of '<owner>/<repo>@<ref>/<path>' coordinates, directly usable by 'get'.

        Raises
        ------
        SourceException
            If 'source' is missing, malformed or the repository/folder cannot be reached.
        """
        if self.CONTEXT_KEY_SOURCE not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_SOURCE}")

        try:
            reference = parse_github_source(context[self.CONTEXT_KEY_SOURCE])
        except ValueError as e:
            raise SourceException(str(e)) from e

        host = self._resolve_host(reference)

        logger.info(f"[list|in] {reference}")

        ref = (
            context.get(self.CONTEXT_KEY_REF)
            or context.get(self.CONTEXT_KEY_BRANCH)
            or reference.ref
            or self.DEFAULT_BRANCH
        )
        path = context.get(self.CONTEXT_KEY_PATH, reference.path) or ""

        entries = self._list_entries(reference, ref, path, reference.token, host)

        files = [entry for entry in entries if entry["type"] == "file"]
        if self.CONTEXT_KEY_SUFFIX in context:
            suffix: str = context[self.CONTEXT_KEY_SUFFIX]
            files = [entry for entry in files if entry["path"].endswith(suffix)]

        result = [
            format_github_file(reference.owner, reference.repo, entry["path"], ref) for entry in files
        ]

        logger.debug(f"[list|out] => {result}")
        logger.info(f"[list|out] => result len: {len(result)}")
        return result

    def get(self, context: dict[str, Any] | None = None) -> Any:
        """Download files from a github repository to the local filesystem.

        Parameters
        ----------
        context : dict[str, Any], optional
            Context dictionary containing 'files' and 'target' (local path). Each file is either a
            '<owner>/<repo>/<path>' or '<owner>/<repo>@<ref>/<path>' coordinate; the optional 'branch'
            context key is used for coordinates that do not pin a ref (defaults to 'main').

        Returns
        -------
        Any
            List of local file paths.

        Raises
        ------
        SourceException
            If 'files' or 'target' are missing, have an invalid type, hold a malformed coordinate
            or if multiple files are requested for a single file target.
        """
        if self.CONTEXT_KEY_FILES not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_FILES}")
        if self.CONTEXT_KEY_TARGET not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_TARGET}")

        files = context[self.CONTEXT_KEY_FILES]
        if not isinstance(files, list):
            if isinstance(files, str):
                files = [files]
            else:
                raise SourceException("files argument must be a list of strings or a string")

        target = context[self.CONTEXT_KEY_TARGET]
        target_is_dir: bool = Path(target).is_dir()
        if not target_is_dir and len(files) > 1:
            raise SourceException(f"target must be a folder when retrieving several files, got: {target}")

        default_branch = context.get(self.CONTEXT_KEY_BRANCH) or self.DEFAULT_BRANCH
        logger.info(f"[get|in] files: {files} branch: {default_branch} target: {target}")

        parsed: list[tuple[GithubReference, str]] = []
        for file in files:
            try:
                reference = parse_github_file(file)
            except ValueError as e:
                raise SourceException(str(e)) from e
            parsed.append((reference, reference.ref or default_branch))

        names = self._local_names(parsed) if target_is_dir else {}
        result: list[str] = []
        for index, (reference, branch) in enumerate(parsed):
            local_file = str(Path(target) / names[index]) if target_is_dir else target

            self._download(self._download_url(reference, branch), reference.token or self._effective_auth, local_file)
            result.append(local_file)

        logger.info(f"[get|out] => {result}")
        return result

    def _local_names(self, parsed: list[tuple[GithubReference, str]]) -> dict[int, str]:
        """Resolve the local file name of each coordinate inside a target folder.

        Coordinates from different repositories often share the same file name (every repo has a
        'README.md'), which would make them overwrite each other. Names are kept as-is when they are
        unique, otherwise each colliding entry is prefixed as '<account>_<repo>_<name>', e.g.
        'jtviegas_airesdeornelas_README.md'.

        Parameters
        ----------
        parsed : list[tuple[GithubReference, str]]
            Coordinates and the branch resolved for each of them.

        Returns
        -------
        dict[int, str]
            Local file name per index of 'parsed'.
        """
        basenames = [Path(reference.path).name for reference, _ in parsed]
        counts: dict[str, int] = {}
        for name in basenames:
            counts[name] = counts.get(name, 0) + 1

        names: dict[int, str] = {}
        used: set[str] = set()
        for index, ((reference, _), name) in enumerate(zip(parsed, basenames, strict=True)):
            candidate = name
            if counts[name] > 1:
                candidate = f"{reference.owner}_{reference.repo}_{name}"
                # the same repository can hold a colliding name twice (e.g. 'a/README.md' and 'b/README.md')
                if candidate in used:
                    candidate = f"{reference.owner}_{reference.repo}_{reference.path.replace('/', '_')}"
            names[index] = candidate
            used.add(candidate)
        return names

    def _download_url(self, reference: GithubReference, ref: str) -> str:
        """Resolve the url used to download a file.

        The Contents API is used whenever the source is not the public github.com host, because
        'raw.githubusercontent.com' has no documented equivalent on ghe.com. The API is also used
        for the public host when explicitly requested through the 'use_api_download' config key.

        Parameters
        ----------
        reference : GithubReference
            File reference holding owner, repo and path.
        ref : str
            Branch, tag or commit sha.

        Returns
        -------
        str
            Url to download the file from, without credentials.
        """
        if self._use_api_download or enterprise_subdomain(self._config_host) is not None:
            url = contents_url(reference.owner, reference.repo, reference.path, self._config_host)
            return f"{url}?ref={quote(ref, safe='')}"
        return raw_file_url(reference.owner, reference.repo, ref, reference.path)

    def _list_entries(
        self, reference: GithubReference, ref: str, path: str, auth: str | None = None, host: str | None = None
    ) -> list[dict[str, Any]]:
        """Retrieve the contents api entries of a repository folder.

        Parameters
        ----------
        reference : GithubReference
            Repository reference.
        ref : str
            Branch, tag or commit sha.
        path : str
            Folder path inside the repository, empty for the repository root.
        auth : str | None
            Token used as authorization header, if any.
        host : str | None
            Github host for this operation, defaults to the configured one.

        Returns
        -------
        list[dict[str, Any]]
            Contents api entries.

        Raises
        ------
        SourceException
            If the repository or folder cannot be reached.
        """
        url = contents_url(reference.owner, reference.repo, path, host or self._config_host)

        label = f"{reference.owner}/{reference.repo}@{ref}/{path}"
        try:
            with requests.get(url, params={"ref": ref}, headers=self._headers(auth), timeout=self._timeout) as response:
                self._raise_for_status(response, label)
                payload = response.json()
        except requests.RequestException as e:
            raise SourceException(f"could not list {label}") from e

        entries = payload if isinstance(payload, list) else [payload]
        for entry in entries:
            if "download_url" not in entry:
                entry["download_url"] = raw_file_url(reference.owner, reference.repo, ref, entry.get("path", path))
        return entries

    def _download(self, url: str, auth: str | None, local_file: str) -> None:
        """Download a raw github url to a local file.

        Parameters
        ----------
        url : str
            Raw file url, without credentials.
        auth : str | None
            Token used as authorization header, if any.
        local_file : str
            Local path where the file will be written.

        Raises
        ------
        SourceException
            If the download fails.
        """
        local = Path(local_file)
        try:
            local.parent.mkdir(parents=True, exist_ok=True)
            headers = self._headers(auth, raw=url.startswith(self._api_base))
            with requests.get(url, headers=headers, timeout=self._timeout, stream=True) as response:
                self._raise_for_status(response, url)
                with local.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=self.CHUNK_SIZE):
                        handle.write(chunk)
        except requests.RequestException as e:
            local.unlink(missing_ok=True)
            raise SourceException(f"could not download {url}") from e

    def _headers(self, auth: str | None = None, raw: bool = False) -> dict[str, str]:
        """Build the request headers.

        Parameters
        ----------
        auth : str | None
            Token used as authorization header, if any.
        raw : bool
            Request the raw file body instead of json, used with the contents api.

        Returns
        -------
        dict[str, str]
            Request headers.
        """
        headers: dict[str, str] = {"Accept": "application/vnd.github.raw" if raw else "application/vnd.github+json"}
        auth = auth or self._effective_auth
        if auth is not None:
            headers["Authorization"] = f"{self.AUTH_SCHEME} {auth}"
        return headers

    @staticmethod
    def _raise_for_status(response: requests.Response, target: str) -> None:
        """Raise a source exception when the response is not a success.

        Parameters
        ----------
        response : requests.Response
            Response to check.
        target : str
            Target being retrieved, used in the error message.

        Raises
        ------
        NoSourceException
            If the target was not found.
        SourceException
            If the target could not be retrieved.
        """
        if response.status_code == requests.codes.not_found:
            raise NoSourceException(f"could not find: {target}")
        if response.status_code == requests.codes.forbidden:
            raise SourceException(
                f"access forbidden to {target} - if this is a rate limit, provide a token with the '{GithubFileSource.CONFIG_KEY_AUTH}' config"
            )
        if response.status_code != requests.codes.ok:
            raise SourceException(f"could not retrieve {target} - status code: {response.status_code}")
