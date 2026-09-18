"""Contracted Hugging Face file-based store.

This module provides the ContractedHFDatasetFileBasedStore, a store that wraps the
HuggingFaceDatasetFileBasedStore and enforces a data contract (as defined in an
``.odcs.yaml`` file) by validating data with a GreatExpectationsValidation
implementation before reading, saving, or updating data.
"""

import logging
from pathlib import Path
from typing import Any, ClassVar
import pandas as pd
import yaml
from huggingface_hub import hf_hub_download, upload_file

from tgedr_dataops_abs.great_expectations_validation import GreatExpectationsValidation, ValidationError
from tgedr_dataops_abs.store import NoStoreException

from tgedr_dataops.store.hf_dataset import DataFrameSplits
from tgedr_dataops.store.hf_dataset_file_based import HuggingFaceDatasetFileBasedStore
from tgedr_dataops.quality.pandas_validation import PandasValidation

logger = logging.getLogger(__name__)


class HuggingFaceDatasetContractMixin:
    """Reusable data-contract helpers for shared Hugging Face datasets.

    Provides helpers to load a contract, translate its ``customProperties`` into a
    Great Expectations expectations dict, download/store a contract in a dataset
    repository, and validate a DataFrame against the contract. Intended to be
    composed into a store class (Open-Closed: extend without modifying the base).
    """

    __CONTRACT_EXTENSION: ClassVar[str] = ".odcs.yaml"
    __EXPECTATIONS_PROPERTY: ClassVar[str] = "expectations"

    def _contract_file_name(self, key: str) -> str:
        """Return the contract file name for the given dataset key.

        Parameters
        ----------
        key : str
            Dataset identifier (e.g. "owner/dataset").

        Returns
        -------
        str
            Contract file name (e.g. "dataset.odcs.yaml").
        """
        dataset_name = key.rsplit("/", 1)[-1]
        return f"{dataset_name}{self.__CONTRACT_EXTENSION}"

    def _load_contract_yaml(self, source: str | Path | dict[str, Any]) -> dict[str, Any]:
        """Load a data contract from a YAML file or reuse an already-parsed dict.

        Parameters
        ----------
        source : str | Path | dict
            A YAML file path or an already-parsed contract dict.

        Returns
        -------
        dict
            The parsed data contract.
        """
        if isinstance(source, dict):
            return source
        path = Path(source)
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _expectations_from_contract(self, contract: dict[str, Any]) -> dict[str, Any]:
        """Translate the contract's ``customProperties`` into an expectations dict.

        The ``customProperties`` attribute is a list of ``{"property", "value"}``
        entries. The entry whose ``property`` equals "expectations" holds the Great
        Expectations dictionary (with ``expectation_suite_name`` and ``expectations``)
        expected by ``GreatExpectationsValidation.validate``.

        Parameters
        ----------
        contract : dict
            The parsed data contract.

        Returns
        -------
        dict
            Great Expectations expectations dict.

        Raises
        ------
        ValidationError
            If the contract has no "expectations" custom property.
        """
        custom_properties = contract.get("customProperties", [])
        for prop in custom_properties:
            if prop.get("property") == self.__EXPECTATIONS_PROPERTY:
                return prop["value"]
        raise ValidationError(
            f"data contract is missing the '{self.__EXPECTATIONS_PROPERTY}' customProperty"
        )

    def _download_contract(self, key: str) -> dict[str, Any]:
        """Download and parse the data contract stored in the dataset repository.

        Parameters
        ----------
        key : str
            Dataset identifier (e.g. "owner/dataset").

        Returns
        -------
        dict
            The parsed data contract.

        Raises
        ------
        NoStoreException
            If the contract file is not found in the dataset repository.
        """
        logger.info(f"[_download_contract|in] (key={key})")
        file_name = self._contract_file_name(key)
        try:
            local_path = hf_hub_download(repo_id=key, filename=file_name, repo_type="dataset")
        except Exception as exc:
            raise NoStoreException(
                f"data contract '{file_name}' not found in dataset '{key}' on Hugging Face Hub."
            ) from exc
        contract = self._load_contract_yaml(local_path)
        logger.info(f"[_download_contract|out] => contract keys: {sorted(contract.keys())}")
        return contract

    def _store_contract(self, key: str, contract: dict[str, Any]) -> None:
        """Store a data contract in the dataset repository.

        Parameters
        ----------
        key : str
            Dataset identifier (e.g. "owner/dataset").
        contract : dict
            The data contract to serialize and upload.
        """
        logger.info(f"[_store_contract|in] (key={key})")
        file_name = self._contract_file_name(key)
        raw = yaml.safe_dump(contract, sort_keys=False)
        upload_file(path_or_fileobj=raw.encode("utf-8"), path_in_repo=file_name, repo_id=key, repo_type="dataset")
        logger.info("[_store_contract|out]")

    def _validate_df(self, df: pd.DataFrame, expectations: dict[str, Any], context: str) -> None:
        """Validate a DataFrame against the Great Expectations expectations.

        Raises ``ValidationError`` when validation fails (fail fast).

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to validate.
        expectations : dict
            Great Expectations expectations dict.
        context : str
            Human-readable context for error messages (e.g. "owner/dataset/train").
        """
        logger.info(f"[_validate_df|in] (context={context}, shape={df.shape})")
        result = self._validation.validate(df, expectations)
        if not result.get("success"):
            failures = result.get("results", [])
            raise ValidationError(f"data for '{context}' failed validation: {failures}")
        logger.info("[_validate_df|out]")


class ContractedHFDatasetFileBasedStore(HuggingFaceDatasetFileBasedStore, HuggingFaceDatasetContractMixin):
    """Hugging Face file-based store enforcing a data contract on read/save/update.

    Extends HuggingFaceDatasetFileBasedStore by validating data against a data
    contract defined in ``{dataset_name}.odcs.yaml`` stored in the dataset
    repository. Reads validate every split; saves/updates validate the incoming
    DataFrame before persisting.
    """

    __CONTRACT_SPLITS: ClassVar[tuple[str, ...]] = ("train", "test", "validation")

    def __init__(
        self,
        config: dict[str, Any] | None = None,
        validation: GreatExpectationsValidation | None = None,
    ) -> None:
        """Initialize the contracted store.

        Parameters
        ----------
        config : dict[str, Any] | None
            Configuration dictionary for the store.
        validation : GreatExpectationsValidation | None
            Validation implementation. Defaults to PandasValidation.
        """
        super().__init__(config=config)
        self._validation: GreatExpectationsValidation = validation or PandasValidation()

    def _resolve_contract(self, key: str, data_contract: str | Path | dict[str, Any] | None) -> tuple[dict, bool]:
        """Resolve the contract to use, given an optional provided contract.

        If ``data_contract`` is provided it is used (and flagged as new); otherwise
        the contract already stored in the dataset repository is downloaded.

        Parameters
        ----------
        key : str
            Dataset identifier.
        data_contract : str | Path | dict | None
            Optional provided contract (path or dict).

        Returns
        -------
        tuple[dict, bool]
            The contract dict and a flag indicating whether it is a new contract.
        """
        if data_contract is not None:
            return self._load_contract_yaml(data_contract), True
        return self._download_contract(key), False

    def get(self, key: str, **kwargs) -> DataFrameSplits:  # noqa: ANN003
        """Load a dataset and validate every split against its data contract.

        Parameters
        ----------
        key : str
            Dataset identifier (e.g. "owner/dataset").
        **kwargs
            Additional keyword arguments forwarded to the base store.

        Returns
        -------
        DataFrameSplits
            The validated dataset splits.

        Raises
        ------
        NoStoreException
            If the dataset or its data contract is not found.
        ValidationError
            If any split fails validation.
        """
        logger.info(f"[get|in] (key={key})")
        result: DataFrameSplits = super().get(key, **kwargs)
        contract = self._download_contract(key)
        expectations = self._expectations_from_contract(contract)
        for split in self.__CONTRACT_SPLITS:
            df = getattr(result, split)
            if df is not None:
                self._validate_df(df, expectations, f"{key}/{split}")
        logger.info(f"[get|out] => shape: {result.shape}")
        return result

    def save(
        self,
        df: pd.DataFrame,
        key: str,
        split: str = "train",
        data_contract: str | Path | dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Validate data and save it, storing a new contract if provided.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to save.
        key : str
            Dataset identifier.
        split : str
            Dataset split to save to.
        data_contract : str | Path | dict | None
            Optional new data contract. If provided, its content is used to validate
            the data and is stored in the repository.
        **kwargs
            Additional keyword arguments forwarded to the base store.

        Raises
        ------
        ValidationError
            If the data fails validation against the contract.
        """
        logger.info(f"[save|in] (df shape={df.shape}, key={key}, split={split}, data_contract={data_contract is not None})")
        contract, is_new = self._resolve_contract(key, data_contract)
        expectations = self._expectations_from_contract(contract)
        self._validate_df(df, expectations, f"{key}/{split}")
        result = super().save(df, key, split=split, **kwargs)
        if is_new:
            self._store_contract(key, contract)
        logger.info("[save|out]")
        return result

    def update(
        self,
        df: pd.DataFrame,
        key: str,
        split: str,
        key_fields: list[str],
        data_contract: str | Path | dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Validate incoming data and update it, storing a new contract if provided.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame containing the data to update.
        key : str
            Dataset identifier.
        split : str
            Dataset split to update.
        key_fields : list[str]
            Column names used as key fields for matching rows.
        data_contract : str | Path | dict | None
            Optional new data contract. If provided, its content is used to validate
            the data and is stored in the repository.
        **kwargs
            Additional keyword arguments forwarded to the base store.

        Raises
        ------
        ValidationError
            If the data fails validation against the contract.
        """
        logger.info(f"[update|in] (df shape={df.shape}, key={key}, split={split}, data_contract={data_contract is not None})")
        contract, is_new = self._resolve_contract(key, data_contract)
        expectations = self._expectations_from_contract(contract)
        self._validate_df(df, expectations, f"{key}/{split}")
        result = super().update(df, key, split, key_fields, **kwargs)
        if is_new:
            self._store_contract(key, contract)
        logger.info("[update|out]")
        return result
