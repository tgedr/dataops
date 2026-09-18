"""Unit/integration tests for ContractedHFDatasetFileBasedStore.

Tests exercise the real store code paths (read, validation, parquet chunking and
local writes) using the sample data and the data contract, mocking only the
Hugging Face network client (HfApi / load_dataset / hf_hub_download / upload_file).
"""

from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
import tempfile
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import yaml
from pandas.testing import assert_frame_equal
from datasets import Dataset, DatasetDict

from tgedr_dataops_abs.great_expectations_validation import ValidationError
from tgedr_dataops_abs.store import NoStoreException

from tgedr_dataops.quality.pandas_validation import PandasValidation
from tgedr_dataops.store.hf_dataset import DataFrameSplits
from src.tgedr_dataops.store.contracted_store import ContractedHFDatasetFileBasedStore, HuggingFaceDatasetContractMixin


RESOURCES = Path("tests/resources")
CONTRACT_PATH = RESOURCES / "demo.odcs.yaml"
SAMPLE_PARQUET = RESOURCES / "faers_demo_sample.parquet"


def make_datasetdict(df: pd.DataFrame, test_rows: int = 0, validation_rows: int = 0) -> DatasetDict:
    """Build a real DatasetDict from the sample DataFrame (fractioned into splits).

    Parameters
    ----------
    df : pd.DataFrame
        Source DataFrame (sample parquet).
    test_rows : int
        Number of trailing rows to use as the "test" split.
    validation_rows : int
        Number of trailing rows (after test) to use as the "validation" split.

    Returns
    -------
    DatasetDict
        A dataset whose splits are derived from the sample data.
    """
    parts: dict[str, Dataset] = {"train": Dataset.from_pandas(df.reset_index(drop=True))}
    start = 0
    if test_rows:
        parts["test"] = Dataset.from_pandas(df.iloc[:test_rows].reset_index(drop=True))
        start = test_rows
    if validation_rows:
        parts["validation"] = Dataset.from_pandas(
            df.iloc[start : start + validation_rows].reset_index(drop=True)
        )
    return DatasetDict(parts)


@contextmanager
def hf_network(
    dataset_exists: bool = True,
    repo_files: list[str] | None = None,
) -> AbstractContextManager[tuple[MagicMock, MagicMock, Path]]:
    """Mock only the Hugging Face network, running local logic for real.

    Uses a real temporary directory for parquet files written by the store
    before upload. Yields (api_mock, contract_upload_mock, tmp_dir).
    """
    with (
        patch("tgedr_dataops.store.hf_dataset_file_based.HfApi") as api_cls,
        patch("src.tgedr_dataops.store.contracted_store.hf_hub_download", return_value=str(CONTRACT_PATH)),
        patch("src.tgedr_dataops.store.contracted_store.upload_file") as contract_up,
        tempfile.TemporaryDirectory() as td,
        patch(
            "tgedr_dataops.store.hf_dataset_file_based.tempfile.TemporaryDirectory",
            return_value=MagicMock(__enter__=lambda _: td, __exit__=lambda *_: None),
        ),
    ):
        api = api_cls.return_value
        api.list_datasets.return_value = iter(["owner/dataset"] if dataset_exists else [])
        api.list_repo_files.return_value = repo_files or []
        yield api, contract_up, Path(td)


@pytest.fixture
def contract() -> dict:  # noqa: D103
    with CONTRACT_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.fixture
def sample_df() -> pd.DataFrame:  # noqa: D103
    return pd.read_parquet(SAMPLE_PARQUET)


@pytest.fixture
def store() -> ContractedHFDatasetFileBasedStore:  # noqa: D103
    return ContractedHFDatasetFileBasedStore()


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------


def test_init_default_validation(store) -> None:  # noqa: ANN001, D103
    assert isinstance(store._validation, PandasValidation)  # noqa: SLF001


def test_init_custom_validation() -> None:  # noqa: D103
    validation = MagicMock()
    store = ContractedHFDatasetFileBasedStore(validation=validation)
    assert store._validation is validation  # noqa: SLF001


def test_init_is_contract_mixin(store) -> None:  # noqa: ANN001, D103
    assert isinstance(store, HuggingFaceDatasetContractMixin)


# ---------------------------------------------------------------------------
# contract helpers
# ---------------------------------------------------------------------------


def test_contract_file_name(store) -> None:  # noqa: ANN001, D103
    assert store._contract_file_name("owner/dataset") == "dataset.odcs.yaml"  # noqa: SLF001
    assert store._contract_file_name("dataset") == "dataset.odcs.yaml"  # noqa: SLF001


def test_load_contract_yaml_from_path(contract) -> None:  # noqa: ANN001, D103
    mixin = HuggingFaceDatasetContractMixin()
    loaded = mixin._load_contract_yaml(CONTRACT_PATH)  # noqa: SLF001
    assert loaded == contract


def test_load_contract_yaml_from_dict(contract) -> None:  # noqa: ANN001, D103
    mixin = HuggingFaceDatasetContractMixin()
    assert mixin._load_contract_yaml(contract) is contract  # noqa: SLF001


def test_expectations_from_contract(contract) -> None:  # noqa: ANN001, D103
    mixin = HuggingFaceDatasetContractMixin()
    expectations = mixin._expectations_from_contract(contract)  # noqa: SLF001
    assert expectations["expectation_suite_name"] == "demo_suite"
    assert len(expectations["expectations"]) > 0


def test_expectations_from_contract_missing(store) -> None:  # noqa: ANN001, D103
    with pytest.raises(ValidationError, match="expectations"):
        store._expectations_from_contract({"kind": "DataContract"})  # noqa: SLF001
    with pytest.raises(ValidationError, match="expectations"):
        store._expectations_from_contract(  # noqa: SLF001
            {"kind": "DataContract", "customProperties": [{"property": "other"}]}
        )


# ---------------------------------------------------------------------------
# get (real read path, network-only mocks)
# --------------------------------------------------------------------------


def test_get_loads_and_validates_all_splits(store, sample_df) -> None:  # noqa: ANN001, D103
    ds = make_datasetdict(sample_df, test_rows=10, validation_rows=10)
    with (
        patch("tgedr_dataops.store.hf_dataset_file_based.datasets.load_dataset", return_value=ds) as load,
        patch("src.tgedr_dataops.store.contracted_store.hf_hub_download", return_value=str(CONTRACT_PATH)),
    ):
        result = store.get(key="owner/dataset")

    load.assert_called_once_with("owner/dataset")
    assert isinstance(result, DataFrameSplits)
    # The base get() round-trips each split through the dataset, then our get()
    # validates every present split against the real contract expectations.
    # NB: the sample parquet carries a non-default index that is dropped on the
    # dataset round-trip, so compare against the reset index.
    assert_frame_equal(result.train, sample_df.reset_index(drop=True))
    assert_frame_equal(result.test, sample_df.iloc[:10].reset_index(drop=True))
    assert_frame_equal(result.validation, sample_df.iloc[10:20].reset_index(drop=True))


def test_get_missing_contract_raises(store, sample_df) -> None:  # noqa: ANN001, D103
    ds = make_datasetdict(sample_df)
    with (
        patch("tgedr_dataops.store.hf_dataset_file_based.datasets.load_dataset", return_value=ds),
        patch("src.tgedr_dataops.store.contracted_store.hf_hub_download", side_effect=Exception("not found")),
        pytest.raises(NoStoreException, match="not found in dataset"),
    ):
        store.get(key="owner/dataset")


def test_get_fails_validation_on_bad_split(store, sample_df) -> None:  # noqa: ANN001, D103
    # Drop a required column so the expectation suite cannot pass.
    bad = sample_df.drop(columns=["primaryid"])
    ds = make_datasetdict(bad)
    with (
        patch("tgedr_dataops.store.hf_dataset_file_based.datasets.load_dataset", return_value=ds),
        patch("src.tgedr_dataops.store.contracted_store.hf_hub_download", return_value=str(CONTRACT_PATH)),
        pytest.raises(ValidationError, match="failed validation"),
    ):
        store.get(key="owner/dataset")


# ---------------------------------------------------------------------------
# save (real validation + chunk/write path, network-only mocks)
# --------------------------------------------------------------------------


def test_save_validates_writes_parquet_and_stores_contract(sample_df) -> None:  # noqa: ANN001, D103
    with hf_network(dataset_exists=False) as (api, contract_up, tmp):
        store = ContractedHFDatasetFileBasedStore()
        store.save(df=sample_df, key="owner/dataset", split="train", data_contract=CONTRACT_PATH)
        # Capture the locally written files while the temp dir still exists.
        parquet_files = sorted(tmp.glob("*.parquet"))
        saved = pd.read_parquet(parquet_files[0]) if parquet_files else pd.DataFrame()

    # Dataset did not exist -> repository created; empty repo -> no prior deletion.
    api.create_repo.assert_called_once_with(
        repo_id="owner/dataset", repo_type="dataset", exist_ok=True, visibility="public"
    )
    # One parquet chunk written locally and uploaded.
    assert len(parquet_files) == 1
    assert_frame_equal(saved, sample_df.reset_index(drop=True))
    assert api.upload_file.call_count == 1
    # The provided contract was stored back to the repository.
    assert contract_up.call_count == 1
    assert contract_up.call_args.kwargs["path_in_repo"] == "dataset.odcs.yaml"


def test_save_reuses_existing_contract(sample_df) -> None:  # noqa: ANN001, D103
    with hf_network(dataset_exists=True, repo_files=["train_00000.parquet"]) as (api, contract_up, tmp):
        store = ContractedHFDatasetFileBasedStore()
        store.save(df=sample_df, key="owner/dataset", split="train")
        n_parquet = len(list(tmp.glob("*.parquet")))

    # No contract argument -> the repo contract is used (network download mocked),
    # so no new contract is stored and no dataset is created.
    assert contract_up.call_count == 0
    api.create_repo.assert_not_called()
    assert n_parquet == 1
    assert api.upload_file.call_count == 1


def test_save_fails_validation_without_writing(sample_df) -> None:  # noqa: ANN001, D103
    bad = sample_df.drop(columns=["primaryid"])
    with hf_network(dataset_exists=True) as (api, contract_up, tmp):
        store = ContractedHFDatasetFileBasedStore()
        with pytest.raises(ValidationError, match="failed validation"):
            store.save(df=bad, key="owner/dataset", split="train", data_contract=CONTRACT_PATH)
        n_parquet = len(list(tmp.glob("*.parquet")))

    # fail fast: nothing persisted, nothing uploaded, nothing stored.
    assert api.upload_file.call_count == 0
    assert contract_up.call_count == 0
    assert n_parquet == 0


# ---------------------------------------------------------------------------
# update (real update path, network-only mocks)
# --------------------------------------------------------------------------


def test_update_validates_persists_and_stores_contract(sample_df) -> None:  # noqa: ANN001, D103
    incoming = sample_df.copy().reset_index(drop=True)
    existing = make_datasetdict(sample_df)

    with (
        hf_network(dataset_exists=True, repo_files=["train_00000.parquet"]) as (api, contract_up, tmp),
        patch("tgedr_dataops.store.hf_dataset_file_based.datasets.load_dataset", return_value=existing),
    ):
        store = ContractedHFDatasetFileBasedStore()
        store.update(
            df=incoming,
            key="owner/dataset",
            split="train",
            key_fields=["primaryid"],
            data_contract=CONTRACT_PATH,
        )
        n_parquet = len(list(tmp.glob("*.parquet")))

    # Existing split replaced and re-uploaded as a single parquet chunk.
    assert n_parquet == 1
    assert api.upload_file.call_count == 1
    # Provided contract stored once.
    assert contract_up.call_count == 1
    assert contract_up.call_args.kwargs["path_in_repo"] == "dataset.odcs.yaml"


def test_update_uses_existing_contract(sample_df) -> None:  # noqa: ANN001, D103
    incoming = sample_df.copy().reset_index(drop=True)
    existing = make_datasetdict(sample_df)

    with (
        hf_network(dataset_exists=True, repo_files=["train_00000.parquet"]) as (api, contract_up, tmp),
        patch("tgedr_dataops.store.hf_dataset_file_based.datasets.load_dataset", return_value=existing),
    ):
        store = ContractedHFDatasetFileBasedStore()
        store.update(df=incoming, key="owner/dataset", split="train", key_fields=["primaryid"])
        n_parquet = len(list(tmp.glob("*.parquet")))

    # No contract argument -> existing repo contract reused, nothing new stored.
    assert contract_up.call_count == 0
    assert n_parquet == 1
    assert api.upload_file.call_count == 1
