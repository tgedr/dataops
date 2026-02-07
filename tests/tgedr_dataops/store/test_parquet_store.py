from collections.abc import Generator
import pytest  # noqa: D100
import pandas as pd
from pandas import DataFrame
from pathlib import Path
import tempfile
import shutil
from pandas.testing import assert_frame_equal

from src.tgedr_dataops.store.parquet_store import ParquetStore




@pytest.fixture(scope="function")
def tmp_dir() -> Generator[str]:
    """Create a fresh temporary directory for each test function."""
    temp_dir = tempfile.mkdtemp(prefix="test_parquet_")
    yield temp_dir
    # Cleanup after test
    if Path(temp_dir).exists():
        shutil.rmtree(temp_dir)


@pytest.fixture
def data() -> DataFrame:  # noqa: D103
    data = pd.DataFrame({"name": ["Alice", "Bob", "Charlie"], "age": [30, 25, 35], "country": ["ES", "DE", "DK"]})
    data["country"] = data["country"].astype(str)
    return data


def test_save(tmp_dir, data):  # noqa: ANN001, ANN201, D103
    o = ParquetStore()
    o.save(data, tmp_dir, ["country"])
    df = o.get(tmp_dir).sort_values(by="name", ascending=True).reset_index(drop=True)  # noqa: PD901
    assert_frame_equal(df, data, check_categorical=False, check_dtype=False)


def test_save_twice(tmp_dir, data):  # noqa: ANN001, ANN201, D103
    o = ParquetStore()
    o.save(data, tmp_dir, ["country"])
    df = o.get(tmp_dir).sort_values(by="name", ascending=True).reset_index(drop=True)  # noqa: PD901
    o.save(df, tmp_dir, ["country"])
    df = o.get(tmp_dir).sort_values(by="name", ascending=True).reset_index(drop=True)  # noqa: PD901
    assert_frame_equal(df, data, check_categorical=False, check_dtype=False)

def test_save_twice_no_partitions(tmp_dir, data):  # noqa: ANN001, ANN201, D103
    file_path = f"{tmp_dir}/test_save_twice_no_partitions.parquet"
    o = ParquetStore()
    o.save(data, file_path)
    df = o.get(file_path).sort_values(by="name", ascending=True).reset_index(drop=True)  # noqa: PD901
    o.save(df, file_path)
    df = o.get(file_path).sort_values(by="name", ascending=True).reset_index(drop=True)  # noqa: PD901
    assert_frame_equal(df, data, check_categorical=False, check_dtype=False)

def test_append_same(tmp_dir, data):  # noqa: ANN001, ANN201, D103
    o = ParquetStore()
    o.save(data, tmp_dir, ["country"])
    df = o.get(tmp_dir).sort_values(by="name", ascending=True).reset_index(drop=True)  # noqa: PD901
    o.save(df, tmp_dir, ["country"], append=True)
    df = o.get(tmp_dir).sort_values(by="name", ascending=True).reset_index(drop=True)  # noqa: PD901
    
    expected_data = pd.concat([data, data], ignore_index=True).sort_values(by="name", ascending=True).reset_index(drop=True)
    assert_frame_equal(df, expected_data, check_categorical=False, check_dtype=False)

def test_append_same_no_partitions(tmp_dir, data):  # noqa: ANN001, ANN201, D103
    file_path = f"{tmp_dir}/test_append_same_no_partitions.parquet"
    o = ParquetStore()
    o.save(data, file_path)
    df = o.get(file_path).sort_values(by="name", ascending=True).reset_index(drop=True)  # noqa: PD901
    o.save(df, file_path, append=True)
    df = o.get(file_path).sort_values(by="name", ascending=True).reset_index(drop=True)  # noqa: PD901
    expected_data = pd.concat([data, data], ignore_index=True).sort_values(by="name", ascending=True).reset_index(drop=True)
    assert_frame_equal(df, expected_data, check_categorical=False, check_dtype=False)


def test_append_more(tmp_dir, data):  # noqa: ANN001, ANN201, D103
    o = ParquetStore()
    o.save(data, tmp_dir, ["country"])

    data_append = pd.DataFrame({"name": ["Dalila"], "age": [38], "country": ["US"]})
    o.save(data_append, tmp_dir, ["country"], append=True)
    df = o.get(tmp_dir).sort_values(by="name", ascending=True).reset_index(drop=True)  # noqa: PD901
    expected_data = pd.DataFrame({"name": ["Alice", "Bob", "Charlie", "Dalila"],
                                   "age": [30, 25, 35, 38], "country": ["ES", "DE", "DK", "US"]})
    assert_frame_equal(df, expected_data, check_categorical=False, check_dtype=False)

    data_append = pd.DataFrame({"name": ["Dalio"], "age": [76], "country": ["US"]})
    o.save(data_append, tmp_dir, ["country"], append=True)
    expected_data = pd.DataFrame({"name": ["Alice", "Bob", "Charlie", "Dalila", "Dalio"],
                                   "age": [30, 25, 35, 38, 76], "country": ["ES", "DE", "DK", "US", "US"]})
    df = o.get(tmp_dir).sort_values(by="name", ascending=True).reset_index(drop=True)  # noqa: PD901
    assert_frame_equal(df, expected_data, check_categorical=False, check_dtype=False)

def test_just_append(tmp_dir, data):  # noqa: ANN001, ANN201, D103
    o = ParquetStore()
    o.save(data, tmp_dir, ["country"], append=True)
    data_append = pd.DataFrame({"name": ["Dalila"], "age": [38], "country": ["US"]})
    o.save(data_append, tmp_dir, ["country"], append=True)
    df = o.get(tmp_dir).sort_values(by="name", ascending=True).reset_index(drop=True)  # noqa: PD901
    expected_data = pd.DataFrame({"name": ["Alice", "Bob", "Charlie", "Dalila"],
                                   "age": [30, 25, 35, 38], "country": ["ES", "DE", "DK", "US"]})
    assert_frame_equal(df, expected_data, check_categorical=False, check_dtype=False)


def test_append_more_no_partitions(tmp_dir, data):  # noqa: ANN001, ANN201, D103
    file_path = f"{tmp_dir}/test_append_more_no_partitions.parquet"
    o = ParquetStore()
    o.save(data, file_path)
    
    data_append = pd.DataFrame({"name": ["Dalila"], "age": [38], "country": ["US"]})
    o.save(data_append, file_path, append=True)
    df = o.get(file_path).sort_values(by="name", ascending=True).reset_index(drop=True)  # noqa: PD901
    expected_data = pd.DataFrame({"name": ["Alice", "Bob", "Charlie", "Dalila"],
                                   "age": [30, 25, 35, 38], "country": ["ES", "DE", "DK", "US"]})
    assert_frame_equal(df, expected_data, check_categorical=False, check_dtype=False)

    data_append = pd.DataFrame({"name": ["Dalio"], "age": [76], "country": ["US"]})
    o.save(data_append, file_path, append=True)
    expected_data = pd.DataFrame({"name": ["Alice", "Bob", "Charlie", "Dalila", "Dalio"],
                                   "age": [30, 25, 35, 38, 76], "country": ["ES", "DE", "DK", "US", "US"]})
    df = o.get(file_path).sort_values(by="name", ascending=True).reset_index(drop=True)  # noqa: PD901
    assert_frame_equal(df, expected_data, check_categorical=False, check_dtype=False)

def test_delete(tmp_dir, data):  # noqa: ANN001, ANN201, D103
    o = ParquetStore()
    o.save(data, tmp_dir, ["country"])

    o.delete(tmp_dir)

    with pytest.raises(FileNotFoundError):
        o.get(tmp_dir)  # Should raise FileNotFoundError since the file has been deleted

def test_ensure_key_parent(temporary_folder):  # noqa: ANN001, ANN201, D103
    key = str(Path(temporary_folder).joinpath("dummy/also_dummy/test_spark_parquet"))
    parent = str(Path(temporary_folder).joinpath("dummy/also_dummy"))
    o = ParquetStore()
    assert not Path(parent).exists()  # Ensure the parent directory exists
    o._ensure_key_parent(key)  # noqa: SLF001
    assert Path(parent).exists()

def test_save_delete_file(tmp_dir, data):  # noqa: ANN001, ANN201, D103
    file: str = f"{tmp_dir}/test_save_delete_file.parquet"
    o = ParquetStore()
    o.save(data, file)
    df = o.get(file).sort_values(by="name", ascending=True).reset_index(drop=True)  # noqa: PD901
    o.delete(file)
    with pytest.raises(FileNotFoundError):
        o.get(file)  # Should raise FileNotFoundError since the file has been deleted

def test_update_with_partition_no_data_yet(tmp_dir, data):  # noqa: ANN001, ANN201, D103
    file_path = f"{tmp_dir}/test_update_with_partition_no_data_yet.parquet"
    o = ParquetStore()
    o.update(df=data, key=file_path, key_fields=["name"], partition_fields=["country"])
    df = o.get(file_path).sort_values(by="name", ascending=True).reset_index(drop=True)  # noqa: PD901
    assert_frame_equal(df, data, check_categorical=False, check_dtype=False)

def test_update_with_partition_existing_field(tmp_dir, data):  # noqa: ANN001, ANN201, D103
    file_path = f"{tmp_dir}/test_update.parquet"
    o = ParquetStore()
    o.save(data, file_path, partition_fields=["country"])

    data_update = pd.DataFrame({"name": ["Alice"], "age": [37], "country": ["ES"]})
    o.update(df=data_update, key=file_path, key_fields=["name"], partition_fields=["country"])
    expected_data = pd.DataFrame({"name": ["Alice", "Bob", "Charlie"],
                                   "age": [37, 25, 35], "country": ["ES", "DE", "DK"]})
    df = o.get(file_path).sort_values(by="name", ascending=True).reset_index(drop=True)  # noqa: PD901
    assert_frame_equal(df, expected_data, check_categorical=False, check_dtype=False)

def test_update_with_partition_non_existing_field(tmp_dir, data):  # noqa: ANN001, ANN201, D103
    file_path = f"{tmp_dir}/test_update.parquet"
    o = ParquetStore()
    o.save(data, file_path, partition_fields=["country"])

    data_update = pd.DataFrame({"name": ["Donacha"], "age": [47], "country": ["ES"]})
    o.update(df=data_update, key=file_path, key_fields=["name"], partition_fields=["country"])
    expected_data = pd.DataFrame({"name": ["Alice", "Bob", "Charlie", "Donacha"],
                                   "age": [30, 25, 35, 47], "country": ["ES", "DE", "DK", "ES"]})
    df = o.get(file_path).sort_values(by="name", ascending=True).reset_index(drop=True)  # noqa: PD901
    assert_frame_equal(df, expected_data, check_categorical=False, check_dtype=False)

def test_update_with_partition_existing_and_non_existing_field(tmp_dir, data):  # noqa: ANN001, ANN201, D103
    file_path = f"{tmp_dir}/test_update.parquet"
    o = ParquetStore()
    o.save(data, file_path, partition_fields=["country"])

    data_update = pd.DataFrame({"name": ["Alice", "Donacha"], "age": [28, 47], "country": ["ES", "ES"]})
    o.update(data_update, file_path, key_fields=["name"], partition_fields=["country"])

    expected_data = pd.DataFrame({"name": ["Alice", "Bob", "Charlie", "Donacha"],
                                   "age": [28, 25, 35, 47], "country": ["ES", "DE", "DK", "ES"]})
    df = o.get(file_path).sort_values(by="name", ascending=True).reset_index(drop=True)  # noqa: PD901
    assert_frame_equal(df, expected_data, check_categorical=False, check_dtype=False)



def test_update_no_partition_existing_field(tmp_dir, data):  # noqa: ANN001, ANN201, D103
    file_path = f"{tmp_dir}/test_update.parquet"
    o = ParquetStore()
    o.save(data, file_path)

    data_update = pd.DataFrame({"name": ["Alice"], "age": [37], "country": ["ES"]})
    o.update(df=data_update, key=file_path, key_fields=["name"])
    expected_data = pd.DataFrame({"name": ["Alice", "Bob", "Charlie"],
                                   "age": [37, 25, 35], "country": ["ES", "DE", "DK"]})
    df = o.get(file_path).sort_values(by="name", ascending=True).reset_index(drop=True)  # noqa: PD901
    assert_frame_equal(df, expected_data, check_categorical=False, check_dtype=False)

def test_update_no_partition_non_existing_field(tmp_dir, data):  # noqa: ANN001, ANN201, D103
    file_path = f"{tmp_dir}/test_update.parquet"
    o = ParquetStore()
    o.save(data, file_path)

    data_update = pd.DataFrame({"name": ["Donacha"], "age": [47], "country": ["ES"]})
    o.update(df=data_update, key=file_path, key_fields=["name"])
    expected_data = pd.DataFrame({"name": ["Alice", "Bob", "Charlie", "Donacha"],
                                   "age": [30, 25, 35, 47], "country": ["ES", "DE", "DK", "ES"]})
    df = o.get(file_path).sort_values(by="name", ascending=True).reset_index(drop=True)  # noqa: PD901
    assert_frame_equal(df, expected_data, check_categorical=False, check_dtype=False)

def test_update_no_partition_existing_and_non_existing_field(tmp_dir, data):  # noqa: ANN001, ANN201, D103
    file_path = f"{tmp_dir}/test_update.parquet"
    o = ParquetStore()
    o.save(data, file_path)

    data_update = pd.DataFrame({"name": ["Alice", "Donacha"], "age": [28, 47], "country": ["ES", "ES"]})
    o.update(data_update, file_path, key_fields=["name"])

    expected_data = pd.DataFrame({"name": ["Alice", "Bob", "Charlie", "Donacha"],
                                   "age": [28, 25, 35, 47], "country": ["ES", "DE", "DK", "ES"]})
    df = o.get(file_path).sort_values(by="name", ascending=True).reset_index(drop=True)  # noqa: PD901
    assert_frame_equal(df, expected_data, check_categorical=False, check_dtype=False)

def test_update_creating_partition(tmp_dir, data):  # noqa: ANN001, ANN201, D103
    file_path = f"{tmp_dir}/test_update.parquet"
    o = ParquetStore()
    data_new = pd.DataFrame({"name": ["Alice", "Bob", "Charlie", "Dalila"],
                                   "age": [30, 25, 35, 38], "country": ["ES", "DE", "DK", "US"]})
    o.save(data_new, file_path, partition_fields=["country"])
    data_update = pd.DataFrame({"name": ["Alice"], "age": [28], "country": ["PT"]})
    # TODO - implement logic to pass this test
    with pytest.raises(TypeError, match="Cannot setitem on a Categorical with a new category, set the categories first"):
        o.update(data_update, file_path, key_fields=["name"], partition_fields=["country"])
