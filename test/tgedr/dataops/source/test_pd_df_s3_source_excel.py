import pandas as pd

from tgedr.dataops.source.pd_df_s3_source import PdDfS3Source

BUCKET = "JustABucket"
URL = f"s3://{BUCKET}/dataset/file.xlsx"


def mocked_data(src, engine):
    if src == URL:
        return pd.DataFrame(
            {
                "name": ["John", "Anna", "Peter", "Linda"],
                "age": [28, 23, 34, 29],
                "city": ["New York", "Paris", "Berlin", "London"],
            }
        )


def test_get(monkeypatch, mocker):
    source = PdDfS3Source()
    monkeypatch.setattr(pd, "read_excel", mocked_data)
    mocker.spy(pd, "read_excel")

    df = source.get({"url": URL, "file_format": "xlsx"})
    assert "John" == df["name"][0]
    assert "London" == df["city"][3]
