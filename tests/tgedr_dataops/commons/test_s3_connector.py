import os

import pytest

from tgedr_dataops.commons.s3_connector import S3Connector


def test_s3_connector_without_credentials(monkeypatch):
    monkeypatch.setenv("S3_CONNECTOR_USE_CREDENTIALS", "0")
    connector = S3Connector()
    
    # Access session to trigger lazy loading
    session = connector._session
    assert session is not None
    
    # Access resource to trigger lazy loading
    resource = connector._resource
    assert resource is not None
    
    # Access client to trigger lazy loading
    client = connector._client
    assert client is not None


def test_s3_connector_with_credentials(monkeypatch):
    monkeypatch.setenv("S3_CONNECTOR_USE_CREDENTIALS", "1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test_key_id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test_secret_key")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "test_session_token")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    
    connector = S3Connector()
    
    # Access session to trigger lazy loading with credentials
    session = connector._session
    assert session is not None
    assert session.region_name == "us-east-1"
    
    # Access resource to trigger lazy loading
    resource = connector._resource
    assert resource is not None
    
    # Access client to trigger lazy loading
    client = connector._client
    assert client is not None
