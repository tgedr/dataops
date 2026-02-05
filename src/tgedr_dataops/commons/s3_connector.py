"""AWS S3 connector module.

This module provides the S3Connector class for establishing and managing
connections to AWS S3 resources.
"""
import os
import boto3


class S3Connector:
    """utility base class to be extended, providing a connection session with aws s3 resources."""

    def __init__(self) -> None:
        """Initialize the S3Connector with empty session, resource, and client attributes."""
        self.__resource = None
        self.__session = None
        self.__client = None

    @property
    def _session(self) -> boto3.Session:
        if self.__session is None:
            if "1" == os.getenv("S3_CONNECTOR_USE_CREDENTIALS", default="0"):
                self.__session = boto3.Session(
                    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                    aws_session_token=os.environ["AWS_SESSION_TOKEN"],
                    region_name=os.environ["AWS_REGION"],
                )
            else:
                self.__session = boto3.Session()

        return self.__session

    @property
    def _resource(self) -> boto3.resources.base.ServiceResource:
        if self.__resource is None:
            self.__resource = self._session.resource("s3")
        return self.__resource

    @property
    def _client(self) -> boto3.client:
        if self.__client is None:
            self.__client = self._session.client("s3")
        return self.__client
