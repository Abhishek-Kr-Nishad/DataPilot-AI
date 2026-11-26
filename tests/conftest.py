"""Shared pytest fixtures for PySpark tests."""

from __future__ import annotations

from typing import Iterator

import pytest
from pyspark.sql import SparkSession

from datapilot.config import load_settings
from datapilot.spark_session import build_spark_session


@pytest.fixture(scope="session")
def spark_session(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    """Provide a shared local SparkSession for PySpark unit and integration tests."""
    root = tmp_path_factory.mktemp("spark_session_root")
    settings = load_settings(project_root=root)
    spark = build_spark_session(settings, app_name_suffix="pytest")
    try:
        yield spark
    finally:
        spark.stop()
