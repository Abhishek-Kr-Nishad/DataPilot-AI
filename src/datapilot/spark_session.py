"""PySpark session management with local Windows and AWS Glue compatibility."""

from __future__ import annotations

import os
import sys
import threading
from contextlib import contextmanager
from typing import Iterator

from pyspark.sql import SparkSession

from datapilot.config import AppSettings
from datapilot.logging_utils import get_logger

logger = get_logger(__name__)
_SPARK_LOCK = threading.RLock()


def configure_spark_environment(settings: AppSettings) -> None:
    """Configure environment variables required for reliable local PySpark execution."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    if settings.local.hadoop_home:
        os.environ["HADOOP_HOME"] = settings.local.hadoop_home
        hadoop_bin = os.path.join(settings.local.hadoop_home, "bin")
        current_path = os.environ.get("PATH", "")
        if os.path.isdir(hadoop_bin) and hadoop_bin not in current_path:
            os.environ["PATH"] = f"{hadoop_bin}{os.pathsep}{current_path}"


def build_spark_session(
    settings: AppSettings,
    app_name_suffix: str | None = None,
) -> SparkSession:
    """Create or retrieve a configured local SparkSession.

    Disables ANSI strict-cast exceptions so malformed raw records evaluate to NULL
    during Silver parsing and are routed cleanly to the quarantine dataset instead
    of crashing batch jobs.
    """
    configure_spark_environment(settings)
    app_name = (
        f"{settings.local.spark_app_name}-{app_name_suffix}"
        if app_name_suffix
        else settings.local.spark_app_name
    )

    logger.info(
        "Starting SparkSession app=%s master=%s python=%s",
        app_name,
        settings.local.spark_master,
        sys.executable,
    )

    spark = (
        SparkSession.builder.master(settings.local.spark_master)
        .appName(app_name)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


@contextmanager
def spark_session_context(
    settings: AppSettings,
    app_name_suffix: str | None = None,
    existing_spark: SparkSession | None = None,
) -> Iterator[SparkSession]:
    """Yield an active SparkSession and stop it only if created by this context."""
    with _SPARK_LOCK:
        if existing_spark is not None:
            yield existing_spark
            return

        spark = build_spark_session(settings, app_name_suffix=app_name_suffix)
        try:
            yield spark
        finally:
            logger.info("Stopping SparkSession app=%s", spark.sparkContext.appName)
            try:
                spark.stop()
            except Exception as exc:
                logger.warning("Ignoring SparkSession stop warning: %s", exc)
