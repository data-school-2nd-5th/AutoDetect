from __future__ import annotations

from datetime import datetime, timezone
import os
from urllib.request import urlopen

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from service.cwe_parser import parse_cwe_weaknesses


def _get_param(name: str, default: str = "") -> str:
    if "dbutils" in globals():
        try:
            return dbutils.widgets.get(name)
        except Exception:
            pass
    return os.getenv(name.upper(), default)


def _read_xml_text(spark: SparkSession, source_xml_path: str) -> str:
    if source_xml_path.startswith(("http://", "https://")):
        with urlopen(source_xml_path, timeout=30) as response:
            return response.read().decode("utf-8")

    payload = spark.read.format("binaryFile").load(source_xml_path).select("content").first()
    if payload is None:
        raise ValueError(f"XML payload not found at {source_xml_path}")

    return payload["content"].decode("utf-8")


def _schema() -> StructType:
    return StructType(
        [
            StructField("weakness_id", StringType(), False),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("extended_description", StringType(), True),
            StructField("alternative_terms", StringType(), True),
            StructField("potential_mitigations", StringType(), True),
            StructField("likelihood_of_exploit", StringType(), True),
            StructField("demonstrative_examples", StringType(), True),
            StructField("selected_observed_examples", StringType(), True),
            StructField("vulnerability_mapping_notes", StringType(), True),
            StructField("content_history_last_modified", StringType(), True),
            StructField("source_version_id", StringType(), True),
            StructField("ingested_at_utc", StringType(), True),
        ]
    )


def _ensure_table(spark: SparkSession, target_table: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            weakness_id STRING,
            title STRING,
            description STRING,
            extended_description STRING,
            alternative_terms STRING,
            potential_mitigations STRING,
            likelihood_of_exploit STRING,
            demonstrative_examples STRING,
            selected_observed_examples STRING,
            vulnerability_mapping_notes STRING,
            content_history_last_modified STRING,
            source_version_id STRING,
            ingested_at_utc STRING
        ) USING DELTA
        """
    )


def _merge(spark: SparkSession, target_table: str) -> None:
    spark.sql(
        f"""
        MERGE INTO {target_table} AS target
        USING incoming_cwe AS source
        ON target.weakness_id = source.weakness_id
        WHEN MATCHED
         AND COALESCE(target.content_history_last_modified, '') <> COALESCE(source.content_history_last_modified, '')
        THEN UPDATE SET
            title = source.title,
            description = source.description,
            extended_description = source.extended_description,
            alternative_terms = source.alternative_terms,
            potential_mitigations = source.potential_mitigations,
            likelihood_of_exploit = source.likelihood_of_exploit,
            demonstrative_examples = source.demonstrative_examples,
            selected_observed_examples = source.selected_observed_examples,
            vulnerability_mapping_notes = source.vulnerability_mapping_notes,
            content_history_last_modified = source.content_history_last_modified,
            source_version_id = source.source_version_id,
            ingested_at_utc = source.ingested_at_utc
        WHEN NOT MATCHED
        THEN INSERT (
            weakness_id,
            title,
            description,
            extended_description,
            alternative_terms,
            potential_mitigations,
            likelihood_of_exploit,
            demonstrative_examples,
            selected_observed_examples,
            vulnerability_mapping_notes,
            content_history_last_modified,
            source_version_id,
            ingested_at_utc
        ) VALUES (
            source.weakness_id,
            source.title,
            source.description,
            source.extended_description,
            source.alternative_terms,
            source.potential_mitigations,
            source.likelihood_of_exploit,
            source.demonstrative_examples,
            source.selected_observed_examples,
            source.vulnerability_mapping_notes,
            source.content_history_last_modified,
            source.source_version_id,
            source.ingested_at_utc
        )
        WHEN NOT MATCHED BY SOURCE THEN DELETE
        """
    )


def main() -> None:
    source_xml_path = _get_param("source_xml_path")
    source_version_id = _get_param("source_version_id")
    target_table = _get_param("target_table", "main.security.cwe_weaknesses")

    if not source_xml_path:
        raise ValueError("source_xml_path is required")
    if not source_version_id:
        raise ValueError("source_version_id is required")

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    xml_text = _read_xml_text(spark, source_xml_path)
    records = parse_cwe_weaknesses(xml_text)

    ingested_at_utc = datetime.now(timezone.utc).isoformat()
    for record in records:
        record["source_version_id"] = source_version_id
        record["ingested_at_utc"] = ingested_at_utc

    incoming_df = spark.createDataFrame(records, schema=_schema())
    incoming_df.createOrReplaceTempView("incoming_cwe")

    _ensure_table(spark, target_table)
    _merge(spark, target_table)


if __name__ == "__main__":
    main()
