from __future__ import annotations

import os
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

DEPRECATED_PREFIX_REGEX = r"(?i)^\s*DEPRECATED:\s*"

BASE_COLUMNS = [
    "weakness_id",
    "title",
    "description",
    "extended_description",
    "alternative_terms",
    "potential_mitigations",
    "likelihood_of_exploit",
    "demonstrative_examples",
    "selected_observed_examples",
    "vulnerability_mapping_notes",
]
DERIVED_COLUMNS = ["search_text"]
ALL_COLUMNS = BASE_COLUMNS + DERIVED_COLUMNS
REMOVED_COLUMNS = {
    "is_deprecated",
    "content_history_last_modified",
    "source_version_id",
    "ingested_at_utc",
}


def _get_param(name: str, default: str = "") -> str:
    if "dbutils" in globals():
        try:
            return dbutils.widgets.get(name)
        except Exception:
            pass
    return os.getenv(name.upper(), default)


def _normalize_whitespace(value: str | None) -> str:
    return " ".join((value or "").split())


def is_deprecated_title(title: str | None) -> bool:
    return bool(re.match(DEPRECATED_PREFIX_REGEX, title or ""))


def sanitize_title_for_search(title: str | None) -> str:
    without_prefix = re.sub(DEPRECATED_PREFIX_REGEX, "", title or "")
    return _normalize_whitespace(without_prefix)


def compose_search_text(
    *,
    weakness_id: str | None,
    title: str | None,
    description: str | None,
    extended_description: str | None,
    potential_mitigations: str | None,
) -> str:
    parts = [
        f"CWE-{(weakness_id or '').strip()}",
        sanitize_title_for_search(title),
        description or "",
        extended_description or "",
        potential_mitigations or "",
    ]

    normalized_parts = [
        _normalize_whitespace(part)
        for part in parts
        if _normalize_whitespace(part)
    ]
    return " ".join(normalized_parts)


def resolve_silver_table(*, target_table: str, silver_table: str = "") -> str:
    explicit = (silver_table or "").strip()
    if explicit:
        return explicit

    target = (target_table or "").strip()
    if not target:
        raise ValueError("target_table is required to derive silver table")

    return f"{target}_silver"


def _silver_schema_ddl() -> str:
    return """
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
        search_text STRING
    """


def _ensure_silver_table(spark: "SparkSession", silver_table: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {silver_table} (
            {_silver_schema_ddl()}
        ) USING DELTA
        """
    )

    existing_columns = {column.lower() for column in spark.table(silver_table).columns}
    if existing_columns.intersection(REMOVED_COLUMNS):
        spark.sql(
            f"""
            CREATE OR REPLACE TABLE {silver_table} (
                {_silver_schema_ddl()}
            ) USING DELTA
            """
        )


def _enable_cdf(spark: "SparkSession", silver_table: str) -> None:
    spark.sql(
        f"""
        ALTER TABLE {silver_table}
        SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """
    )


def _prepare_source_view(spark: "SparkSession", bronze_table: str) -> None:
    from pyspark.sql import functions as F

    bronze_df = spark.table(bronze_table)

    title_clean_expr = F.trim(
        F.regexp_replace(
            F.coalesce(F.col("title"), F.lit("")), DEPRECATED_PREFIX_REGEX, ""
        )
    )
    is_deprecated_expr = F.upper(F.trim(F.coalesce(F.col("title"), F.lit("")))).startswith(
        "DEPRECATED:"
    )
    search_text_expr = F.trim(
        F.regexp_replace(
            F.concat_ws(
                " ",
                F.concat(F.lit("CWE-"), F.coalesce(F.col("weakness_id"), F.lit(""))),
                title_clean_expr,
                F.coalesce(F.col("description"), F.lit("")),
                F.coalesce(F.col("extended_description"), F.lit("")),
                F.coalesce(F.col("potential_mitigations"), F.lit("")),
            ),
            r"\s+",
            " ",
        )
    )

    silver_source_df = bronze_df.filter(~is_deprecated_expr).select(
        *[F.col(column_name) for column_name in BASE_COLUMNS],
        search_text_expr.alias("search_text"),
    )
    silver_source_df.createOrReplaceTempView("incoming_cwe_silver")


def _merge(spark: "SparkSession", silver_table: str) -> None:
    compare_columns = [column for column in ALL_COLUMNS if column != "weakness_id"]
    changed_condition = " OR ".join(
        [
            f"COALESCE(CAST(target.{column} AS STRING), '') <> "
            f"COALESCE(CAST(source.{column} AS STRING), '')"
            for column in compare_columns
        ]
    )
    update_assignments = ",\n            ".join(
        [f"{column} = source.{column}" for column in compare_columns]
    )
    insert_columns = ",\n            ".join(ALL_COLUMNS)
    insert_values = ",\n            ".join([f"source.{column}" for column in ALL_COLUMNS])

    spark.sql(
        f"""
        MERGE INTO {silver_table} AS target
        USING incoming_cwe_silver AS source
        ON target.weakness_id = source.weakness_id
        WHEN MATCHED
         AND ({changed_condition})
        THEN UPDATE SET
            {update_assignments}
        WHEN NOT MATCHED
        THEN INSERT (
            {insert_columns}
        ) VALUES (
            {insert_values}
        )
        WHEN NOT MATCHED BY SOURCE THEN DELETE
        """
    )


def transform_bronze_to_silver(
    spark: "SparkSession", *, bronze_table: str, silver_table: str
) -> None:
    _ensure_silver_table(spark, silver_table)
    _enable_cdf(spark, silver_table)
    _prepare_source_view(spark, bronze_table)
    _merge(spark, silver_table)


def main() -> None:
    from pyspark.sql import SparkSession

    bronze_table = _get_param("target_table", "main.security.cwe_weaknesses")
    explicit_silver_table = _get_param("silver_table", "")
    silver_table = resolve_silver_table(
        target_table=bronze_table,
        silver_table=explicit_silver_table,
    )

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    transform_bronze_to_silver(
        spark,
        bronze_table=bronze_table,
        silver_table=silver_table,
    )

    bronze_count = spark.table(bronze_table).count()
    silver_count = spark.table(silver_table).count()
    missing_search_text = spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {silver_table} WHERE search_text IS NULL OR TRIM(search_text) = ''"
    ).first()["cnt"]
    print(
        "cwe_silver_summary "
        f"bronze_table={bronze_table} silver_table={silver_table} "
        f"bronze_count={bronze_count} silver_count={silver_count} "
        f"missing_search_text={missing_search_text}"
    )


if __name__ == "__main__":
    main()
