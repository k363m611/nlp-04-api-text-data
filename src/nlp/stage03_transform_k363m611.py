"""
stage03_transform_k363m611.py

Source: validated JSON object
Sink: Polars DataFrame

Purpose
    Transform validated JSON data into a structured format.
"""

import logging
from typing import Any

import polars as pl


def run_transform(
    json_data: list[dict[str, Any]],
    LOG: logging.Logger,
) -> pl.DataFrame:
    """Transform JSON into a structured DataFrame.

    Args:
        json_data (list[dict[str, Any]]): Validated JSON data.
        LOG (logging.Logger): The logger instance.

    Returns:
        pl.DataFrame: The transformed dataset.
    """
    LOG.info("========================")
    LOG.info("STAGE 03: TRANSFORM starting...")
    LOG.info("========================")

    records: list[dict[str, Any]] = []

    for record in json_data:
        records.append(
            {
                "user_id": record["userId"],
                "post_id": record["id"],
                "title": record["title"],
                "body": record["body"],
            }
        )

    df: pl.DataFrame = pl.DataFrame(records)

    # Derived fields
    df = df.with_columns(
        [
            pl.col("title").str.len_chars().alias("title_length"),
            pl.col("body").str.len_chars().alias("body_length"),
            pl.col("title").str.split(" ").list.len().alias("title_word_count"),
            pl.col("body").str.split(" ").list.len().alias("body_word_count"),
        ]
    )

    LOG.info("Transformation complete.")
    LOG.info(f"DataFrame preview:\n{df.head()}")
    LOG.info("Sink: Polars DataFrame created")

    return df
