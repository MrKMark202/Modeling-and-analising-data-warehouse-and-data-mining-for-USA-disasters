import sys
sys.stdout.reconfigure(encoding="utf-8")

from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, row_number, current_timestamp,
    upper, trim
)

# ------------------------------------------------------------------ #
#  Spark helper                                                      #
# ------------------------------------------------------------------ #
def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("US_DISASTER_SCHEMA")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

# ------------------------------------------------------------------ #
#  Transform  –  COUNTY-level State_DIM                              #
# ------------------------------------------------------------------ #
def transform_state_dim(spark: SparkSession, jdbc_cfg: dict, csv_path: str):
    """
    Build State_DIM where *country_name* (county) is the natural key.
    Surrogate key: state_tk (1 … n by alphabetical country_name)
    """

    # 1) Extract from OLTP ---------------------------------------------------
    db_states = (
        spark.read.jdbc(
            jdbc_cfg["url"],
            '"State"',                       # original OLTP table
            properties=jdbc_cfg["properties"],
        )
        .select(
            upper(trim(col("name"))).alias("state_name"),
            upper(trim(col("country_name"))).alias("country_name"),
        )
    )

    # 2) Extract from 20 % CSV ----------------------------------------------
    csv_states = (
        spark.read.csv(csv_path, header=True, inferSchema=True)
        .select(
            upper(trim(col("state"))).alias("state_name"),
            upper(trim(col("country_name"))).alias("country_name"),
        )
    )

    # 3) Union & deduplicate on county --------------------------------------
    all_counties = (
        db_states.unionByName(csv_states)
        .filter(col("country_name").isNotNull())
        .dropDuplicates(["country_name"])          # → 1 red / county
    )

    # 4) Surrogate key & SCD columns ----------------------------------------
    w = Window.orderBy("country_name")
    dim = (
        all_counties.withColumn("state_tk", row_number().over(w))
        .withColumn("version", lit(1))
        .withColumn("date_from", current_timestamp())
        .withColumn("date_to", lit("2200-01-01 00:00:00").cast("timestamp"))
        .select(
            "state_tk", "version", "date_from", "date_to",
            "state_name", "country_name"
        )
    )

    # 5) UNKNOWN row --------------------------------------------------------
    unknown = spark.createDataFrame(
        [(0, 1, datetime.utcnow(), None, "UNKNOWN", "UNKNOWN")],
        schema=dim.schema
    )

    final_df = unknown.unionByName(dim)

    # 6) Validation printout -------------------------------------------------
    print("STATE_DIM (county-level) rows:", final_df.count())  # ≈ 3 316 (1 UNKNOWN)
    return final_df

# ------------------------------------------------------------------ #
#  CLI entry-point                                                   #
# ------------------------------------------------------------------ #
if __name__ == "__main__":
    spark = get_spark_session()

    JDBC = {
        "url": "jdbc:postgresql://localhost:5432/us_disasters2",
        "properties": {
            "user":     "postgres",
            "password": "1234",
            "driver":   "org.postgresql.Driver",
        },
    }

    CSV_20 = "data/US_DISASTERS_PROCESSED_20.csv"

    state_dim_df = transform_state_dim(spark, JDBC, CSV_20)

    state_dim_df.write.jdbc(
        JDBC["url"],
        "state_dim",              # county-level dimension
        mode="overwrite",
        properties=JDBC["properties"],
    )

    print("Dimenzijska tablica 'state_dim' (county-level) uspješno spremljena.")
    spark.stop()
