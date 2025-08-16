import sys
sys.stdout.reconfigure(encoding="utf-8")

from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, row_number, current_timestamp,
    upper, trim, coalesce
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
#  Transform – COUNTY-level County_DIM                               #
# ------------------------------------------------------------------ #
def transform_county_dim(spark: SparkSession, jdbc_cfg: dict, csv_path: str):
    """
    Build County_DIM bez ikakvih FK-ova.

    Prirodni ključ: county_name (UPPER+TRIM)
    Surrogate key: county_tk (1 … n po abecedi county_name)
    SCD kolone: version, date_from, date_to (open-ended 2200-01-01)
    """

    # 1) Extract iz OLTP: "County" (samo county_name)
    db_counties = (
        spark.read.jdbc(
            jdbc_cfg["url"], '"County"', properties=jdbc_cfg["properties"]
        )
        .select(upper(trim(col("county_name"))).alias("county_name"))
        .filter(col("county_name").isNotNull())
        .dropDuplicates(["county_name"])
    )

    # 2) Extract iz 20% CSV: podrži varijante county_name / county
    csv_counties = (
        spark.read.csv(csv_path, header=True, inferSchema=True)
        .select(upper(trim(coalesce(col("county_name"), col("county_name")))).alias("county_name"))
        .filter(col("county_name").isNotNull())
        .dropDuplicates(["county_name"])
    )

    # 3) Union & deduplicate po county_name
    all_counties = (
        db_counties.unionByName(csv_counties)
        .dropDuplicates(["county_name"])
    )

    # 4) Surrogate key & SCD kolone
    w = Window.orderBy("county_name")
    dim = (
        all_counties.withColumn("county_tk", row_number().over(w))
        .withColumn("version", lit(1))
        .withColumn("date_from", current_timestamp())
        .withColumn("date_to", lit("2200-01-01 00:00:00").cast("timestamp"))
        .select("county_tk", "version", "date_from", "date_to", "county_name")
    )

    # 5) UNKNOWN red (TK=0)
    unknown = spark.createDataFrame(
        [(0, 1, datetime.utcnow(), datetime(2200, 1, 1, 0, 0, 0), "UNKNOWN")],
        schema=dim.schema
    )

    final_df = unknown.unionByName(dim)

    print("COUNTY_DIM rows:", final_df.count())
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

    county_dim_df = transform_county_dim(spark, JDBC, CSV_20)

    county_dim_df.write.jdbc(
        JDBC["url"],
        "county_dim",   # county-level dimension
        mode="overwrite",
        properties=JDBC["properties"],
    )

    print("Dimenzijska tablica 'county_dim' uspješno spremljena.")
    spark.stop()
