import sys
sys.stdout.reconfigure(encoding="utf-8")

from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lit, row_number, current_timestamp, upper, trim

################################################################################
# Spark helper
################################################################################

def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("US_DISASTER_SCHEMA")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

################################################################################
# Transform Disaster_DIM
################################################################################

def transform_disaster_dim(spark: SparkSession, jdbc_cfg: dict, csv_path: str):
    """
    Vrati Disaster_DIM DataFrame spreman za load u DW.

    • Prirodni ključ: incident_type (UPPER+TRIM radi konzistencije)
    • Surrogate key: disaster_tk (1...n po abecedi incident_type)
    • SCD1 kolone: version, date_from, date_to
    • UNKNOWN red (disaster_tk = 0)
    """

    # 1) Extract iz OLTP
    db_df = (
        spark.read.jdbc(
            jdbc_cfg["url"], '"Disaster"', properties=jdbc_cfg["properties"]
        )
        .select(upper(trim(col("incident_type"))).alias("incident_type"))
    )

    # 2) Extract iz CSV
    csv_df = (
        spark.read.csv(csv_path, header=True, inferSchema=True)
        .select(upper(trim(col("incident_type"))).alias("incident_type"))
    )

    # 3) Union & deduplicate
    all_disasters = (
        db_df.unionByName(csv_df)
        .dropna(subset=["incident_type"])
        .dropDuplicates(["incident_type"])
    )

    # 4) Surrogate key & SCD kolone
    w = Window.orderBy("incident_type")
    dim = (
        all_disasters.withColumn("disaster_tk", row_number().over(w))
        .withColumn("version", lit(1))
        .withColumn("date_from", current_timestamp())
        .withColumn("date_to", lit("2200-01-01 00:00:00").cast("timestamp"))
        .select("disaster_tk", "version", "date_from", "date_to", "incident_type")
    )

    # 5) UNKNOWN red
    schema = dim.schema
    unknown_row = (0, 1, datetime.utcnow(), None, "UNKNOWN")
    unknown_df = spark.createDataFrame([unknown_row], schema=schema)

    final_df = unknown_df.unionByName(dim)

    print("DISASTER_DIM redaka:", final_df.count())
    return final_df

################################################################################
# Main entry
################################################################################

if __name__ == "__main__":
    spark = get_spark_session()

    JDBC = {
        "url": "jdbc:postgresql://localhost:5432/us_disasters2",
        "properties": {
            "user": "postgres",
            "password": "1234",
            "driver": "org.postgresql.Driver",
        },
    }

    CSV_20 = "data/US_DISASTERS_PROCESSED_20.csv"

    disaster_dim_df = transform_disaster_dim(spark, JDBC, CSV_20)

    disaster_dim_df.write.jdbc(
        JDBC["url"], "disaster_dim", mode="overwrite", properties=JDBC["properties"]
    )

    print("Dimenzijska tablica 'Disaster_DIM' uspješno spremljena.")
    spark.stop()
