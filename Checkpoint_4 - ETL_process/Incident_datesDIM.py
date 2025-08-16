import sys
sys.stdout.reconfigure(encoding="utf-8")

"""
incident_dates_dim.py   –  county-level DW dimenzija

• Prirodni ključ: (incident_begin_date, incident_end_date)
• Atribut: incident_duration  [int, sati]
• Surrogate: incident_dates_tk
• UNKNOWN red (tk = 0, trajanje = 0)
"""

from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    lit,
    row_number,
    current_timestamp,
    to_date,
    datediff,      
    coalesce      
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
#  Transform                                                         #
# ------------------------------------------------------------------ #
def transform_incident_dates_dim(
    spark: SparkSession,
    jdbc_cfg: dict,
    csv_path: str,
):
    """Vrati Incident_datesDIM DataFrame spreman za DW load."""

    # -------- Extract iz baze --------------------------------------
    db_dates = (
        spark.read.jdbc(jdbc_cfg["url"], '"Declaration"',
                        properties=jdbc_cfg["properties"])
        .select(
            to_date(col("incident_begin_date")).alias("incident_begin_date"),
            to_date(col("incident_end_date")).alias("incident_end_date"),
            col("incident_duration").cast("int")
        )
    )

    # -------- Extract iz CSV-a -------------------------------------
    csv_dates = (
        spark.read.csv(csv_path, header=True, inferSchema=True)
        .select(
            to_date(col("incident_begin_date")).alias("incident_begin_date"),
            to_date(col("incident_end_date")).alias("incident_end_date"),
            col("incident_duration").cast("int")
        )
    )

    # -------- Union & deduplicate ----------------------------------
    all_dates = (
        db_dates.unionByName(csv_dates)
        .dropna(subset=["incident_begin_date", "incident_end_date"])
        .withColumn(
            "incident_duration",
            # ako je NULL, izračunaj (datediff * 24)  – fallback
            coalesce(
                col("incident_duration"),
                datediff(col("incident_end_date"),
                         col("incident_begin_date")) * 24
            ).cast("int")
        )
        .dropDuplicates(["incident_begin_date", "incident_end_date"])
    )

    # -------- Surrogate key & SCD ----------------------------------
    w = Window.orderBy("incident_begin_date", "incident_end_date")
    dim_dates = (
        all_dates.withColumn("incident_dates_tk", row_number().over(w))
        .withColumn("version", lit(1))
        .withColumn("date_from", current_timestamp())
        .withColumn("date_to", lit("2200-01-01 00:00:00").cast("timestamp"))
        .select(
            "incident_dates_tk", "version", "date_from", "date_to",
            "incident_begin_date", "incident_end_date", "incident_duration"
        )
    )

    # -------- UNKNOWN row ------------------------------------------
    unknown_row = (
        0,                # incident_dates_tk
        1,                # version
        datetime.utcnow(),
        None,             # date_to
        None,             # begin
        None,             # end
        0                 # incident_duration
    )
    unknown_df = spark.createDataFrame([unknown_row], dim_dates.schema)

    final_df = unknown_df.unionByName(dim_dates)

    print("INCIDENT_DATES_DIM redaka:", final_df.count())
    return final_df

# ------------------------------------------------------------------ #
#  Main                                                              #
# ------------------------------------------------------------------ #
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

    dates_dim_df = transform_incident_dates_dim(spark, JDBC, CSV_20)

    dates_dim_df.write.jdbc(
        JDBC["url"], "incident_datesdim",
        mode="overwrite", properties=JDBC["properties"]
    )

    print("Dimenzijska tablica 'Incident_datesDIM' uspješno spremljena.")
    spark.stop()
