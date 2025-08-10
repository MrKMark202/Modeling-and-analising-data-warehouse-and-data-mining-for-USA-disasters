import sys
sys.stdout.reconfigure(encoding="utf-8")

"""disaster_dim.py

Generira dimenziju **Disaster_DIM** za star-shemu US‑Disasters DW‑a.

• Prirodni ključ = `disaster_number` (FEMA jedinstveni ID)  
• Surrogate = `disaster_tk` (deterministički po rastućem `disaster_number`)  
• SCD‑1 stupci : `version`, `date_from`, `date_to`  
• Dodaje red *UNKNOWN* (`disaster_tk = 0`) — sprječava gubitak redaka pri LEFT JOIN‑u.
"""

from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    lit,
    row_number,
    current_timestamp,
    coalesce,
)

################################################################################
# Spark helper                                                                   #
################################################################################

def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("US_DISASTER_SCHEMA")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

################################################################################
# Transform Disaster_DIM                                                         #
################################################################################

def transform_disaster_dim(
    spark: SparkSession,
    jdbc_cfg: dict,
    csv_path: str,
):
    """Vrati Disaster_DIM DataFrame spreman za load u DW."""

    BOOL_COLS = [
        "ih_program_declared",
        "ia_program_declared",
        "pa_program_declared",
        "hm_program_declared",
    ]

    ##################################################################
    # 1) Extract                                                    #
    ##################################################################
    def cast_bool(df):
        """ Cast boolean stupaca i odaberi korisne kolone. """
        sel = [col("disaster_number"), col("incident_type")]
        sel.extend([col(c).cast("boolean").alias(c) for c in BOOL_COLS])
        return df.select(*sel)

    db_df = cast_bool(
        spark.read.jdbc(
            jdbc_cfg["url"], '"Disaster"', properties=jdbc_cfg["properties"]
        )
    )

    csv_df = cast_bool(
        spark.read.csv(csv_path, header=True, inferSchema=True)
    )

    ##################################################################
    # 2) Union & deduplicate on natural key                          #
    ##################################################################
    all_disasters = (
        db_df.unionByName(csv_df)
        .dropna(subset=["disaster_number"])
        .dropDuplicates(["disaster_number"])  # <-- jedan red po disasteru!
    )

    ##################################################################
        ##################################################################
    # 3) Surrogate key + SCD columns                                 #
    ##################################################################
    w = Window.orderBy("disaster_number")
    dim = (
        all_disasters.withColumn("disaster_tk", row_number().over(w))
        .withColumn("version", lit(1))
        .withColumn("date_from", current_timestamp())
        .withColumn("date_to", lit("2200-01-01 00:00:00").cast("timestamp"))
    )

    # Reorder columns to deterministic schema
    COL_ORDER = [
        "disaster_tk",
        "version",
        "date_from",
        "date_to",
        "disaster_number",
        "incident_type",
        *BOOL_COLS,
    ]
    dim = dim.select(*COL_ORDER)

    ##################################################################
    # ---- UNKNOWN row ---------------------------------------------- -----------------------------------------------------
    # 1) Preuzmi shemu iz dim (sigurno odgovara vrstama)
    schema = dim.schema

    unknown_row = (
        0,                  # disaster_tk
        1,                  # version
        datetime.utcnow(),  # date_from
        None,               # date_to
        None,               # disaster_number
        None,               # incident_type
        False,
        False,
        False,
        False,
    )

    unknown_df = spark.createDataFrame([unknown_row], schema=schema)
    final_df = unknown_df.unionByName(dim)


    print("DISASTER_DIM redaka:", final_df.count())
    return final_df

################################################################################
# Main entry                                                                     #
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
