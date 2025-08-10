import sys
sys.stdout.reconfigure(encoding="utf-8")

"""declaration_dim.py

Generira dimenziju **Declaration_DIM** (dekret FEMA-e) za DW.

• Prirodni ključ = `declaration_request_number`  (svaka deklaracija je jedinstvena)  
• Surrogate ključ = `declaration_tk` (deterministički, sortiran po `declaration_request_number`)  
• SCD‑1 stupci: `version`, `date_from`, `date_to`  
• Dodaje red *UNKNOWN* (`declaration_tk = 0`).
"""

from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    lit,
    row_number,
    current_timestamp,
    to_date,
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
# Transform Declaration_DIM                                                      #
################################################################################

def transform_declaration_dim(
    spark: SparkSession,
    jdbc_cfg: dict,
    csv_path: str,
):
    """Vrati Declaration_DIM DataFrame spreman za DW load."""

    ##################################################################
    # 1) Extract                                                    #
    ##################################################################
    COLS = [
        "declaration_title",
        "declaration_type",
        "declaration_date",
        "declaration_request_number",
    ]

    db_df = (
        spark.read.jdbc(
            jdbc_cfg["url"], '"Declaration"', properties=jdbc_cfg["properties"]
        )
        .select(*COLS)
    )

    csv_df = (
        spark.read.csv(csv_path, header=True, inferSchema=True)
        .select(*COLS)
    )

    ##################################################################
    # 2) Union & deduplicate on natural key                          #
    ##################################################################
    all_decl = (
        db_df.unionByName(csv_df)
        .dropna(subset=["declaration_request_number"])
        .dropDuplicates(["declaration_request_number"])
        .withColumn("declaration_date", to_date(col("declaration_date")))
    )

    ##################################################################
    # 3) Surrogate key + SCD columns                                 #
    ##################################################################
    w = Window.orderBy("declaration_request_number")
    dim = (
        all_decl.withColumn("declaration_tk", row_number().over(w))
        .withColumn("version", lit(1))
        .withColumn("date_from", current_timestamp())
        .withColumn("date_to", lit("2200-01-01 00:00:00").cast("timestamp"))
    )

    # enforce column order
    COL_ORDER = [
        "declaration_tk",
        "version",
        "date_from",
        "date_to",
        *COLS,
    ]
    dim = dim.select(*COL_ORDER)

    ##################################################################
    # 4) UNKNOWN row                                                 #
    ##################################################################
    unknown_values = (
        0,  # declaration_tk
        1,  # version
        datetime.utcnow(),
        None,  # date_to
        None,  # declaration_title
        None,  # declaration_type
        None,  # declaration_date
        None,  # declaration_request_number
    )

    unknown_df = spark.createDataFrame([unknown_values], schema=dim.schema)
    final_df = unknown_df.unionByName(dim)

    print("DECLARATION_DIM redaka:", final_df.count())
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

    declaration_dim_df = transform_declaration_dim(spark, JDBC, CSV_20)

    declaration_dim_df.write.jdbc(
        JDBC["url"], "declaration_dim", mode="overwrite", properties=JDBC["properties"]
    )

    print("Dimenzijska tablica 'Declaration_DIM' uspješno spremljena.")
    spark.stop()