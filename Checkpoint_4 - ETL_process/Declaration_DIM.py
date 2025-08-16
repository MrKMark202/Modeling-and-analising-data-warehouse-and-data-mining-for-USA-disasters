import sys
sys.stdout.reconfigure(encoding="utf-8")

"""declaration_dim.py — Declaration_DIM (SCD1)"""

from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lit, row_number, current_timestamp, to_date

# ---------------- Spark helper ---------------- #
def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("US_DISASTER_SCHEMA")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

# ---------------- Transform ---------------- #
def transform_declaration_dim(spark: SparkSession, jdbc_cfg: dict, csv_path: str):
    """Vrati Declaration_DIM DataFrame spreman za DW load."""

    COLS = [
        "declaration_title",
        "declaration_type",
        "declaration_date",
        "declaration_request_number",
        "ih_program_declared",
        "ia_program_declared",
        "hm_program_declared",
        "pa_program_declared",
    ]
    BOOLS = [
        "ih_program_declared",
        "ia_program_declared",
        "hm_program_declared",
        "pa_program_declared",
    ]

    # 1) Extract
    db_df = (
        spark.read.jdbc(jdbc_cfg["url"], '"Declaration"', properties=jdbc_cfg["properties"])
        .select(*COLS)
    )
    for c in BOOLS:
        db_df = db_df.withColumn(c, col(c).cast("boolean"))

    csv_df = spark.read.csv(csv_path, header=True, inferSchema=True).select(*COLS)
    for c in BOOLS:
        csv_df = csv_df.withColumn(c, col(c).cast("boolean"))

    # 2) Union & deduplicate (prirodni ključ: declaration_request_number)
    all_decl = (
        db_df.unionByName(csv_df)
        .dropna(subset=["declaration_request_number"])
        .dropDuplicates(["declaration_request_number"])
        .withColumn("declaration_date", to_date(col("declaration_date")))
    )

    # 3) Surrogate key + SCD1 kolone
    w = Window.orderBy("declaration_request_number")
    dim = (
        all_decl.withColumn("declaration_tk", row_number().over(w))
        .withColumn("version", lit(1))
        .withColumn("date_from", current_timestamp())
        .withColumn("date_to", lit("2200-01-01 00:00:00").cast("timestamp"))
    )

    COL_ORDER = [
        "declaration_tk", "version", "date_from", "date_to", *COLS,
    ]
    dim = dim.select(*COL_ORDER)

    # 4) UNKNOWN red (točna shema i tipovi)
    unknown_row = (
        0, 1, datetime.utcnow(), None,
        None, None, None, None,  # title, type, date, request_number
        False, False, False, False
    )
    unknown_df = spark.createDataFrame([unknown_row], schema=dim.schema)
    final_df = unknown_df.unionByName(dim)

    cnt = final_df.count()
    print("DECLARATION_DIM rows to write:", cnt)
    if cnt == 0:
        raise RuntimeError("Declaration_DIM je prazan – ništa ne zapisujem.")

    return final_df

# ---------------- Main ---------------- #
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

    dim_df = transform_declaration_dim(spark, JDBC, CSV_20)

    # Eksplicitno piši u PUBLIC schemu; kreiraj kolone jasnih tipova kod prvog kreiranja
    dim_df.write \
        .mode("overwrite") \
        .option("truncate", "true") \
        .option(
            "createTableColumnTypes",
            "declaration_tk INTEGER, "
            "version INTEGER, "
            "date_from TIMESTAMP, "
            "date_to TIMESTAMP, "
            "declaration_title VARCHAR(1000), "
            "declaration_type VARCHAR(1000), "
            "declaration_date DATE, "
            "declaration_request_number INTEGER, "
            "ih_program_declared BOOLEAN, "
            "ia_program_declared BOOLEAN, "
            "hm_program_declared BOOLEAN, "
            "pa_program_declared BOOLEAN"
        ) \
        .jdbc(
            url=JDBC["url"],
            table="public.declaration_dim",   # ⬅️ eksplicitna shema + ime
            properties=JDBC["properties"]
        )

    # Read‑back verifikacija da tablica stvarno postoji u bazi i ima retke
    check_df = spark.read.jdbc(
        JDBC["url"], "public.declaration_dim", properties=JDBC["properties"]
    )
    print("public.declaration_dim rows (read-back):", check_df.count())
    check_df.printSchema()

    print("Dimenzijska tablica 'public.declaration_dim' uspješno spremljena.")
    spark.stop()
