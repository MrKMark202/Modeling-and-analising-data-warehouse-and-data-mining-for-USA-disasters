"""
ETL skripta – **stabilna verzija**
=================================
Popravci na temelju zadnjih simptoma:
1. **Detaljan logging**: broj redaka nakon svakog koraka + kolike NULL‑ove imamo.
2. **CSV death‑ovi se zadržavaju** i spajaju s onima iz baze (`coalesce`).
3. **Dedup je privremeno izbačen** – prvo osiguravamo da sve prođe; kasnije možemo vratiti strožiji ključ.
4. **state_dim** koristi kolonu `state_name`.
5. Sve NULL vrijednosti u `deaths` pretvaramo u **0** (možeš promijeniti sentinel).
"""

import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    upper,
    to_date,
    to_timestamp,
    coalesce,
    row_number,
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
#  Date‑parsing helper                                               #
# ------------------------------------------------------------------ #

def norm_date(col_name: str, fmt: str):
    return to_date(to_timestamp(col(col_name), fmt))

# ------------------------------------------------------------------ #
#  Build staging_fact                                               #
# ------------------------------------------------------------------ #

def make_staging_fact(spark: SparkSession, jdbc: dict, csv_path: str):
    """Vrati staging DataFrame + basic QA logove."""

    # 20 % CSV ------------------------------------------------------
    csv_df = (
        spark.read.csv(csv_path, header=True, inferSchema=True)
        .withColumn("country_name", trim(col("country_name")))
        .withColumn("state",         trim(col("state")))
        .withColumn("incident_begin_date", norm_date("incident_begin_date", "MM/dd/yyyy"))
        .withColumn("incident_end_date",   norm_date("incident_end_date",   "MM/dd/yyyy"))
        .withColumn("deaths", col("deaths").cast("int"))
        .select(
            "country_name",
            "state",
            "disaster_number",
            "declaration_request_number",
            "incident_begin_date",
            "incident_end_date",
            "deaths",
        )
    )
    print("CSV rows:", csv_df.count())

    # 80 % DB -------------------------------------------------------
    decl = (
        spark.read.jdbc(jdbc["url"], '"Declaration"', properties=jdbc["properties"])
        .select("declaration_request_number", "state_fk", "disaster_fk")
    )
    state = (
        spark.read.jdbc(jdbc["url"], '"State"', properties=jdbc["properties"])
        .select(
            col("id").alias("state_fk"),
            trim(col("country_name")).alias("country_name"),
            trim(col("name")).alias("state"),
        )
    )
    disaster = (
        spark.read.jdbc(jdbc["url"], '"Disaster"', properties=jdbc["properties"])
        .withColumn("incident_begin_date", norm_date("incident_begin_date", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("incident_end_date",   norm_date("incident_end_date",   "yyyy-MM-dd HH:mm:ss"))
        .withColumn("deaths", col("deaths").cast("int"))
        .select(
            col("id").alias("disaster_fk"),
            "disaster_number",
            "incident_begin_date",
            "incident_end_date",
            "deaths",
        )
    )

    fact80 = (
        decl.join(state, "state_fk", "left")
            .join(disaster, "disaster_fk", "left")
            .select(
                "country_name",
                "state",
                "disaster_number",
                "declaration_request_number",
                "incident_begin_date",
                "incident_end_date",
                "deaths",
            )
    )
    print("DB rows (fact80):", fact80.count())

    staging_fact = fact80.unionByName(csv_df)

    # QA: koliko NULL datuma?
    null_dates = staging_fact.filter(col("incident_begin_date").isNull() | col("incident_end_date").isNull()).count()
    print("Rows with NULL dates in staging:", null_dates)

    print("Staging rows total:", staging_fact.count())
    return staging_fact

# ------------------------------------------------------------------ #
#  Build Disaster_FACT                                              #
# ------------------------------------------------------------------ #

def build_fact(spark: SparkSession, jdbc: dict, csv_path: str):
    staging = make_staging_fact(spark, jdbc, csv_path)

    # ---------------- Dimenzije -----------------------------------
    state_dim = (
        spark.read.jdbc(jdbc["url"], "state_dim", properties=jdbc["properties"])
        .select(
            "state_tk",
            upper(trim(col("country_name"))).alias("country_name"),
            upper(trim(col("state_name"))).alias("state"),  # kolona u dimenziji
        )
    )
    disaster_dim = (
        spark.read.jdbc(jdbc["url"], "disaster_dim", properties=jdbc["properties"])
        .select("disaster_tk", "disaster_number")
    )
    declaration_dim = (
        spark.read.jdbc(jdbc["url"], "declaration_dim", properties=jdbc["properties"])
        .select("declaration_tk", "declaration_request_number")
    )
    # ----------------- DIMENZIJA DATUMA ---------------------------------
    dates_dim = (
        spark.read.jdbc(
            jdbc["url"],
            "incident_datesdim",
            properties=jdbc["properties"]
        )
        .withColumn("incident_begin_date", to_date(col("incident_begin_date")))
        .withColumn("incident_end_date",   to_date(col("incident_end_date")))
        .select("incident_dates_tk", "incident_begin_date", "incident_end_date")
    )  # ⇐ OVA zagrada zatvara cijeli blok – nema ništa poslije!

    UNKNOWN = lit(0)

    fact_df = (
        staging.alias("f")
        # ---- STATE ------------------------------------------------
        .join(
            state_dim.alias("s"),
            (upper(trim(col("f.country_name"))) == col("s.country_name")) &
            (upper(trim(col("f.state")))        == col("s.state")),
            "left",
        )
        # ---- DISASTER --------------------------------------------
        .join(
            disaster_dim.alias("d"),
            col("f.disaster_number") == col("d.disaster_number"),
            "left",
        )
        # ---- DECLARATION -----------------------------------------
        .join(
            declaration_dim.alias("dc"),
            col("f.declaration_request_number") == col("dc.declaration_request_number"),
            "left",
        )
        # ---- DATES -----------------------------------------------
        .join(
            dates_dim.alias("id"),
            (col("f.incident_begin_date") == col("id.incident_begin_date")) &
            (col("f.incident_end_date")   == col("id.incident_end_date")),
            "left",
        )
        .select(
            coalesce(col("s.state_tk"), UNKNOWN).alias("state_tk"),
            coalesce(col("d.disaster_tk"), UNKNOWN).alias("disaster_tk"),
            coalesce(col("dc.declaration_tk"), UNKNOWN).alias("declaration_tk"),
            coalesce(col("id.incident_dates_tk"), UNKNOWN).alias("incident_dates_tk"),
            coalesce(col("f.deaths"), lit(0)).alias("deaths"),  # NULL → 0
        )
    )

    fact_df = fact_df.withColumn(
        "disaster_fact_tk",
        row_number().over(
            Window.orderBy(
                "state_tk", "disaster_tk", "declaration_tk", "incident_dates_tk"
            )
        ),
    )

    print("Fact rows:", fact_df.count())
    return fact_df

# ------------------------------------------------------------------ #
#  Main                                                             #
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

    fact_df = build_fact(spark, JDBC, CSV_20)

    fact_df.write.jdbc(
        JDBC["url"], "disaster_fact", mode="overwrite", properties=JDBC["properties"]
    )

    print("Tablica 'disaster_fact' uspješno spremljena.")
    spark.stop()
