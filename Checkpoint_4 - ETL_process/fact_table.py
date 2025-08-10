import sys, datetime
sys.stdout.reconfigure(encoding="utf-8")

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, trim, upper, to_date,
    coalesce, row_number
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
#  Build staging_fact (80 % DB  +  20 % CSV)                         #
# ------------------------------------------------------------------ #
def make_staging_fact(spark: SparkSession, jdbc: dict, csv_path: str):
    """Staging = 80 % iz baze  ∪  20 % iz CSV-a (≈55 684 redaka)."""

    # 20 % CSV --------------------------------------------------------------
    csv_df = (
        spark.read.csv(csv_path, header=True, inferSchema=True)
        .select(
            trim(col("country_name")).alias("country_name"),   # <<< NOVO
            trim(col("state")).alias("state"),                # (može poslužiti za kontrolu)
            col("disaster_number"),
            col("declaration_request_number"),
            to_date(col("incident_begin_date")).alias("incident_begin_date"),
            to_date(col("incident_end_date")).alias("incident_end_date"),
            col("deaths").cast("int"),
        )
    )

    # 80 % iz baze ----------------------------------------------------------
    decl = spark.read.jdbc(jdbc["url"], '"Declaration"', properties=jdbc["properties"]) \
        .select("declaration_request_number", "state_fk", "disaster_fk")

    state = spark.read.jdbc(jdbc["url"], '"State"', properties=jdbc["properties"]) \
        .select(
            col("id").alias("state_fk"),
            trim(col("country_name")).alias("country_name"),  # <<< NOVO
            trim(col("name")).alias("state")
        )

    disaster = spark.read.jdbc(jdbc["url"], '"Disaster"', properties=jdbc["properties"]) \
        .select(
            col("id").alias("disaster_fk"),
            col("disaster_number"),
            to_date(col("incident_begin_date")).alias("incident_begin_date"),
            to_date(col("incident_end_date")).alias("incident_end_date"),
            col("deaths").cast("int"),
        )

    fact80 = (
        decl.join(state, "state_fk", "left")
            .join(disaster, "disaster_fk", "left")
            .select(
                "country_name",                                  # <<< NOVO
                "state",
                "disaster_number",
                "declaration_request_number",
                "incident_begin_date",
                "incident_end_date",
                "deaths",
            )
    )

    staging_fact = fact80.unionByName(csv_df)
    print("Staging_fact rows:", staging_fact.count())          # kontrola
    return staging_fact

# ------------------------------------------------------------------ #
#  Build Disaster_FACT                                               #
# ------------------------------------------------------------------ #
def build_fact(spark: SparkSession, jdbc: dict, csv_path: str):
    staging = make_staging_fact(spark, jdbc, csv_path)

    # ---------------- Dimenzije -------------------------------------------
    state_dim = spark.read.jdbc(jdbc["url"], "state_dim", properties=jdbc["properties"]) \
        .select(
            "state_tk",
            upper(trim(col("country_name"))).alias("country_name")   # <<< NOVO
        )

    disaster_dim = spark.read.jdbc(jdbc["url"], "disaster_dim", properties=jdbc["properties"]) \
        .select("disaster_tk", "disaster_number")

    declaration_dim = spark.read.jdbc(jdbc["url"], "declaration_dim", properties=jdbc["properties"]) \
        .select("declaration_tk", "declaration_request_number")

    dates_dim = spark.read.jdbc(jdbc["url"], "incident_datesdim", properties=jdbc["properties"]) \
        .select("incident_dates_tk", "incident_begin_date", "incident_end_date")

    UNKNOWN = lit(0)

    fact_df = (
        staging.alias("f")
        # ----------- JOIN State (po county) -------------------------------
        .join(
            state_dim.alias("s"),
            upper(trim(col("f.country_name"))) == col("s.country_name"),   # <<< NOVO
            "left"
        )
        # ----------- JOIN Disaster ---------------------------------------
        .join(
            disaster_dim.alias("d"),
            col("f.disaster_number") == col("d.disaster_number"),
            "left"
        )
        # ----------- JOIN Declaration ------------------------------------
        .join(
            declaration_dim.alias("dc"),
            col("f.declaration_request_number") == col("dc.declaration_request_number"),
            "left"
        )
        # ----------- JOIN Dates ------------------------------------------
        .join(
            dates_dim.alias("id"),
            (col("f.incident_begin_date") == col("id.incident_begin_date")) &
            (col("f.incident_end_date")   == col("id.incident_end_date")),
            "left"
        )
        # ----------- Select FKs + mjera -----------------------------------
        .select(
            coalesce(col("s.state_tk"), UNKNOWN).alias("state_tk"),
            coalesce(col("d.disaster_tk"), UNKNOWN).alias("disaster_tk"),
            coalesce(col("dc.declaration_tk"), UNKNOWN).alias("declaration_tk"),
            coalesce(col("id.incident_dates_tk"), UNKNOWN).alias("incident_dates_tk"),
            col("f.deaths").alias("deaths"),
        )
    )

    # ----------- Sekvencijalni surrogate za FACT --------------------------
    fact_df = fact_df.withColumn(
        "disaster_fact_tk",
        row_number().over(
            Window.orderBy(
                "state_tk", "disaster_tk", "declaration_tk", "incident_dates_tk"
            )
        )
    )

    print("Disaster_FACT rows:", fact_df.count())
    return fact_df

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

    fact_df = build_fact(spark, JDBC, CSV_20)

    fact_df.write.jdbc(
        JDBC["url"], "disaster_fact", mode="overwrite", properties=JDBC["properties"]
    )

    print("Tablica 'Disaster_FACT' uspješno spremljena.")
    spark.stop()
