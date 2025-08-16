import sys
sys.stdout.reconfigure(encoding="utf-8")

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, trim, upper, to_date, row_number,
    coalesce as s_coalesce, when, datediff, current_timestamp
)

# ---------------- Spark helper ---------------- #
def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("US_DISASTER_SCHEMA")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

# ---------------- Normalizer ---------------- #
def norm_str(c):
    return upper(trim(c))

# ---------------- STAGING (80% DB ∪ 20% CSV) ---------------- #
def make_staging_fact(spark: SparkSession, jdbc: dict, csv_path: str):
    # ---- 20% CSV ----
    csv_df = (
        spark.read.csv(csv_path, header=True, inferSchema=True)
        .select(
            norm_str(col("state_name")).alias("state_name"),
            norm_str(col("county_name")).alias("county_name"),
            norm_str(col("incident_type")).alias("incident_type"),
            col("declaration_request_number").cast("int").alias("declaration_request_number"),
            to_date(col("incident_begin_date")).alias("incident_begin_date"),
            to_date(col("incident_end_date")).alias("incident_end_date"),
            col("incident_duration").cast("int").alias("incident_duration"),
            col("deaths").cast("int").alias("deaths"),
        )
    )

    # ---- 80% iz baze: Declaration → County → State; + Disaster ----
    decl = (
        spark.read.jdbc(jdbc["url"], '"Declaration"', properties=jdbc["properties"])
        .select(
            col("declaration_request_number").cast("int").alias("declaration_request_number"),
            to_date(col("incident_begin_date")).alias("incident_begin_date"),
            to_date(col("incident_end_date")).alias("incident_end_date"),
            col("incident_duration").cast("int").alias("incident_duration"),
            col("deaths").cast("int").alias("deaths"),
            col("county_fk"),
            col("disaster_fk"),
        )
    )

    county = (
        spark.read.jdbc(jdbc["url"], '"County"', properties=jdbc["properties"])
        .select(
            col("id").alias("county_fk"),
            norm_str(col("county_name")).alias("county_name"),
            col("state_fk")
        )
    )

    state = (
        spark.read.jdbc(jdbc["url"], '"State"', properties=jdbc["properties"])
        .select(
            col("id").alias("state_fk"),
            norm_str(col("state_name")).alias("state_name")
        )
    )

    disaster = (
        spark.read.jdbc(jdbc["url"], '"Disaster"', properties=jdbc["properties"])
        .select(
            col("id").alias("disaster_fk"),
            norm_str(col("incident_type")).alias("incident_type")
        )
    )

    fact80 = (
        decl.join(county, "county_fk", "left")
            .join(state, "state_fk", "left")
            .join(disaster, "disaster_fk", "left")
            .select(
                "state_name",
                "county_name",
                "incident_type",
                "declaration_request_number",
                "incident_begin_date",
                "incident_end_date",
                "incident_duration",
                "deaths",
            )
    )

    # ---- UNION 80% + 20% ----
    staging = fact80.unionByName(csv_df)

    # Ako duration fali, izračunaj ga iz datuma
    staging = staging.withColumn(
        "incident_duration",
        s_coalesce(
            col("incident_duration"),
            when(col("incident_begin_date").isNotNull() & col("incident_end_date").isNotNull(),
                 datediff(col("incident_end_date"), col("incident_begin_date"))
            ).otherwise(lit(None)).cast("int")
        )
    )

    print("Staging_fact rows:", staging.count())
    return staging

# ---------------- Backfill za incident_datesdim ---------------- #
def backfill_incident_dates_dim(spark: SparkSession, jdbc: dict, staging_df):
    # 1) postojeća dimenzija
    dim = (
        spark.read.jdbc(jdbc["url"], "incident_datesdim", properties=jdbc["properties"])
        .select(
            col("incident_dates_tk"),
            to_date(col("incident_begin_date")).alias("ib_dim"),
            to_date(col("incident_end_date")).alias("ie_dim"),
            col("incident_duration").cast("int").alias("dur_dim"),
            col("version"),
            col("date_from"),
            col("date_to"),
        )
    )

    # 2) staging kombinacije s ujednačenim trajanjem
    staging_keys = (
        staging_df
        .withColumn(
            "dur_std",
            s_coalesce(
                col("incident_duration"),
                datediff(col("incident_end_date"), col("incident_begin_date")).cast("int")
            )
        )
        .select(
            col("incident_begin_date").alias("ib"),
            col("incident_end_date").alias("ie"),
            col("dur_std").alias("dur")
        )
        .dropna(subset=["ib", "ie"])
        .dropDuplicates(["ib", "ie", "dur"])
    )

    # 3) nedostajuće kombinacije (anti-join)
    missing = (
        staging_keys.alias("s")
        .join(
            dim.select("ib_dim","ie_dim","dur_dim").dropDuplicates(),
            (col("s.ib")==col("ib_dim")) & (col("s.ie")==col("ie_dim")) &
            (
                (col("s.dur").isNull() & col("dur_dim").isNull()) |
                (col("s.dur")==col("dur_dim"))
            ),
            "left_anti"
        )
    )

    cnt_missing = missing.count()
    if cnt_missing > 0:
        max_tk_row = spark.read.jdbc(jdbc["url"], "incident_datesdim", properties=jdbc["properties"])\
                            .agg({"incident_dates_tk": "max"}).collect()[0]
        max_tk = max_tk_row[0] if max_tk_row and max_tk_row[0] is not None else 0

        w = Window.orderBy("ib","ie","dur")
        to_append = (
            missing.repartition(8)  # opcionalno: malo ubrza row_number
            .withColumn("incident_dates_tk", (lit(max_tk) + row_number().over(w)).cast("int"))
            .withColumn("version", lit(1))
            .withColumn("date_from", current_timestamp())  # NOT NULL
            .withColumn("date_to",   lit("2200-01-01 00:00:00").cast("timestamp"))
            .select(
                "incident_dates_tk",
                col("ib").alias("incident_begin_date"),
                col("ie").alias("incident_end_date"),
                col("dur").alias("incident_duration"),
                "version", "date_from", "date_to"
            )
        )

        to_append.write.jdbc(
            jdbc["url"], "incident_datesdim", mode="append", properties=jdbc["properties"]
        )
        print(f"[incident_datesdim] Backfilled {cnt_missing} new rows.")
    else:
        print("[incident_datesdim] No backfill needed.")

    # 5) vrati svježu dimenziju za join
    return (
        spark.read.jdbc(jdbc["url"], "incident_datesdim", properties=jdbc["properties"])
        .select(
            col("incident_dates_tk"),
            to_date(col("incident_begin_date")).alias("ib_dim"),
            to_date(col("incident_end_date")).alias("ie_dim"),
            col("incident_duration").cast("int").alias("dur_dim"),
        )
        .dropDuplicates(["ib_dim","ie_dim","dur_dim"])
    )

# ---------------- BUILD FACT ---------------- #
def build_fact(spark: SparkSession, jdbc: dict, csv_path: str):
    staging = make_staging_fact(spark, jdbc, csv_path)

    # >>> backfill incident_datesdim na temelju staginga <<<
    dates_dim = backfill_incident_dates_dim(spark, jdbc, staging)

    # ---- DIM tablice ----
    state_dim = (
        spark.read.jdbc(jdbc["url"], "state_dim", properties=jdbc["properties"])
        .select(
            col("state_tk"),
            norm_str(col("state_name")).alias("state_name_dim"),
        )
        .dropDuplicates(["state_name_dim"])
    )

    county_dim = (
        spark.read.jdbc(jdbc["url"], "county_dim", properties=jdbc["properties"])
        .select(
            col("county_tk"),
            norm_str(col("county_name")).alias("county_name_dim"),
        )
        .dropDuplicates(["county_name_dim"])
    )

    disaster_dim = (
        spark.read.jdbc(jdbc["url"], "disaster_dim", properties=jdbc["properties"])
        .select(
            col("disaster_tk"),
            norm_str(col("incident_type")).alias("incident_type_dim"),
        )
        .dropDuplicates(["incident_type_dim"])
    )

    declaration_dim = (
        spark.read.jdbc(jdbc["url"], "declaration_dim", properties=jdbc["properties"])
        .select(
            col("declaration_tk"),
            col("declaration_request_number").cast("int").alias("drn_dim"),
        )
        .dropDuplicates(["drn_dim"])
    )

    UNKNOWN = lit(0)

    fact_df = (
        staging.alias("f")
        # State po nazivu
        .join(
            state_dim.alias("s"),
            norm_str(col("f.state_name")) == col("s.state_name_dim"),
            "left"
        )
        # County po nazivu
        .join(
            county_dim.alias("c"),
            norm_str(col("f.county_name")) == col("c.county_name_dim"),
            "left"
        )
        # Disaster po incident_type
        .join(
            disaster_dim.alias("d"),
            norm_str(col("f.incident_type")) == col("d.incident_type_dim"),
            "left"
        )
        # Declaration po request number
        .join(
            declaration_dim.alias("dc"),
            col("f.declaration_request_number") == col("dc.drn_dim"),
            "left"
        )
        # Dates po (begin, end) + tolerantno na duration
        .join(
            dates_dim.alias("id"),
            (col("f.incident_begin_date") == col("id.ib_dim")) &
            (col("f.incident_end_date")   == col("id.ie_dim")) &
            (
                (col("id.dur_dim").isNull() & col("f.incident_duration").isNull()) |
                (s_coalesce(col("f.incident_duration"), lit(-9999)) ==
                 s_coalesce(col("id.dur_dim"), lit(-9999)))
            ),
            "left"
        )
        .select(
            s_coalesce(col("s.state_tk"),        UNKNOWN).alias("state_tk"),
            s_coalesce(col("c.county_tk"),       UNKNOWN).alias("county_tk"),
            s_coalesce(col("d.disaster_tk"),     UNKNOWN).alias("disaster_tk"),
            s_coalesce(col("dc.declaration_tk"), UNKNOWN).alias("declaration_tk"),
            s_coalesce(col("id.incident_dates_tk"), UNKNOWN).alias("incident_dates_tk"),
            col("f.deaths").cast("int").alias("deaths"),
        )
    )

    # Surrogate ključ za fact (deterministički po FK-ovima)
    fact_df = fact_df.withColumn(
        "disaster_fact_tk",
        row_number().over(
            Window.orderBy("state_tk", "county_tk", "disaster_tk", "declaration_tk", "incident_dates_tk")
        )
    ).select(
        "disaster_fact_tk",
        "state_tk", "county_tk", "disaster_tk", "declaration_tk", "incident_dates_tk",
        "deaths"
    )

    # Sanity report
    total = fact_df.count()
    unk_state  = fact_df.filter(col("state_tk") == 0).count()
    unk_county = fact_df.filter(col("county_tk") == 0).count()
    unk_dis    = fact_df.filter(col("disaster_tk") == 0).count()
    unk_decl   = fact_df.filter(col("declaration_tk") == 0).count()
    unk_dates  = fact_df.filter(col("incident_dates_tk") == 0).count()
    print(f"FACT rows: {total} | UNKNOWNs — state:{unk_state}, county:{unk_county}, disaster:{unk_dis}, declaration:{unk_decl}, dates:{unk_dates}")

    return fact_df

# ---------------- MAIN ---------------- #
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
