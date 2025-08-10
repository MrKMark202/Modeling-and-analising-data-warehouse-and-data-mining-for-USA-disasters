# 🧩 Spark ETL – Kreiranje `State_DIM` dimenzijske tablice

## 🎯 Cilj  
Kreirati **dimenzijsku tablicu** `state_dim` na razini *county* (`country_name`).  
Tablica dobiva:

| Stupac            | Opis                                          |
|-------------------|-----------------------------------------------|
| `state_tk`        | Surrogate ključ (1 … n)                       |
| `version`         | SCD verzija (zasad 1)                         |
| `date_from`       | Datum početka valjanosti reda                 |
| `date_to`         | Datum kraja valjanosti (NULL = aktivan)       |
| `state_name`      | Naziv savezne države (VELIKA slova)           |
| `country_name`    | Naziv county‑ja (prirodni ključ)              |

---

## 🚀 1. SparkSession
```python
spark = (SparkSession.builder
         .appName("US_DISASTER_SCHEMA")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
```
Manji broj particija smanjuje overhead za dataset od ~60 k redova.

---

## 🗄️ 2. Extract
### 2.1 Iz OLTP baze
```python
db_states = spark.read.jdbc(... '"State"' ...) \
           .select(upper(trim(col("name"))).alias("state_name"),
                   upper(trim(col("country_name"))).alias("country_name"))
```
### 2.2 Iz CSV‑a (20 %)
```python
csv_states = spark.read.csv(...).select(
                 upper(trim(col("state"))).alias("state_name"),
                 upper(trim(col("country_name"))).alias("country_name"))
```

---

## 🔗 3. Union & dedup
```python
all_counties = (db_states.unionByName(csv_states)
                         .filter(col("country_name").isNotNull())
                         .dropDuplicates(["country_name"]))
```
Po jednom redu za svaki county.

---

## 🔢 4. Surrogate ključ + SCD
```python
w = Window.orderBy("country_name")
dim = (all_counties
       .withColumn("state_tk", row_number().over(w))
       .withColumn("version", lit(1))
       .withColumn("date_from", current_timestamp())
       .withColumn("date_to", lit(None).cast("timestamp")))
```

---

## ❓ 5. UNKNOWN red
```python
unknown = spark.createDataFrame(
    [(0,1,datetime.utcnow(),None,"UNKNOWN","UNKNOWN")],
    schema=dim.schema)
final_df = unknown.unionByName(dim)
```

---

## 💾 6. Spremanje
```python
final_df.write.jdbc(url, "state_dim", mode="overwrite", properties=props)
```

Rezultat: **≈ 3 316** redova (3 315 county‑ja + UNKNOWN).

---