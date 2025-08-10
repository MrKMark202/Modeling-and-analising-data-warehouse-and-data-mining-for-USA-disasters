# 📅 Spark ETL – Kreiranje `Incident_datesDIM` dimenzijske tablice

## 🎯 Cilj

Dimenzija **Incident\_datesDIM** opisuje trajanje FEMA incidenta:

| Stupac                | Opis                                                       |
| --------------------- | ---------------------------------------------------------- |
| `incident_dates_tk`   | Surrogate ključ (1 … n)                                    |
| `version`             | SCD verzija (trenutno 1)                                   |
| `date_from`           | Datum kada red postaje aktivan                             |
| `date_to`             | Datum kraja (NULL = još aktivan)                           |
| `incident_begin_date` | Početni datum incidenta                                    |
| `incident_end_date`   | Završni datum incidenta                                    |
| `incident_duration`   | Trajanje u satima (ako nedostaje ‑ izračuna se fallbackom) |

> **Prirodni ključ**: (`incident_begin_date`, `incident_end_date`)
>
> Dodajemo i **UNKNOWN red** (`incident_dates_tk = 0`) za nepovezane činjenice.

---

## 🗂️ Arhitektura koda

### 1. Unicode izlaz

```python
sys.stdout.reconfigure(encoding="utf-8")
```

Omogućava prikaz hrvatskih znakova u Windows/VS Code terminalu.

---

### 2. Spark helper

```python
def get_spark_session():
    return (SparkSession.builder
            .appName("US_DISASTER_SCHEMA")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate())
```

- Pokreće Spark aplikaciju s malim brojem shuffle‑particija (4) ‑ dovoljno za \~60 k redova.

---

### 3. Transform funkcija `transform_incident_dates_dim()`

#### 3.1 Extract iz baze (80 %)

```python
spark.read.jdbc(... '"Disaster"' ...)
    .select(to_date(col("incident_begin_date")).alias("incident_begin_date"),
            to_date(col("incident_end_date")).alias("incident_end_date"),
            col("incident_duration").cast("int"))
```

- `` – skida vremensku zonu / vrijeme → čisti samo datum.
- `incident_duration` se pretvara u `int` (sati).

#### 3.2 Extract iz CSV‑a (20 %)

Ista schema, isti `select`. Time je `unionByName` trivijalan.

#### 3.3 Union + dedup

```python
all_dates = (db.unionByName(csv)
             .dropna(subset=["incident_begin_date", "incident_end_date"])
             .withColumn("incident_duration", coalesce(
                     col("incident_duration"),                  # ako postoji
                     datediff(col("incident_end_date"),         # inače izračunaj
                              col("incident_begin_date")) * 24
             ).cast("int"))
             .dropDuplicates(["incident_begin_date", "incident_end_date"]))
```

- `coalesce` ⇒ ako je trajanje NULL → izračunaj razliku dana × 24 = sati.
- `dropDuplicates` jamči **jedan red po (begin, end)**.

#### 3.4 Surrogate ključ + SCD

```python
w = Window.orderBy("incident_begin_date", "incident_end_date")
dim = (all_dates
       .withColumn("incident_dates_tk", row_number().over(w))
       .withColumn("version", lit(1))
       .withColumn("date_from", current_timestamp())
       .withColumn("date_to", lit(None).cast("timestamp")))
```

- Surrogate ključ dobiva stabilan redoslijed (sortirano po datumima).

#### 3.5 UNKNOWN red

```python
unknown = spark.createDataFrame([(0,1,datetime.utcnow(),None,None,None,0)], schema=dim.schema)
final_df = unknown.unionByName(dim)
```

- `incident_dates_tk = 0`, sve NULL osim trajanja = 0.

---

### 4. Pisanje u DWH

```python
final_df.write.jdbc(url, "incident_datesdim", mode="overwrite", properties=props)
```

- Tablica se overrida pri svakom pokretanju.

---

## 📊 Očekivani rezultat

```text
INCIDENT_DATES_DIM redaka: ≈ 3 605   # 3 604 stvarnih + 1 UNKNOWN
```

---

## 📝 Napomene i best‑practice

1. **Fallback duration** – računanje `datediff * 24` pretpostavlja da su datumi uključivi i da nema vremenske zone; za preciznije trajanje koristi `unix_timestamp()`.
2. Ako će se datumi ažurirati (SCD‑2) moraš:
   - zatvoriti stari red (`date_to = NOW()`)
   - unijeti novi red s (`version += 1`).
3. Imaš `surrogate` i `natural` ključ → možeš provjeriti povijesnu promjenu duljine incidenta.

---