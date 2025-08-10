# 🌪️ Spark ETL – Kreiranje `Disaster_DIM` dimenzijske tablice

## 🎯 Cilj
Dimenzija **Disaster_DIM** opisuje svaki FEMA „disaster“ (katastrofu) i koje su vrste federalnih programa (IA, PA, …) aktivirane.

| Stupac                 | Opis                                                       |
|------------------------|------------------------------------------------------------|
| `disaster_tk`          | Surrogate ključ (1 … n)                                    |
| `version`              | SCD‑verzija (trenutno 1)                                   |
| `date_from`            | Datum kada red postaje aktivan                             |
| `date_to`              | Datum kraja (NULL = još aktivan)                           |
| `disaster_number`      | **Prirodni ključ** – FEMA ID (jedinstven)                  |
| `incident_type`        | Vrsta incidenta (Tornado, Blizzard, Hurricane, …)          |
| `*_program_declared`   | Četiri booleana: IH, IA, PA, HM                             |

> `disaster_tk = 0` rezerviran je za **UNKNOWN** red, da se pri LEFT‑JOIN‑u ne gube činjenice.

---

## 🗂️ Struktura skripte

### 1. Unicode izlaz
```python
sys.stdout.reconfigure(encoding="utf-8")
```
Za siguran prikaz hrvatskih znakova u Windows/VS Code terminalu.

---

### 2. Spark helper
```python
def get_spark_session():
    return (SparkSession.builder
            .appName("US_DISASTER_SCHEMA")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate())
```
- 4 shuffle‑particije – dovoljno za
> 8 k redova; manji overhead.

---

### 3. Transform funkcija `transform_disaster_dim()`

#### 3.1 Extract & casting
```python
BOOL_COLS = ["ih_program_declared", "ia_program_declared", "pa_program_declared", "hm_program_declared"]

def cast_bool(df):
    sel = [col("disaster_number"), col("incident_type")]
    sel += [col(c).cast("boolean").alias(c) for c in BOOL_COLS]
    return df.select(*sel)

db_df  = cast_bool(spark.read.jdbc(... '"Disaster"' ...))
csv_df = cast_bool(spark.read.csv(csv_path, header=True, inferSchema=True))
```
- Iz baze (80 %) i CSV‑a (20 %) čitamo potpuno istu strukturu.
- Svi boolean stupci konvertirani su `cast("boolean")` kako bi `unionByName` bio siguran.

#### 3.2 Union + dedup
```python
all_disasters = (db_df.unionByName(csv_df)
                 .dropna(subset=["disaster_number"])
                 .dropDuplicates(["disaster_number"]))
```
- Jamčimo **jedan red za svaki `disaster_number`** (prirodni ključ).

#### 3.3 Surrogate + SCD
```python
w = Window.orderBy("disaster_number")
dim = (all_disasters
       .withColumn("disaster_tk", row_number().over(w))
       .withColumn("version", lit(1))
       .withColumn("date_from", current_timestamp())
       .withColumn("date_to", lit(None).cast("timestamp")))
```
- `row_number()` daje stabilan, deterministički `disaster_tk`.

#### 3.4 Re‑ordering kolona
```python
COL_ORDER = ["disaster_tk", "version", "date_from", "date_to", "disaster_number", "incident_type", *BOOL_COLS]
```
- Garantira da će schema biti uvijek identičnog reda – bitno kod `overwrite`.

#### 3.5 UNKNOWN red
```python
unknown_row = (0, 1, datetime.utcnow(), None, None, None, False, False, False, False)
final_df = spark.createDataFrame([unknown_row], schema=dim.schema).unionByName(dim)
```
- `disaster_tk = 0` i sve booleane `False`.

---

### 4. Pisanje u DWH
```python
final_df.write.jdbc(url, "disaster_dim", mode="overwrite", properties=props)
```
- Svako pokretanje puni svježu dimenziju.

---

## 📊 Očekivani rezultat
```text
DISASTER_DIM redaka: ≈ 4 272   # 4 271 stvarnih + 1 UNKNOWN
```

---

## 📝 Best‑practice napomene
1. U budućnosti ako FEMA promijeni `incident_type` za isti `disaster_number`, trebat ćeš SCD‑2 logiku: zatvoriti stari red i insertati novi s verzijom 2.
2. Boolean polja možeš pretvoriti i u `TINYINT(1)` u bazi radi kompatibilnosti.
3. `surrogate` ≠ `natural`: nikad ne koristi `disaster_number` direktno kao foreign key u fact‑tablici.

---