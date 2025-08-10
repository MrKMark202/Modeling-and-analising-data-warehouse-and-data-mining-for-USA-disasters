# ⭐ Spark ETL – Kreiranje `Disaster_FACT` tablice

## 🎯 Cilj
Fakt‑tablica **Disaster_FACT** povezuje sve dimenzije (State, Disaster, Declaration, Incident_Dates) i sprema dvije mjere:

* `deaths` – broj smrtnih slučajeva
* (implicitno) broj redova = broj deklaracija

Izvori podataka:
* **80 %** – već učitani OLTP podaci u bazi (`State`, `Disaster`, `Declaration`)
* **20 %** – preostali CSV `US_DISASTERS_PROCESSED_20.csv`

Ukupan očekivani broj redova nakon spajanja ≈ **55 684** (80 % + 20 %).

---

## 📂 Datoteka: `fact_table.py`

### 1. Unicode izlaz
```python
sys.stdout.reconfigure(encoding="utf-8")
```
Kako bi terminal pravilno prikazivao hrvatske znakove.

---

### 2. Spark helper
```python
SparkSession.builder \
    .appName("US_DISASTER_SCHEMA") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
```
Četiri shuffle‑particije su OK za ~60 k redova.

---

## 🏗️ Dio A – Funkcija `make_staging_fact()`

```python
csv_df  = spark.read.csv(csv_path, header=True, inferSchema=True) ...
```
* Iz CSV‑a uzima samo potrebne stupce + **`country_name`** (county) – bitno za mapiranje u `State_DIM`.

```python
decl = spark.read.jdbc(... '"Declaration"' ...)
state = spark.read.jdbc(... '"State"' ...)
disaster = spark.read.jdbc(... '"Disaster"' ...)
```
* Čitamo OLTP tablice i mapiramo `state_fk` i `disaster_fk` na stvarne vrijednosti.

```python
fact80 = decl.join(state, "state_fk") \
           .join(disaster, "disaster_fk") ...
```
Dobijamo **80 %** staging set s istim kolonama kao `csv_df`.

```python
staging_fact = fact80.unionByName(csv_df)
```
Rezultat ≈ 55 684 redova:
* Duplikati su dozvoljeni (svaka deklaracija se zadržava).

---

## 🏗️ Dio B – Funkcija `build_fact()`

### 1. Učitavanje dimenzija
```python
state_dim      = spark.read.jdbc(... "state_dim"      ...)
disaster_dim   = spark.read.jdbc(... "disaster_dim"   ...)
declaration_dim = spark.read.jdbc(... "declaration_dim" ...)
dates_dim      = spark.read.jdbc(... "incident_datesdim" ...)
```
Svaka dimenzija sadrži **surrogate ključ** + prirodni ključ(eve) potrebne za join.

### 2. JOIN logika
```python
coalesce(col("s.state_tk"), UNKNOWN).alias("state_tk")
```
* Ako ne postoji match → koristi `0` (UNKNOWN). Time **nikad** ne gubimo redove.

| Redoslijed join‑ova | Ključ                                    |
|-------------------- | ---------------------------------------- |
| **State**           | `country_name` (u velikim slovima)      |
| **Disaster**        | `disaster_number`                       |
| **Declaration**     | `declaration_request_number`            |
| **Incident_Dates**  | `(incident_begin_date, incident_end_date)` |

### 3. Odabir mjera
```python
.select(..., col("f.deaths").alias("deaths"))
```
Za sada imamo samo jednu mjeru – `deaths`.

### 4. Surrogate ključ fakt‑reda
```python
row_number().over(Window.orderBy("state_tk", "disaster_tk", "declaration_tk", "incident_dates_tk"))
```
Deterministički generira `disaster_fact_tk`.

---

## 💾 Pisanje u bazu
```python
fact_df.write.jdbc(url, "disaster_fact", mode="overwrite", properties=props)
```
Tablica se potpuno prepiše pri svakom pokretanju ETL‑a (SCD‑type 0 za fact).

---

## 📊 Očekivani output
* `Staging_fact rows:` ≈ 55 684
* `Disaster_FACT rows:` jednako stagingu (jer UNKNOWN sprečava gubitke)

---

## 📝 Best‑practice napomene
1. Ako dodaš nove mjere (npr. `injuries`, `damage_usd`) – samo proširi `.select()` u `build_fact()`.
2. Za velike clustere povećaj `spark.sql.shuffle.partitions` kako bi raspodijelio workload.
3. **Modeliranje**: ovu tablicu možeš agregirati na razine (state, godina, incident_type) za OLAP izvještavanje.

---