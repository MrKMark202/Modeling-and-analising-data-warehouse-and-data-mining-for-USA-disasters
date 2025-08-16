from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker

# Kreiranje SparkSession-a
spark = SparkSession.builder.appName("US_Disasters").getOrCreate()

# Kreiranje konekcije na PostgreSQL bazu
DATABASE_URL = "postgresql://postgres:1234@localhost:5432/us_disasters2"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# Definicija tablica

class State(Base):
    __tablename__ = 'State'
    id = Column(Integer, primary_key=True, autoincrement=True)
    state_name = Column(String, nullable=False, unique=True)
    
class County(Base):
    __tablename__ = 'County'
    id = Column(Integer, primary_key=True, autoincrement=True)
    county_name = Column(String, nullable=False, unique=True)
    state_fk = Column(Integer, ForeignKey('State.id'), nullable=False)
    

class Disaster(Base):
    __tablename__ = 'Disaster'
    id = Column(Integer, primary_key=True, autoincrement=True)
    incident_type = Column(String, nullable=False, unique=True)

class Declaration(Base):
    __tablename__ = 'Declaration'
    id = Column(Integer, primary_key=True, autoincrement=True)
    declaration_title = Column(String, nullable=False)
    declaration_type = Column(String, nullable=False)
    declaration_date = Column(Date, nullable=False)
    declaration_request_number = Column(Integer, nullable=False)
    incident_begin_date = Column(Date, nullable=False)
    incident_end_date = Column(Date, nullable=False)
    incident_duration = Column(Integer, nullable=False)
    ih_program_declared = Column(Boolean, nullable=False)
    ia_program_declared = Column(Boolean, nullable=False)
    pa_program_declared = Column(Boolean, nullable=False)
    hm_program_declared = Column(Boolean, nullable=False)
    deaths = Column(Integer, nullable=False)
    disaster_fk = Column(Integer, ForeignKey('Disaster.id'), nullable=False)
    county_fk = Column(Integer, ForeignKey('County.id'), nullable=False)


# Kreiranje tablica
Base.metadata.create_all(engine)

# Funkcija za učitavanje CSV-a i spremanje u bazu pomoću Spark-a
def load_csv_to_db(csv_path):
    df = spark.read.csv(csv_path, header=True, inferSchema=True)
    
    #Punjenje tablice State
    state = df.select("state_name").distinct().collect()
    for row in state:
        existing_state = session.query(State).filter_by(state_name=str(row.state_name)).first()
        if not existing_state:
            session.add(State(state_name=row.state_name))
    session.commit()
    
    state_rows = df.select("state_name").dropna(subset=["state_name"]).distinct().collect()
    existing_states = {s.state_name: s.id for s in session.query(State).all()}

    for r in state_rows:
        sname = str(r.state_name)
        if sname not in existing_states:
            s = State(state_name=sname)
            session.add(s)
            session.flush()
            existing_states[sname] = s.id
    session.commit()

    # --- COUNTY: upiši sve jedinstvene (state_name, county_name) s FK na State ---
    county_rows = (
        df.select("state_name", "county_name")
        .dropna(subset=["state_name", "county_name"])
        .distinct()
        .collect()
    )

    # Ako koristiš UniqueConstraint na (county_name, state_fk), duplikati će ionako pasti;
    # svejedno je dobro provjeriti prije inserta:
    # učitaj postojeće county-je u mapu (county_name, state_fk) -> id
    existing_counties = {}
    for c in session.query(County).all():
        existing_counties[(c.county_name, c.state_fk)] = c.id

    for r in county_rows:
        sname = str(r.state_name)
        cname = str(r.county_name)
        sid = existing_states.get(sname)
        if not sid:
            # ne bi se smjelo dogoditi jer smo States već popunili, ali za svaki slučaj
            s = State(state_name=sname)
            session.add(s); session.flush()
            sid = s.id
            existing_states[sname] = sid

        key = (cname, sid)
        if key not in existing_counties:
            c = County(county_name=cname, state_fk=sid)
            session.add(c)
            session.flush()
            existing_counties[key] = c.id
    session.commit()

    # Punjenje tablice Disaster
    disasters = df.select("incident_type").distinct().collect()             

    for row in disasters:
        session.add(
            Disaster(
                incident_type=row.incident_type,
            )
        )
    session.commit()

    # Punjenje tablice Declaration
    declarations = (
        df.select(
            "declaration_title", 
            "declaration_type", 
            "declaration_date", 
            "declaration_request_number", 
            "incident_begin_date",
            "incident_end_date",
            "incident_duration",
            "ih_program_declared",
            "ia_program_declared",
            "pa_program_declared",
            "hm_program_declared",
            "deaths",
            "county_name",
            "incident_type"
        ).collect()
    )
    
    for row in declarations:
        disaster = session.query(Disaster).filter_by(incident_type=row.incident_type).first()
        county = session.query(County).filter_by(county_name=row.county_name).first()
        if disaster:
            declaration = Declaration(
                declaration_title=row.declaration_title,
                declaration_type=row.declaration_type,
                declaration_date=row.declaration_date,
                declaration_request_number=row.declaration_request_number,
                incident_begin_date=row.incident_begin_date,
                incident_end_date=row.incident_end_date,
                incident_duration=row.incident_duration,
                ih_program_declared=row.ih_program_declared,
                ia_program_declared=row.ia_program_declared,
                pa_program_declared=row.pa_program_declared,
                hm_program_declared=row.hm_program_declared,
                deaths=row.deaths,
                disaster_fk=disaster.id,
                county_fk=county.id
            )
            session.add(declaration)
    session.commit()
    print("Podaci uspješno spremljeni u bazu!")

# Poziv funkcije
load_csv_to_db("data/US_DISASTERS_PROCESSED_80.csv")