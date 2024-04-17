from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from config import SQLALCHEMY_DATABASE_URI

# Create the engine and connect to the SQLite database
engine = create_engine(SQLALCHEMY_DATABASE_URI)
Base = declarative_base()
Base.metadata.bind = engine
Session = sessionmaker(bind=engine)

# Define SQLAlchemy models
class AllLines(Base):
    __tablename__ = 'all_betting_lines'
    id = Column(String, primary_key=True)
    sport = Column(String)
    home_team = Column(String)
    away_team = Column(String)
    start_time = Column(DateTime)
    sportsbook = Column(String)
    outcome = Column(String)
    decimal_odds = Column(Float)
    update_time = Column(DateTime)

class AvgOdds(Base):
    __tablename__ = 'avg_odds'
    id = Column(String, primary_key=True)
    sport = Column(String)
    home_team = Column(String)
    away_team = Column(String)
    start_time = Column(DateTime)
    outcome = Column(String)
    decimal_odds = Column(Float)
    update_time = Column(DateTime)
    
class PlusEvBets(Base):
    __tablename__ = 'plus_ev_bets'
    id = Column(String, primary_key=True)
    sport = Column(String)
    start_time = Column(DateTime)
    home_team = Column(String)
    away_team = Column(String)
    outcome = Column(String)
    sportsbook = Column(String)
    decimal_odds = Column(Float)
    avg_odds = Column(Float)
    best_odds_update_time = Column(DateTime)
    avg_odds_update_time = Column(DateTime)
    mean_implied_probability = Column(Float)
    best_implied_probability = Column(Float)
    thresh = Column(Float)
    predicted_probability = Column(Float)
    kelly = Column(Float)
    half_kelly = Column(Float)



if __name__ == '__main__':
    # Create all defined tables in the database
    session = Session()
    Base.metadata.create_all(engine)
    session.commit()
    session.close()
    print('Tables created successfully')
    
