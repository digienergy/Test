from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, DateTime, Double, Float,BigInteger
from sqlalchemy.sql import func
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
from datetime import datetime, timedelta

DATABASE_URL = "postgresql://postgres:Apollore100@35.221.226.168:5432/solar_data"

engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class SolarPreprocessData(Base):
    __tablename__ = 'test_preprocess_main_data'
    __table_args__ = {"schema": "data_collection"}  

    id = Column(String, primary_key=True)  # Assuming 'id' is the primary key
    dataloggerSN = Column(String)
    time = Column(DateTime)
    year = Column(Integer)
    month = Column(Integer)
    day = Column(Integer)
    hour = Column(Integer)
    min = Column(Integer)
    sec = Column(Integer)
    電網頻率 = Column(Float)  # '電網頻率' as double precision
    電網A相電壓 = Column(Float)
    電網B相電壓 = Column(Float)
    電網C相電壓 = Column(Float)
    MPPT1 = Column(Float)
    MPPT2 = Column(Float)
    MPPT3 = Column(Float)
    MPPT4 = Column(Float)
    累積發電量 = Column(Float)
    當日發電量 = Column(Float)
    PV1電流 = Column(Float)
    PV2電流 = Column(Float)
    PV3電流 = Column(Float)
    PV4電流 = Column(Float)
    PV5電流 = Column(Float)
    PV6電流 = Column(Float)
    PV7電流 = Column(Float)
    PV8電流 = Column(Float)
    PV10電流 = Column(Float)
    PV11電流 = Column(Float)
    PV18電流 = Column(Float)
    PV19電流 = Column(Float)
    PV20電流 = Column(Float)
    PV17電流 = Column(Float)
    狀態1 = Column(Float)
    告警1 = Column(Float)
    無功功率 = Column(Float)
    有功功率 = Column(Float)
    內部溫度 = Column(Float)
    csq = Column(BigInteger)
    #direction = Column(String)
    #type = Column(String)
    #ver = Column(String)
    #ver_date = Column(BigInteger)
    #zip = Column(String)
    brand = Column(String)
    device_type = Column(String)
    modbus_addr = Column(BigInteger)
    SN = Column(String)
    電網A相電流 = Column(Float)
    電網B相電流 = Column(Float)
    電網C相電流 = Column(Float)

    def __repr__(self):
        return f"<TestPreprocessMainData(id={self.id}, deviceid={self.deviceid}, time={self.time})>"

class EnergySummary(Base):
    __tablename__ = 'energy_summary'  # Name of the table
    __table_args__ = {"schema": "small_young"}  
    # Define columns
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    dataloggerSN = Column(String, nullable=True)
    modbus_addr = Column(BigInteger, nullable=True)
    daily_generation = Column(Double, nullable=True)
    total_generation = Column(Double, nullable=True)
    equivalent_trees = Column(Double, nullable=True)
    standard_coal_saved = Column(Double, nullable=True)
    co2_reduction = Column(Double, nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=True)

    def __repr__(self):
        return f"<EnergySummary(id={self.id})>"

class Equipment(Base):
    __tablename__ = 'equipments'  # Name of the table in the database
    __table_args__ = {"schema": "small_young"}  
    # Define columns
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    dataloggerSN = Column(String)
    內部溫度 = Column(Float)
    #direction = Column(String, nullable=True)
    #type = Column(String, nullable=True)
    #ver = Column(String, nullable=True)
    #ver_date = Column(Integer, nullable=True)
    #zip = Column(String, nullable=True)
    brand = Column(String, nullable=True)
    device_type = Column(String, nullable=True)
    modbus_addr = Column(BigInteger, nullable=True)
    狀態1  = Column(Double, nullable=True)
    告警1  = Column(Double, nullable=True)
    SN = Column(Double, nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=True)
    

    def __repr__(self):
        return f"<Equipment(solar_area_id={self.dataloggerSN}>"


class Area(Base):
    __tablename__ = 'area'  
    __table_args__ = {"schema": "small_young"}  
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    solar_area_id = Column(String(255), nullable=True)
    address = Column(String(255), nullable=True)
    district = Column(String(255), nullable=True)
    longitude = Column(String(255), nullable=True)  # Store as float for precision
    latitude = Column(String(255), nullable=True)  # Store as float for precision
    person_in_charge = Column(String(255), nullable=True)
    company = Column(String(255), nullable=True) 
    dataloggerSN = Column(String(255), nullable=True) 
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=True)  # Automatically sets the current time

    def __repr__(self):
        return f"<Area(solar_area_id={self.solar_area_id}>"

class EnergyHour(Base):
    __tablename__ = 'energy_hour'
    __table_args__ = {"schema": "small_young"} 
    # 定義表格欄位
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)  # 自動遞增主鍵
    dataloggerSN = Column(String(255), nullable=True) 
    hour_generation = Column(Double, nullable=True)  # hour發電量
    modbus_addr = Column(BigInteger, nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow)  # 自動填充時間戳記，使用當前時間



class EnergyDay(Base):
    __tablename__ = 'energy_day'
    __table_args__ = {"schema": "small_young"} 
    # 定義表格欄位
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)  # 自動遞增主鍵
    dataloggerSN = Column(String(255), nullable=True)  
    day_generation = Column(Double, nullable=True)  # 總發電量
    modbus_addr = Column(BigInteger, nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow)  # 自動填充時間戳記，使用當前時間