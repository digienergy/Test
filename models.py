from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, DateTime, Double, Float,BigInteger
from sqlalchemy.sql import func
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class SolarPreprocessData(Base):
    __tablename__ = 'preprocess_main_data'
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
    # 電網相關數據
    電網頻率 = Column(Float)  # '電網頻率' as double precision
    電網A相電壓 = Column(Float)
    電網B相電壓 = Column(Float)
    電網C相電壓 = Column(Float)
    電網AB線電壓 = Column(Float)
    電網BC線電壓 = Column(Float)
    電網CV線電壓 = Column(Float)
    電網A相電流 = Column(Float)
    電網B相電流 = Column(Float)
    電網C相電流 = Column(Float)
    # MPPT 輸入電壓與電流
    MPPT1輸入電壓 = Column(Float)
    MPPT2輸入電壓 = Column(Float)
    MPPT3輸入電壓 = Column(Float)
    MPPT4輸入電壓 = Column(Float)
    MPPT1輸入電流 = Column(Float)
    MPPT2輸入電流 = Column(Float)
    MPPT3輸入電流 = Column(Float)
    # PV 電流
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
    PV17電流 = Column(Float)
    PV18電流 = Column(Float)
    PV19電流 = Column(Float)
    PV20電流 = Column(Float)
    # 功率相關數據
    額定功率 = Column(Float)
    輸入功率 = Column(Float)
    有功功率 = Column(Float)
    無功功率 = Column(Float)
    # 發電數據
    當日發電量 = Column(Float)
    累積發電量 = Column(Float)
    # 狀態與告警
    狀態1 = Column(Float)
    告警1 = Column(Float)
    # 其他數據
    內部溫度 = Column(Float)
    絕緣阻抗值 = Column(Float)
    # 基本信息
    csq = Column(BigInteger)
    brand = Column(String)
    device_type = Column(String)
    modbus_addr = Column(BigInteger)
    SN = Column(String)
    # Timestamp
    timestamp = Column(DateTime)

    def __repr__(self):
        return f"<TestPreprocessMainData(id={self.id}, deviceid={self.deviceid}, time={self.time})>"

class EnergySummary(Base):
    __tablename__ = 'energy_summary'  
    __table_args__ = {"schema": "small_young"}  

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    dataloggerSN = Column(String, nullable=True)
    modbus_addr = Column(BigInteger, nullable=True)
    mppt1 = Column(Double, nullable=True)
    mppt2 = Column(Double, nullable=True)
    mppt3 = Column(Double, nullable=True)
    mppt4 = Column(Double, nullable=True)
    ac_reactive_power = Column(Double, nullable=True)
    daily_generation = Column(Double, nullable=True)
    total_generation = Column(Double, nullable=True)
    equivalent_trees = Column(Double, nullable=True)
    standard_coal_saved = Column(Double, nullable=True)
    co2_reduction = Column(Double, nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=True)

    def __repr__(self):
        return f"<EnergySummary(id={self.id})>"

class Equipment(Base):
    __tablename__ = 'equipments' 
    __table_args__ = {"schema": "small_young"}  

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    dataloggerSN = Column(String)
    temperature = Column(Float)
    brand = Column(String, nullable=True)
    device_type = Column(String, nullable=True)
    modbus_addr = Column(Integer, nullable=True)
    state1  = Column(Integer, nullable=True)
    alarm1  = Column(Integer, nullable=True)
    alarm_start_time = Column(DateTime, nullable=True)
    alarm_end_time = Column(DateTime, nullable=True)
    state_message = Column(String, nullable=True)
    state_description = Column(String, nullable=True)
    SN = Column(Double, nullable=True)
    level = Column(String, nullable=True)
    timestamp = Column(DateTime, default=datetime.now, nullable=True)
    

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
    timestamp = Column(DateTime, default=datetime.now, nullable=True)  # Automatically sets the current time

    def __repr__(self):
        return f"<Area(solar_area_id={self.solar_area_id}>"

class EnergyHour(Base):
    __tablename__ = 'energy_hour'
    __table_args__ = {"schema": "small_young"} 
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)  # 自動遞增主鍵
    dataloggerSN = Column(String(255), nullable=True) 
    hour_generation = Column(Double, nullable=True)  # hour發電量
    modbus_addr = Column(Integer, nullable=True)
    weather = Column(String(255), nullable=True) 
    timestamp = Column(DateTime, default = datetime.now)  # 自動填充時間戳記，使用當前時間



class EnergyDay(Base):
    __tablename__ = 'energy_day'
    __table_args__ = {"schema": "small_young"} 
    # 定義表格欄位
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)  # 自動遞增主鍵
    dataloggerSN = Column(String(255), nullable=True)  
    day_generation = Column(Double, nullable=True)  # 總發電量
    modbus_addr = Column(Integer, nullable=True)
    weather = Column(String(255), nullable=True) 
    timestamp = Column(DateTime, default=datetime.now)  # 自動填充時間戳記，使用當前時間

class EnergyMonth(Base):
    __tablename__ = 'energy_month'
    __table_args__ = {"schema": "small_young"} 
    # 定義表格欄位
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)  # 自動遞增主鍵
    dataloggerSN = Column(String(255), nullable=True)  
    month_generation = Column(Double, nullable=True)  # 總發電量
    modbus_addr = Column(Integer, nullable=True)
    timestamp = Column(DateTime, default=datetime.now)  # 自動填充時間戳記，使用當前時間
