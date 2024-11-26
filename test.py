import schedule
import logging
import time
import os
from dotenv import load_dotenv
import random
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func
from sqlalchemy.sql import distinct
import models
from logging.handlers import TimedRotatingFileHandler

# 設置日誌文件的目錄
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# 配置 TimedRotatingFileHandler，設置為每天旋轉一次
log_filename = os.path.join(log_dir, "app.log")  # 日誌文件的名稱，會根據日期自動生成
handler = TimedRotatingFileHandler(log_filename, when="midnight", interval=1, backupCount=7)
handler.suffix = "%Y-%m-%d.log"  # 日誌文件的後綴為日期，例如 "app-2024-11-15.log"
handler.setLevel(logging.INFO)  # 設置日誌的級別

# 設置日誌格式，包含日期和日誌級別等
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)

# 創建 logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)


# 資料庫連接設置
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)


def get_energy_summary():
    session = Session()
    dataloggerSNs = ['10132230202714']
    clean_data=[]
    try:
        for dataloggerSN  in dataloggerSNs :
            query = (session.query(
                models.SolarPreprocessData.time,
                models.SolarPreprocessData.累積發電量,
                models.SolarPreprocessData.當日發電量,
                models.SolarPreprocessData.dataloggerSN,
                models.SolarPreprocessData.modbus_addr
            ).filter(models.SolarPreprocessData.dataloggerSN  == dataloggerSN )
            .distinct(models.SolarPreprocessData.dataloggerSN)
            .order_by(models.SolarPreprocessData.dataloggerSN .desc(),models.SolarPreprocessData.time.desc()) 
            )    
            results = query.all()
            for r in results:
                for i in r:
                    clean_data.append(i)

            standard_coal, co2_reduction, equivalent_trees = calculate_environmental_benefits(results[0][1])
            clean_data.append(standard_coal)
            clean_data.append(co2_reduction)
            clean_data.append(equivalent_trees)

        return clean_data
      
    finally:
        session.close()
def calculate_environmental_benefits(total_generation):
    """
    計算環保效益
    Calculate environmental benefits
    
    Args:
        total_generation: 總發電量(kWh)
    Returns:
        tuple: (標準煤節約量, CO₂減排量, 等效植樹量)
    """
    # 標準煤節約量 (噸)
    standard_coal = total_generation * 0.0003215
    
    # CO₂減排量 (噸)
    co2_reduction = total_generation * 0.509 / 1000
    
    # 等效植樹量 (棵)
    # 一棵樹每年吸收 18.3 kg CO₂
    equivalent_trees = co2_reduction * 1000 / 18.3
    
    return standard_coal, co2_reduction, equivalent_trees

def insert_energy_summary(data):

    session = Session()

    try:
        if data :
            new_record = models.EnergySummary(
                dataloggerSN = data[3],
                daily_generation = data[2],
                total_generation = data[1],  
                modbus_addr = data[4],
                standard_coal_saved = data[5],
                co2_reduction = data[6],
                equivalent_trees = data[7],
                timestamp = data[0]
            )
            session.add(new_record)
            session.commit()
        else:
            new_record = models.EnergySummary(
                timestamp = datetime.now()
            )
            session.add(new_record)
            session.commit()
        dataloggerSNs = ["00000000000002", "00000000000001", "00000000000000", "11111111111111", 
                "22222222222222", "33333333333333", "44444444444444", "55555555555555", 
                "66666666666666", "77777777777777", "99999999999999", 
                "88888888888888"]  
        fake_total_generation = random.randint(35266,40000)

        standard_coal, co2_reduction, equivalent_trees = calculate_environmental_benefits(fake_total_generation)
        for dataloggerSN in dataloggerSNs :
            new_record = models.EnergySummary(
                dataloggerSN = dataloggerSN,
                daily_generation = random.randint(50, 100),
                total_generation = fake_total_generation,  
                modbus_addr = 1,
                standard_coal_saved = standard_coal,
                co2_reduction = co2_reduction,
                equivalent_trees = equivalent_trees,
                timestamp = datetime.now()
            )

            session.add(new_record)
            session.commit()

        print(f"Inserting record into EnergySummary")
    except Exception as e:
        session.rollback()
        print(f"Error inserting record into EnergySummary: {e}")
    finally:
        session.close()

def get_equipment():
    session = Session()
    dataloggerSNs = ['10132230202714']
    try:
        for dataloggerSN  in dataloggerSNs :

            query = (
                session.query(
                    models.SolarPreprocessData.dataloggerSN ,
                    models.SolarPreprocessData.內部溫度,
                    #models.SolarPreprocessData.direction,
                    #models.SolarPreprocessData.type ,
                    #models.SolarPreprocessData.ver,
                    #models.SolarPreprocessData.ver_date,
                    #models.SolarPreprocessData.zip,
                    models.SolarPreprocessData.brand,
                    models.SolarPreprocessData.device_type,
                    models.SolarPreprocessData.modbus_addr,
                    models.SolarPreprocessData.SN,
                    models.SolarPreprocessData.狀態1,
                    models.SolarPreprocessData.告警1,
                    models.SolarPreprocessData.time,
                )
                .filter(models.SolarPreprocessData.dataloggerSN  == dataloggerSN )
                .distinct(models.SolarPreprocessData.dataloggerSN )
                .order_by(models.SolarPreprocessData.dataloggerSN , models.SolarPreprocessData.time.desc())
            )

            results = query.all()

        return results
    finally:
        session.close()

def insert_equipment(data):
    session = Session()  
    try:
        if data :
            for item in data:   
                new_record = models.Equipment(
                        dataloggerSN  = item[0],
                        內部溫度 = item[1],
                        #direction = item[2],
                        #type  = item[3],
                        #ver = item[4],
                        #ver_date = item[5],
                        #zip = item[6],
                        brand = item[2],
                        device_type = item[3],
                        modbus_addr = item[4],
                        SN = item[5],
                        狀態1 = item[6],
                        告警1 = item[7],
                        timestamp = item[8]
                )
                
               
                session.add(new_record)
                session.commit()
        else:
            new_record = models.Equipment(
                timestamp = datetime.now()
            )
            session.add(new_record)
            session.commit()
        dataloggerSNs = ["00000000000002", "00000000000001", "00000000000000", "11111111111111", 
                    "22222222222222", "33333333333333", "44444444444444", "55555555555555", 
                    "66666666666666", "77777777777777", "99999999999999", 
                    "88888888888888"]

        for dataloggerSN in dataloggerSNs :
            new_record = models.Equipment(
                    dataloggerSN  = dataloggerSN,
                    內部溫度 = random.randint(20, 30),
                    brand = "GDW_MT",
                    device_type = "INVERTER",
                    modbus_addr = 1,
                    SN = '6050KMTN22AR9999',
                    狀態1 = 1,
                    告警1 = 0,
                    timestamp = datetime.now()
                    )

            session.add(new_record)
            session.commit()
        print(f"Inserting into the equipment table")

    except Exception as e:

        session.rollback()
        print(f"Error inserting record into Equipment: {e}")
    
    finally:
        # Always close the session after operation
        session.close()

def get_energy_hour():
    session = Session()
    dataloggerSNs = ['10132230202714']
    now = datetime.now()
    one_hour_ago = now - timedelta(hours=1)
    clean_data=[]
    for dataloggerSN  in dataloggerSNs :
        query = (session.query(
            models.SolarPreprocessData.time,
            models.SolarPreprocessData.當日發電量,
            models.SolarPreprocessData.dataloggerSN,
            models.SolarPreprocessData.modbus_addr
        ).filter(models.SolarPreprocessData.dataloggerSN  == dataloggerSN )
        .filter(models.SolarPreprocessData.time >= one_hour_ago)
        .order_by(models.SolarPreprocessData.time.desc()) 
        )    
        results = query.all()  
        if results :
            clean_data.append(dataloggerSN)  
            clean_data.append(results[0][1]-results[-1][1])  
            clean_data.append(results[0][3])

    return clean_data
      
def insert_energy_hour(data):
    session = Session()
    try:
        if data :
            new_record = models.EnergyHour(
                dataloggerSN = data[0],
                hour_generation = data[1],
                modbus_addr = data[2],
                timestamp = datetime.now()
            )
        else:
            new_record = models.EnergyHour(
                dataloggerSN = 'null',
                hour_generation = 0,
                modbus_addr = 'null',
                timestamp = datetime.now()
            )
        session.add(new_record)
        session.commit()

        dataloggerSNs = ["00000000000002", "00000000000001", "00000000000000", "11111111111111", 
                "22222222222222", "33333333333333", "44444444444444", "55555555555555", 
                "66666666666666", "77777777777777", "99999999999999", 
                "88888888888888"]
        for dataloggerSN in dataloggerSNs:
            new_record = models.EnergyHour(
                dataloggerSN = dataloggerSN,
                hour_generation = random.uniform(0, 20),
                modbus_addr = 1,
                timestamp = datetime.now()
                )
            session.add(new_record)
            session.commit()
        print(f"inserting record into EnergyHour")
    except Exception as e:
        session.rollback()
        print(f"Error inserting record into EnergyHour: {e}")
    finally:
        session.close()

def get_energy_day():
    session = Session()
    dataloggerSNs = ['10132230202714']
    now = datetime.now()
    one_hour_ago = now - timedelta(hours=1)
    clean_data=[]
    for dataloggerSN  in dataloggerSNs :
        query = (session.query(
            models.SolarPreprocessData.time,
            models.SolarPreprocessData.當日發電量,
            models.SolarPreprocessData.dataloggerSN,
            models.SolarPreprocessData.modbus_addr
        ).filter(models.SolarPreprocessData.dataloggerSN  == dataloggerSN )
        .filter(models.SolarPreprocessData.time >= one_hour_ago)
        .order_by(models.SolarPreprocessData.time.desc()) 
        )    
        results = query.all()  
       
        if results :
            clean_data.append(dataloggerSN)  
            clean_data.append(results[0][1])  
            clean_data.append(results[0][3])
    print("get energy_day sucessful")
    return clean_data
      
def insert_energy_day(data):
    session = Session()
    try:

        if data :   
            new_record = models.EnergyDay(
                dataloggerSN = data[0],
                day_generation = data[1],
                modbus_addr = data[2],
                timestamp = datetime.now()
            )
        else:
            new_record = models.EnergyDay(
                timestamp = datetime.now()
            )
        session.add(new_record)
        session.commit()
 
        dataloggerSNs = ["00000000000002", "00000000000001", "00000000000000", "11111111111111", 
                "22222222222222", "33333333333333", "44444444444444", "55555555555555", 
                "66666666666666", "77777777777777", "99999999999999", 
                "88888888888888"]
        for dataloggerSN in dataloggerSNs:
            new_record = models.EnergyDay(
                dataloggerSN = dataloggerSN,
                day_generation = random.randint(100,200),
                modbus_addr = 1,
                timestamp = datetime.now()
            )
            session.add(new_record)
            session.commit()
        print(f"Inserting record into EnergyDay")

    except Exception as e:
        session.rollback()
        print(f"Error inserting record into EnergyDay: {e}")
    finally:
        session.close()


def multithread_query_and_insert():
    with ThreadPoolExecutor(max_workers=6) as executor:
     
        executor.submit(scheduled_equipment)
        executor.submit(scheduled_energy_summary)
        executor.submit(scheduled_energy_hour)
        executor.submit(scheduled_energy_day)

# 定時任務函數
def scheduled_equipment():

    print("scheduled_equipment started.")
    data = get_equipment()
    insert_equipment(data)
    print("scheduled_equipment end")

def scheduled_energy_summary():

    print("scheduled_energy_summary started.")
    data = get_energy_summary()
    insert_energy_summary(data)
    print("scheduled_energy_summary end")

def scheduled_energy_hour():

    print("scheduled_energy_hour started.")
    data = get_energy_hour()
    insert_energy_hour(data)
    print("scheduled_energy_hour end")

def scheduled_energy_day():

    print("scheduled_energy_day started.")
    data = get_energy_day()
    insert_energy_day(data)
    print("scheduled_energy_day end")

# 設置排程
schedule.every(60).seconds.do(scheduled_equipment)  # 60 秒執行 
schedule.every(60).seconds.do(scheduled_energy_summary)  # 60 秒執行
# schedule.every(1).seconds.do(scheduled_energy_hour)
# schedule.every(1).seconds.do(scheduled_energy_day)
schedule.every().hour.at(":59").do(scheduled_energy_hour)
schedule.every().day.at("23:59").do(scheduled_energy_day)

# 主程式：持續執行排程
if __name__ == "__main__":
    logging.info("Scheduler started.")
    while True:
        schedule.run_pending()
        time.sleep(1)
