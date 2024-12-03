import schedule
import logging
import time
import os
import requests
from dotenv import load_dotenv
import random
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func,desc
from sqlalchemy.sql import distinct
import models
from logging.handlers import TimedRotatingFileHandler

weather_data = None


# 設置日誌文件的目錄
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# 配置 TimedRotatingFileHandler，設置為每天旋轉一次
log_filename = os.path.join(log_dir, "script")  # 日誌文件的名稱，會根據日期自動生成
handler = TimedRotatingFileHandler(log_filename, when="midnight", interval=1, backupCount=7)
# handler = TimedRotatingFileHandler(log_filename, when="S", interval=5, backupCount=3)  # 每 5 秒輪替


handler.suffix = "%Y-%m-%d.log"  # 日誌文件的後綴為日期，例如 "app-2024-11-15.log"
handler.setLevel(logging.INFO)  # 設置日誌的級別
#禁用緩衝,會寫入當下的log
handler.terminator = "\n"
# 設置日誌格式，包含日期和日誌級別等
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)

# 創建 logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)
logging.basicConfig(handlers=[handler], level=logging.INFO)

# 資料庫連接設置
load_dotenv()
DATABASE_URL=os.getenv("DATABASE_URL")
engine = create_engine(
    DATABASE_URL,
    pool_size=10,                # 最大連線數
    max_overflow=20,             # 額外可溢出連線數
    pool_timeout=30,             # 連線等待超時時間 (秒)
    pool_recycle=1800            # 回收連線時間 (秒，30 分鐘)
)


engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)


def get_energy_summary():
    dataloggerSNs = ['10132230202714']
    clean_data = []
    try:
        for dataloggerSN in dataloggerSNs:
            # 使用 with 語法管理 Session
            with Session() as session:
                query = (session.query(
                    models.SolarPreprocessData.time,
                    models.SolarPreprocessData.累積發電量,
                    models.SolarPreprocessData.當日發電量,
                    models.SolarPreprocessData.dataloggerSN,
                    models.SolarPreprocessData.modbus_addr,
                    models.SolarPreprocessData.有功功率,
                    models.SolarPreprocessData.MPPT1,
                    models.SolarPreprocessData.MPPT2,
                    models.SolarPreprocessData.MPPT3,
                    models.SolarPreprocessData.MPPT4
                ).filter(models.SolarPreprocessData.dataloggerSN == dataloggerSN)
                .distinct(models.SolarPreprocessData.dataloggerSN)
                .order_by(models.SolarPreprocessData.dataloggerSN.desc(),
                          models.SolarPreprocessData.time.desc())
                .limit(100))  # 限制只取得前 100 筆資料

                results = query.all()
                for r in results:
                    for i in r:
                        clean_data.append(i)

                standard_coal, co2_reduction, equivalent_trees = calculate_environmental_benefits(results[0][1])
                clean_data.append(standard_coal)
                clean_data.append(co2_reduction)
                clean_data.append(equivalent_trees)
                
        logging.info("successfully got record from preprocess_main_data")
        return clean_data

    except Exception as e:
        logging.debug(f"Error getting record from preprocess_main_data: {e}")

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
    try:
        if data:
            # 使用 with 語法管理 Session
            with Session() as session:
                new_record = models.EnergySummary(
                    dataloggerSN=data[3],
                    daily_generation=round(data[2], 2),
                    total_generation=round(data[1], 2),
                    modbus_addr=data[4],
                    mppt1=round(data[6], 2),
                    mppt2=round(data[7], 2),
                    mppt3=round(data[8], 2),
                    mppt4=round(data[9], 2),
                    ac_reactive_power=round(data[5], 2),
                    standard_coal_saved=round(data[10], 2),
                    co2_reduction=round(data[11], 2),
                    equivalent_trees=round(data[12], 2),
                    timestamp=data[0]
                )
                session.add(new_record)
                session.commit()
        else:
            # 使用 with 語法管理 Session
            with Session() as session:
                new_record = models.EnergySummary(
                    timestamp=datetime.now()
                )
                session.add(new_record)
                session.commit()

        # 以下插入 fake 資料
        dataloggerSNs = ["00000000000002", "00000000000001", "00000000000000", "11111111111111",
                         "22222222222222", "33333333333333", "44444444444444", "55555555555555",
                         "66666666666666", "77777777777777", "99999999999999",
                         "88888888888888"]
        fake_total_generation = random.randint(35266, 40000)

        standard_coal, co2_reduction, equivalent_trees = calculate_environmental_benefits(fake_total_generation)

        # 使用 with 語法管理 Session
        with Session() as session:
            for dataloggerSN in dataloggerSNs:
                new_record = models.EnergySummary(
                    dataloggerSN=dataloggerSN,
                    daily_generation=random.randint(50, 100),
                    total_generation=round(fake_total_generation, 2),
                    modbus_addr=1,
                    mppt1=random.randint(550, 600),
                    mppt2=random.randint(550, 600),
                    mppt3=random.randint(550, 600),
                    mppt4=random.randint(550, 600),
                    ac_reactive_power=random.randint(1, 10),
                    standard_coal_saved=round(standard_coal, 2),
                    co2_reduction=round(co2_reduction, 2),
                    equivalent_trees=round(equivalent_trees, 2),
                    timestamp=datetime.now()
                )

                session.add(new_record)
                session.commit()

        logging.info(f"Inserting record into EnergySummary")
    except Exception as e:
        logging.debug(f"Error inserting record into EnergySummary: {e}")

def get_equipment():
    dataloggerSNs = ['10132230202714']
    try:
        # 使用 with 語法來管理 Session
        with Session() as session:
            results = []
            for dataloggerSN in dataloggerSNs:
                query = (
                    session.query(
                        models.SolarPreprocessData.dataloggerSN,
                        models.SolarPreprocessData.內部溫度,
                        models.SolarPreprocessData.brand,
                        models.SolarPreprocessData.device_type,
                        models.SolarPreprocessData.modbus_addr,
                        models.SolarPreprocessData.SN,
                        models.SolarPreprocessData.狀態1,
                        models.SolarPreprocessData.告警1,
                        models.SolarPreprocessData.time,
                    )
                    .filter(models.SolarPreprocessData.dataloggerSN == dataloggerSN)
                    .distinct(models.SolarPreprocessData.dataloggerSN)
                    .order_by(models.SolarPreprocessData.dataloggerSN, models.SolarPreprocessData.time.desc())
                )

                results.extend(query.all())  # 將結果加入到 results 清單中

        return results
    except Exception as e:
        logging.debug(f"Error getting equipment data: {e}")

def state_and_alarm(record_dict):
    alarm = record_dict["alarm1"]
    state = record_dict["state1"]
    n = random.randint(0, 31)
    if alarm == 1 :
        record_dict["alarm_start_time"] = datetime.now()   

    if state == 2 : #TODO reference Table 8-2
        record_dict["alarm_start_time"] = datetime.now()  
         
        error_codes = {
                31: {"message": "SCI Fail", "description": "1. 受外部因素（例如磁場影響等）引起的暫時性現象\n2. 控制板故障"},
                30: {"message": "Flash R/W Fail", "description": "1. 受外部因素（例如磁場影響等）引起的暫時性現象\n2. 機器內部元件損壞"},
                29: {"message": "Fac Fail", "description": "1. 安規設置錯誤\n2. 電網頻率不穩定"},
                28: {"message": "AFCI Fault", "description": "1. PV組串接觸不良\n2. PV組串對地絕緣異常"},
                27: {"message": "TBD", "description": "待定"},
                26: {"message": "TBD", "description": "待定"},
                25: {"message": "Relay Chk Fail", "description": "1. 繼電器異常\n2. 控制電路異常\n3. 交流測接線異常（可能存在虛接或短路現象）"},
                24: {"message": "TBD", "description": "待定"},
                23: {"message": "ARCFail-HW", "description": "防逆流功能異常（澳洲安規）"},
                22: {"message": "TBD", "description": "待定"},
                21: {"message": "TBD", "description": "待定"},
                20: {"message": "TBD", "description": "待定"},
                19: {"message": "DCI High", "description": "機器檢測到內部直流輸入分量超出正常範圍"},
                18: {"message": "Isolation Fail", "description": "1. 光伏面板接地線未連接或連接有誤\n2. 直流線破損\n3. 交流端零、地線接線有誤\n4. 在早晚或陰雨天氣，空氣濕度較高時容易引發ISO報錯"},
                17: {"message": "Vac Fail", "description": "1. 安規設置錯誤\n2. 市電電壓不穩定\n3. 交流線線徑過小或交流線較長導致阻值過高，壓降過高\n4. 交流線接線有誤，導致交流端電壓異常"},
                16: {"message": "FAN Fail", "description": "1. 外部風扇被異物阻塞\n2. 風扇內部接線異常"},
                15: {"message": "PV Over Voltage", "description": "PV組串電壓（開路電壓）超出逆變器最大直流輸入電壓"},
                14: {"message": "TBD", "description": "待定"},
                13: {"message": "Overtemp.", "description": "1. 機器長時間在高溫環境下運行\n2. 機器安裝環境不利於散熱（例如封閉空間）"},
                12: {"message": "TBD", "description": "待定"},
                11: {"message": "DC Bus High", "description": "1. 光伏組串電壓超過機器最大直流輸入電壓\n2. 控制板故障"},
                10: {"message": "Ground I Fail", "description": "1. 交流測零地線接線有誤\n2. 在早晚或陰雨天氣，空氣濕度較高時可能引起報錯"},
                9: {"message": "Utility Loss", "description": "1. 電網停電\n2. 機器AC端接線異常\n3. AC開關連接異常或開關損壞\n4. AC端未連接"},
                8: {"message": "TBD", "description": "待定"},
                7: {"message": "TBD", "description": "待定"},
                6: {"message": "TBD", "description": "待定"},
                5: {"message": "TBD", "description": "待定"},
                4: {"message": "TBD", "description": "待定"},
                3: {"message": "TBD", "description": "待定"},
                2: {"message": "AC HCT Fail", "description": "1. 受外部因素（例如磁場影響等）引起的暫時性現象\n2. 控制板故障"},
                1: {"message": "GFCI Fail", "description": "1. 受外部因素（例如磁場影響等）引起的暫時性現象\n2. 控制板故障"},
                0: {"message": "TBD", "description": "待定"}
            }
        
        record_dict["state_message"] =  error_codes[n]["message"]
        record_dict["state_description"] = error_codes[n]["description"]

    return record_dict

def insert_equipment(data):
    try:
        # 使用 with 語法來管理 session
        with Session() as session:
            if data:
                for item in data:
                    record_dict = {
                        "dataloggerSN": item[0],
                        "temperature": item[1],
                        "brand": item[2],
                        "device_type": item[3],
                        "modbus_addr": item[4],
                        "SN": item[5],
                        "state1": item[6],
                        "alarm1": item[7],
                        "timestamp": item[8],
                    }
                    if record_dict["state1"] == 2:
                        record_dict = state_and_alarm(record_dict)
                    new_record = models.Equipment(**record_dict)
                    session.add(new_record)
                    session.commit()

            else:
                new_record = models.Equipment(
                    timestamp=datetime.now()
                )
                session.add(new_record)
                session.commit()

            # 假設 dataloggerSNs 是一組假數據
            dataloggerSNs = ["00000000000002", "00000000000001", "00000000000000", "11111111111111", 
                             "22222222222222", "33333333333333", "44444444444444", "55555555555555", 
                             "66666666666666", "77777777777777", "99999999999999", "88888888888888"]

            for dataloggerSN in dataloggerSNs:
                record_dict = {
                    "dataloggerSN": dataloggerSN,
                    "temperature": random.randint(20, 30),
                    "brand": "GDW_MT",
                    "device_type": "INVERTER",
                    "modbus_addr": 1,
                    "SN": '6050KMTN22AR9999',
                    "state1": random.randint(0, 4),
                    "alarm1": 0,
                    "timestamp": datetime.now(),
                }
                if record_dict["state1"] != 1:
                    record_dict["alarm1"] = 1
                    record_dict = state_and_alarm(record_dict)
                new_record = models.Equipment(**record_dict)
                session.add(new_record)
                session.commit()

            logging.info(f"Inserting into the equipment table")

    except Exception as e:
        logging.info(f"Error inserting record into Equipment: {e}")

def get_energy_hour():
    try:
        # 使用 with 語法來管理 session
        with Session() as session:
            dataloggerSNs = ['10132230202714']
            now = datetime.now()
            one_hour_ago = now - timedelta(hours=1)
            clean_data = []

            for dataloggerSN in dataloggerSNs:
                query = (
                    session.query(
                        models.SolarPreprocessData.time,
                        models.SolarPreprocessData.當日發電量,
                        models.SolarPreprocessData.dataloggerSN,
                        models.SolarPreprocessData.modbus_addr
                    )
                    .filter(models.SolarPreprocessData.dataloggerSN == dataloggerSN)
                    .filter(models.SolarPreprocessData.time >= one_hour_ago)
                    .order_by(models.SolarPreprocessData.time.desc())
                )
                results = query.all()

                if results:
                    clean_data.append(dataloggerSN)
                    # 計算當日發電量差異
                    if results[0][1] - results[-1][1] >= 0:
                        clean_data.append(results[0][1] - results[-1][1])
                    else:
                        clean_data.append(0)
                    clean_data.append(results[0][3])

            logging.info("Successfully retrieved energy data.")
            return clean_data

    except Exception as e:
        # 捕捉錯誤並記錄錯誤訊息
        logging.error(f"Error occurred while retrieving energy data: {e}")
        
      
def insert_energy_hour(data):
    try:
        
        with Session() as session:

            if data:
                new_record = models.EnergyHour(
                    dataloggerSN=data[0],
                    hour_generation=round(data[1], 2),
                    modbus_addr=data[2],
                    timestamp=datetime.now()
                )
            else:
                new_record = models.EnergyHour(
                    dataloggerSN='null',
                    hour_generation=0,
                    modbus_addr='null',
                    timestamp=datetime.now()
                )

            session.add(new_record)
            session.commit()

            dataloggerSNs = ["00000000000002", "00000000000001", "00000000000000", "11111111111111", 
                             "22222222222222", "33333333333333", "44444444444444", "55555555555555", 
                             "66666666666666", "77777777777777", "99999999999999", "88888888888888"]

            for dataloggerSN in dataloggerSNs:
                new_record = models.EnergyHour(
                    dataloggerSN=dataloggerSN,
                    hour_generation=random.uniform(0, 20),
                    modbus_addr=1,
                    timestamp=datetime.now()
                )
                session.add(new_record)

            session.commit()
            logging.info("Successfully inserted records into EnergyHour.")

    except Exception as e:
        # 捕捉錯誤並記錄錯誤訊息
        logging.error(f"Error inserting record into EnergyHour: {e}")

def get_energy_day():
    clean_data = []
    dataloggerSNs = ['10132230202714']
    now = datetime.now()
    today_start = datetime(now.year, now.month, now.day)  # 获取当天开始时间

    try:
        # 使用 with 语法来管理 session
        with Session() as session:
            for dataloggerSN in dataloggerSNs:
                query = (
                    session.query(
                        models.SolarPreprocessData.time,
                        models.SolarPreprocessData.當日發電量,
                        models.SolarPreprocessData.dataloggerSN,
                        models.SolarPreprocessData.modbus_addr
                    )
                    .filter(models.SolarPreprocessData.dataloggerSN == dataloggerSN)
                    .filter(models.SolarPreprocessData.time >= today_start)  # 限制查询当天数据
                    .filter(models.SolarPreprocessData.當日發電量 > 0)  # 过滤 當日發電量 不为 0
                    .order_by(models.SolarPreprocessData.time.desc())  # 按时间降序
                )
                
                result = query.first()

                if result:
                    clean_data.append(dataloggerSN)
                    clean_data.append(result[1])
                    clean_data.append(result[3])

            logging.info("get energy_day successful")
            return clean_data

    except Exception as e:
        logging.error(f"Error getting energy_day data: {e}")
      
def insert_energy_day(data):
    try:
        with Session() as session:
            if data:
                new_record = models.EnergyDay(
                    dataloggerSN=data[0],
                    day_generation=round(data[1], 2),
                    modbus_addr=data[2],
                    timestamp=datetime.now()
                )
            else:
                new_record = models.EnergyDay(
                    timestamp=datetime.now()
                )

            session.add(new_record)
            session.commit()

            dataloggerSNs = [
                "00000000000002", "00000000000001", "00000000000000", "11111111111111", 
                "22222222222222", "33333333333333", "44444444444444", "55555555555555", 
                "66666666666666", "77777777777777", "99999999999999", "88888888888888"
            ]

            for dataloggerSN in dataloggerSNs:
                new_record = models.EnergyDay(
                    dataloggerSN=dataloggerSN,
                    day_generation=random.randint(100, 200),
                    modbus_addr=1,
                    timestamp=datetime.now()
                )
                session.add(new_record)
                session.commit()

            logging.info("Inserting record into EnergyDay")

    except Exception as e:
        logging.debug(f"Error inserting record into EnergyDay: {e}")

def weather_exchange(weather):

    if '晴' in weather :
        return 'sunny'
    elif '多雲' in weather :
        return 'cloudy'
    elif '雨' in weather :
        return 'rainy'
    elif '陰' in weather :
        return 'overcast'

def update_hour_energy():
    dataloggerSNs = ["00000000000002", "00000000000001", "00000000000000", 
                     "11111111111111", "22222222222222", "33333333333333",
                     "44444444444444", "55555555555555", "66666666666666",
                     "77777777777777", "99999999999999", "88888888888888", "10132230202714"]
    
    url = "https://opendata.cwa.gov.tw/api/v1/rest/datastore/O-A0001-001?Authorization=CWA-B16BBF2C-E747-4E39-BF07-286710733FAE"
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()  # 將回應轉為 JSON 格式
            weather = data["records"]["Station"][181]["WeatherElement"]["Weather"]
            weather = weather_exchange(weather)

            # 使用 with 语法来管理数据库会话
            with Session() as session:
                for dataloggerSN in dataloggerSNs:
                    record = session.query(models.EnergyHour). \
                                    filter(models.EnergyHour.dataloggerSN == dataloggerSN). \
                                    order_by(desc(models.EnergyHour.timestamp)).first()
                    
                    if record:
                        record.weather = weather
                        session.commit()

            logging.info(f"Successful update weather into EnergyHour")

        else:
            logging.warning(f"Failed to fetch data from API, status code: {response.status_code}")
        
    except Exception as e:
        logging.debug(f"Error updating weather into EnergyHour: {e}")

def get_day_weather():

    url = "https://opendata.cwa.gov.tw/api/v1/rest/datastore/F-C0032-001?Authorization=CWA-B16BBF2C-E747-4E39-BF07-286710733FAE"
    response = requests.get(url)
    try:
        if response.status_code == 200:
            data = response.json()  
            weather = data["records"]["location"][13]['weatherElement'][0]['time'][1]['parameter']['parameterName']
            weather_data = weather_exchange(weather)

    except Exception as e:

        logging.debug("Error get weather from 氣象局: {e}")


def insert_day_weather():
    dataloggerSNs = ["00000000000002", "00000000000001", "00000000000000", 
                     "11111111111111", "22222222222222", "33333333333333",
                     "44444444444444", "55555555555555", "66666666666666",
                     "77777777777777", "99999999999999", "88888888888888", "10132230202714"]

    weather_data = "Sunny"  # 假设这是你要插入的天气数据

    try:
        # 使用 with 语法来管理数据库会话
        with Session() as session:
            for dataloggerSN in dataloggerSNs:
                record = session.query(models.EnergyDay). \
                                filter(models.EnergyDay.dataloggerSN == dataloggerSN). \
                                order_by(desc(models.EnergyDay.timestamp)).first()
                
                if record:
                    record.weather = weather_data
                    session.commit()

        logging.info("Successful update weather_data into EnergyDay")

    except Exception as e:
        logging.debug(f"Error updating weather_data into EnergyDay: {e}")


# 定時任務函數
def scheduled_equipment():

    logging.info("scheduled_equipment started.")
    data = get_equipment()
    insert_equipment(data)
    logging.info("scheduled_equipment end")

def scheduled_energy_summary():

    logging.info("scheduled_energy_summary started.")
    data = get_energy_summary()
    insert_energy_summary(data)
    logging.info("scheduled_energy_summary end")

def scheduled_energy_hour():
    hour = datetime.now().hour
    if hour <= 6 or hour >=18 :
        return
    logging.info("scheduled_energy_hour started.")
    data = get_energy_hour()
    insert_energy_hour(data)
    update_hour_energy()
    logging.info("scheduled_energy_hour end")

def scheduled_energy_day():

    logging.info("scheduled_energy_day started.")
    data = get_energy_day()
    insert_energy_day(data)
    logging.info("scheduled_energy_day end")

def scheduled_get_day_weather():
    get_day_weather()

def scheduled_insert_day_weather():
    insert_day_weather()

# 設置排程
schedule.every(60).seconds.do(scheduled_equipment)  # 60 秒執行 
schedule.every(60).seconds.do(scheduled_energy_summary)  # 60 秒執行
# schedule.every(1).seconds.do(scheduled_energy_hour)
# schedule.every(1).seconds.do(scheduled_energy_day)
schedule.every().hour.at(":59").do(scheduled_energy_hour)
schedule.every().day.at("21:00").do(scheduled_energy_day)
schedule.every().day.at("00:10").do(scheduled_get_day_weather)  # 60 秒執行
schedule.every().day.at("21:10").do(scheduled_insert_day_weather)  # 60 秒執行

# 主程式：持續執行排程
if __name__ == "__main__":
    logging.info("Scheduler started.")
    while True:
        schedule.run_pending()
        time.sleep(1)
