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
from sqlalchemy.sql import distinct,func
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
    clean_data = []
    try:
        # 使用 with 語法管理 Session
        with Session() as session:
            results = (session.query(
                models.SolarPreprocessData.time,
                models.SolarPreprocessData.累積發電量,
                models.SolarPreprocessData.當日發電量,
                models.SolarPreprocessData.dataloggerSN,
                models.SolarPreprocessData.modbus_addr,
                models.SolarPreprocessData.有功功率,
                models.SolarPreprocessData.MPPT1輸入電壓,
                models.SolarPreprocessData.MPPT2輸入電壓,
                models.SolarPreprocessData.MPPT3輸入電壓,
                models.SolarPreprocessData.MPPT4輸入電壓
            ).filter(models.SolarPreprocessData.dataloggerSN == '10132230202714')
            .order_by(models.SolarPreprocessData.time.desc())
            .first())  # 取得最新一筆資料

            result = list(results)

            standard_coal, co2_reduction, equivalent_trees = calculate_environmental_benefits(results[1])
            result.append(standard_coal)
            result.append(co2_reduction)
            result.append(equivalent_trees)

        logging.info("successfully got record from preprocess_main_data")
        return result

    except Exception as e:
        logging.debug(f"Error getting record from preprocess_main_data: {e}")

def calculate_ac_reactivate_power(data):

    voltage_a = data.pop(9)
    voltage_b = data.pop(9)
    voltage_c = data.pop(9)
    current_a = data.pop(9)
    current_b = data.pop(9)
    current_c = data.pop(9)
    ac_reactive_power = round(voltage_a*current_a+voltage_b*current_b+voltage_c*current_c,2)/1000
    data[5] = ac_reactive_power

    return data
def get_miaoli_energy_summary():
    clean_data = []
    try:
        # 使用 with 語法管理 Session
        with Session() as session:
            # Subquery to get the latest time for each modbus_addr
            latest_time_subquery = (
                session.query(
                    models.SolarPreprocessData.modbus_addr,
                    func.max(models.SolarPreprocessData.time).label("latest_time")
                )
                .filter(models.SolarPreprocessData.dataloggerSN == '10132230202639')
                .group_by(models.SolarPreprocessData.modbus_addr)
                .subquery()
            )

            results = (
                session.query(
                    models.SolarPreprocessData.time,
                    func.coalesce(models.SolarPreprocessData.累積發電量,0).label('累積發電量'),
                    func.coalesce(models.SolarPreprocessData.當日發電量,0).label('當日發電量'),
                    models.SolarPreprocessData.dataloggerSN,
                    models.SolarPreprocessData.modbus_addr,
                    func.coalesce(models.SolarPreprocessData.輸入功率,0).label('輸入功率'),
                    func.coalesce(models.SolarPreprocessData.MPPT1輸入電壓,0).label('MPPT1輸入電壓'),
                    func.coalesce(models.SolarPreprocessData.MPPT2輸入電壓,0).label('MPPT2輸入電壓'),
                    func.coalesce(models.SolarPreprocessData.MPPT3輸入電壓,0).label('MPPT3輸入電壓'),
                    func.coalesce(models.SolarPreprocessData.電網A相電壓,0).label('電網A相電壓'),
                    func.coalesce(models.SolarPreprocessData.電網B相電壓,0).label('電網B相電壓'),
                    func.coalesce(models.SolarPreprocessData.電網C相電壓,0).label('電網C相電壓'),
                    func.coalesce(models.SolarPreprocessData.電網A相電流,0).label('電網A相電流'),
                    func.coalesce(models.SolarPreprocessData.電網B相電流,0).label('電網B相電流'),
                    func.coalesce(models.SolarPreprocessData.電網C相電流,0).label('電網C相電流'),
                )
                .join(
                    latest_time_subquery,
                    (models.SolarPreprocessData.modbus_addr == latest_time_subquery.c.modbus_addr)
                    & (models.SolarPreprocessData.time == latest_time_subquery.c.latest_time)
                )
                .distinct()  
                .all()
            )

            for i in results :

                i_list = list(i)
                data = calculate_ac_reactivate_power(i_list)

                if i_list[2] :
                    standard_coal, co2_reduction, equivalent_trees = calculate_environmental_benefits(i[2])
                else:
                    i_list[2]=0
                    standard_coal, co2_reduction, equivalent_trees = calculate_environmental_benefits(i[2])
                
                i_list.append(standard_coal)
                i_list.append(co2_reduction)
                i_list.append(equivalent_trees)
                
        logging.info("successfully got record from preprocess_main_data")

        return results

    except Exception as e:
        logging.debug(f"Error getting record from preprocess_main_data: {e}")

def insert_miaoli_energy_summary(datas):
    try:
        if datas:
                   
            with Session() as session:
                for data in datas:
                    new_record = models.EnergySummary(
                        total_generation=round(data[1], 2),
                        daily_generation=round(data[2], 2),
                        dataloggerSN=data[3],
                        modbus_addr=data[4],
                        ac_reactive_power=round(data[5], 2),
                        mppt1=round(data[6], 2),
                        mppt2=round(data[7], 2),
                        mppt3=round(data[8], 2),
                        standard_coal_saved=data[9],
                        co2_reduction=data[10],
                        equivalent_trees=data[11],
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

        logging.info("Inserting Miaoli record into EnergySummary")
    except Exception as e:
        logging.debug(f"Error inserting Miaoli record into EnergySummary: {e}")

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
    
    return round(standard_coal,2), round(co2_reduction,2), round(equivalent_trees,2)

def insert_energy_summary(data):
    try:
        if data:
            # 使用 with 語法管理 Session
            with Session() as session:
                
                new_record = models.EnergySummary(
                    total_generation=round(data[1], 2),
                    daily_generation=round(data[2], 2),
                    dataloggerSN=data[3],
                    modbus_addr=data[4],
                    ac_reactive_power=round(data[5], 2),
                    mppt1=round(data[6], 2),
                    mppt2=round(data[7], 2),
                    mppt3=round(data[8], 2),
                    mppt4=round(data[9], 2),
                    standard_coal_saved=data[10],
                    co2_reduction=data[11],
                    equivalent_trees=data[12],
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
                         "33333333333333", "44444444444444", "55555555555555",
                         "66666666666666", "77777777777777", "99999999999999",
                         "88888888888888","777"]
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
                    standard_coal_saved=standard_coal,
                    co2_reduction=co2_reduction,
                    equivalent_trees=equivalent_trees,
                    timestamp=datetime.now()
                )

                session.add(new_record)
                session.commit()

        logging.info(f"Inserting record into EnergySummary")
    except Exception as e:
        logging.debug(f"Error inserting record into EnergySummary: {e}")

def get_equipment():
    dataloggerSNs = ['10132230202714']
    results = []
    try:
        # 使用 with 語法來管理 Session
        with Session() as session:
            
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

                results.extend(query.first())  # 將結果加入到 results 清單中
        return results
    except Exception as e:
        logging.debug(f"Error getting equipment data: {e}")

def miaoli_state_and_alarm(data):
    #reference &*1 or &*8 Inverter fault code Bit on spec.
    result={}
    result['dataloggerSN'] = data[0]
    result['temperature'] = data[1]
    result['brand'] = data[2]
    result['device_type'] = data[3]
    result['modbus_addr'] = data[4]
    result['SN'] = data[5]
    result['state1'] = data[6]
    result['alarm1'] = data[7]
    result['timestamp'] = data[8]

    fault_code_map = {
            1: {"description": "Fan warning", "level": "Warning"},
            4: {"description": "StrPIDconfig Warning", "level": "Minor"},
            8: {"description": "StrReverse or StrShort fault", "level": "Critical"},
            16: {"description": "Model Init fault", "level": "Critical"},
            32: {"description": "Grid Volt Sample different", "level": "Major"},
            64: {"description": "ISO Sample different", "level": "Major"},
            128: {"description": "GFCI Sample different", "level": "Major"},
            256: {"description": "PV1 or PV2 circuit short", "level": "Critical"},
            512: {"description": "PV1 or PV2 boost driver broken", "level": "Major"},
            4096: {"description": "AFCI Fault", "level": "Critical"},
            16384: {"description": "AFCI Module fault", "level": "Major"},
        }
    
    if result['alarm1'] in fault_code_map:
        fault_data = fault_code_map[result['alarm1']]
        result['alarm_start_time'] = datetime.now()
        result["state_message"] = fault_data["description"]
        result["description"] = fault_data["description"]
        result["level"] = fault_data["level"]

    return result

def get_miaoli_equipment():
    
    results = []
    try:
        # 使用 with 語法來管理 Session
        with Session() as session:
            
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
                .filter(models.SolarPreprocessData.dataloggerSN == '10132230202639')
                .distinct(models.SolarPreprocessData.modbus_addr)
                .order_by(models.SolarPreprocessData.modbus_addr, models.SolarPreprocessData.time.desc())
            ).all()
            
            for i in query:
                i_list = list(i)
                data = miaoli_state_and_alarm(i_list)
                results.append(data)

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
            31: {"message": "SCI Fail", "description": "1. 受外部因素（例如磁場影響等）引起的暫時性現象\n2. 控制板故障", "level": "Critical"},
            30: {"message": "Flash R/W Fail", "description": "1. 受外部因素（例如磁場影響等）引起的暫時性現象\n2. 機器內部元件損壞", "level": "Major"},
            29: {"message": "Fac Fail", "description": "1. 安規設置錯誤\n2. 電網頻率不穩定", "level": "Major"},
            28: {"message": "AFCI Fault", "description": "1. PV組串接觸不良\n2. PV組串對地絕緣異常", "level": "Minor"},
            27: {"message": "TBD", "description": "待定", "level": "Warning"},
            26: {"message": "TBD", "description": "待定", "level": "Warning"},
            25: {"message": "Relay Chk Fail", "description": "1. 繼電器異常\n2. 控制電路異常\n3. 交流測接線異常（可能存在虛接或短路現象）", "level": "Major"},
            24: {"message": "TBD", "description": "待定", "level": "Warning"},
            23: {"message": "ARCFail-HW", "description": "防逆流功能異常（澳洲安規）", "level": "Minor"},
            22: {"message": "TBD", "description": "待定", "level": "Warning"},
            19: {"message": "DCI High", "description": "機器檢測到內部直流輸入分量超出正常範圍", "level": "Critical"},
            18: {"message": "Isolation Fail", "description": "1. 光伏面板接地線未連接或連接有誤\n2. 直流線破損\n3. 交流端零、地線接線有誤\n4. 在早晚或陰雨天氣，空氣濕度較高時容易引發ISO報錯", "level": "Major"},
            17: {"message": "Vac Fail", "description": "1. 安規設置錯誤\n2. 市電電壓不穩定\n3. 交流線線徑過小或交流線較長導致阻值過高，壓降過高\n4. 交流線接線有誤，導致交流端電壓異常", "level": "Minor"},
            15: {"message": "PV Over Voltage", "description": "PV組串電壓（開路電壓）超出逆變器最大直流輸入電壓", "level": "Critical"},
            13: {"message": "Overtemp.", "description": "1. 機器長時間在高溫環境下運行\n2. 機器安裝環境不利於散熱（例如封閉空間）", "level": "Major"},
            10: {"message": "Ground I Fail", "description": "1. 交流測零地線接線有誤\n2. 在早晚或陰雨天氣，空氣濕度較高時可能引起報錯", "level": "Minor"},
            9: {"message": "Utility Loss", "description": "1. 電網停電\n2. 機器AC端接線異常\n3. AC開關連接異常或開關損壞\n4. AC端未連接", "level": "Critical"}
        }

        # 取得錯誤代碼資訊
        if n in error_codes:
            record_dict["state_message"] = error_codes[n]["message"]
            record_dict["state_description"] = error_codes[n]["description"]
            record_dict["level"] = error_codes[n]["level"]
        else:
            record_dict["state_message"] = "Unknown Error"
            record_dict["state_description"] = "No description available"
            record_dict["level"] = "Warning"
    return record_dict

def insert_equipment(data):
    try:
        # 使用 with 語法來管理 session
        with Session() as session:
            if data: 
                 
                record_dict = {
                    "dataloggerSN": data[0],
                    "temperature": data[1],
                    "brand": data[2],
                    "device_type": data[3],
                    "modbus_addr": data[4],
                    "SN": data[5],
                    "state1": data[6],
                    "alarm1": data[7],
                    "timestamp": data[8],
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
                             "33333333333333", "44444444444444", "55555555555555", 
                             "66666666666666", "77777777777777", "99999999999999", "88888888888888","777"]

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
        logging.debug(f"Error inserting record into Equipment: {e}")

def insert_miaoli_equipment(datas):
    try:
        with Session() as session:
            if datas: 
                for data in datas:

                    new_record = models.Equipment(**data)

                    session.add(new_record)
                    session.commit()
        logging.info("Successfully inserted records into equipments.")

    except Exception as e :
        logging.debug(f"Error inserted records into equipments:{e}.")

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

            logging.info("Successfully get energy data.")
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
                             "33333333333333", "44444444444444", "55555555555555", 
                             "66666666666666", "77777777777777", "99999999999999", "88888888888888","777"]

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
                "33333333333333", "44444444444444", "55555555555555", 
                "66666666666666", "77777777777777", "99999999999999", "88888888888888","777"
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

def get_energy_monthly():
    clean_data = []
    dataloggerSNs = ['10132230202714']
    now = datetime.now()
    
    # 获取当月开始时间和下月开始时间
    month_start = datetime(now.year, now.month, 1)
    next_month_start = (month_start + timedelta(days=32)).replace(day=1)  # 下个月第一天
    try:
        # 使用 with 语法管理 session
        with Session() as session:
            for dataloggerSN in dataloggerSNs:
                # 查询当月最后一笔记录
                last_record = (
                    session.query(
                        models.SolarPreprocessData.time,
                        models.SolarPreprocessData.累積發電量,
                        models.SolarPreprocessData.dataloggerSN,
                        models.SolarPreprocessData.modbus_addr
                    )
                    .filter(models.SolarPreprocessData.dataloggerSN == dataloggerSN)
                    .filter(models.SolarPreprocessData.time >=month_start)  # 限制查询当月数据
                    .filter(models.SolarPreprocessData.time < next_month_start)
                    .filter(models.SolarPreprocessData.累積發電量 > 0)  # 过滤 累積發電量 > 0
                    .order_by(models.SolarPreprocessData.time.desc())  # 按时间降序
                    .first()  # 获取最后一笔记录
                )

                # 查询当月第一笔记录
                first_record = (
                    session.query(
                        models.SolarPreprocessData.time,
                        models.SolarPreprocessData.累積發電量,
                        models.SolarPreprocessData.dataloggerSN,
                        models.SolarPreprocessData.modbus_addr
                    )
                    .filter(models.SolarPreprocessData.dataloggerSN == dataloggerSN)
                    .filter(models.SolarPreprocessData.time >= month_start)  # 限制查询当月数据
                    .filter(models.SolarPreprocessData.time < next_month_start)
                    .filter(models.SolarPreprocessData.累積發電量 > 0)  # 过滤 累積發電量 > 0
                    .order_by(models.SolarPreprocessData.time.asc())  # 按时间升序
                    .first()  # 获取第一笔记录
                )

                # 如果存在记录，添加到 clean_data
                if first_record and last_record:
                    clean_data.append({
                        "dataloggerSN": dataloggerSN,
                        "time": datetime.now(),
                        "cumulative_energy": round(last_record[1]-first_record[1],2),
                        "modbus_addr": first_record[3],
                        })

            logging.info("get energy_month successful")
            return clean_data

    except Exception as e:
        logging.error(f"Error getting energy_month data: {e}")
        return []

def insert_energy_monthly(data_list):
    try:
        with Session() as session:
            # 插入主记录
            if data_list:
                for data in data_list :
                    new_record = models.EnergyMonth(
                        dataloggerSN=data['dataloggerSN'],
                        month_generation=round(data['cumulative_energy'], 2),
                        modbus_addr=data['modbus_addr'],
                        timestamp=datetime.now()
                    )
                    session.add(new_record)
            else:
                new_record = models.EnergyMonth(
                    timestamp=datetime.now()
                )
                session.add(new_record)
            
            session.commit()

            # 插入多条模拟记录
            dataloggerSNs = [
                "00000000000002", "00000000000001", "00000000000000", "11111111111111", 
                "33333333333333", "44444444444444", "55555555555555", 
                "66666666666666", "77777777777777", "99999999999999", "88888888888888", "777"
            ]

            for dataloggerSN in dataloggerSNs:
                new_record = models.EnergyMonth(
                    dataloggerSN=dataloggerSN,
                    month_generation=round(random.uniform(500, 1000), 2),  # 模拟生成 1000 到 5000 范围的随机数
                    modbus_addr=1,  # 模拟生成 1 到 10 范围的随机 Modbus 地址
                    timestamp=datetime.now()
                )
                session.add(new_record)

            session.commit()  # 一次性提交批量插入的记录
            logging.info("Inserting records into EnergyMonth completed successfully.")

    except Exception as e:
        logging.error(f"Error inserting record into EnergyMonth: {e}")


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
                     "11111111111111", "33333333333333",
                     "44444444444444", "55555555555555", "66666666666666",
                     "77777777777777", "99999999999999", "88888888888888", "10132230202714","777"]
    
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
    global weather_data 
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
                     "11111111111111", "33333333333333",
                     "44444444444444", "55555555555555", "66666666666666",
                     "77777777777777", "99999999999999", "88888888888888", "10132230202714","777"]

    global weather_data   

    try:
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

def scheduled_miaoli_energy_summary():
    logging.info("scheduled_miaoli_energy_summary started.")
    data = get_miaoli_energy_summary()
    insert_miaoli_energy_summary(data)
    logging.info("scheduled_miaoli_energy_summary end")

def scheduled_miaoli_equipment():
    logging.info("scheduled_miaoli_equipments started.")
    data = get_miaoli_equipment()
    insert_miaoli_equipment(data)
    logging.info("scheduled_miaoli_equipments end")

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

def is_last_day_of_month():
    """检查今天是否是本月的最后一天。"""
    today = datetime.now()
    next_day = today + timedelta(days=1)
    return next_day.month != today.month

def scheduled_energy_monthly():

    logging.info("scheduled_energy_monthly started.")
    data = get_energy_monthly()
    insert_energy_monthly(data)
    logging.info("scheduled_energy_monthly end")

def scheduled_get_day_weather():
    get_day_weather()

def scheduled_insert_day_weather():
    insert_day_weather()

# 設置排程
schedule.every(60).seconds.do(scheduled_equipment)  # 60 秒執行 
schedule.every(60).seconds.do(scheduled_energy_summary)  # 60 秒執行
schedule.every(300).seconds.do(scheduled_miaoli_energy_summary)
schedule.every(300).seconds.do(scheduled_miaoli_equipment)    
schedule.every().hour.at(":59").do(scheduled_energy_hour)
schedule.every().day.at("21:00").do(scheduled_energy_day)
schedule.every().day.at("00:10").do(scheduled_get_day_weather)  # 60 秒執行
schedule.every().day.at("21:10").do(scheduled_insert_day_weather)  # 60 秒執行
schedule.every().day.at("21:00").do(
     lambda: scheduled_energy_monthly() if is_last_day_of_month() else None
)
# 主程式：持續執行排程
if __name__ == "__main__":
    logging.info("Scheduler started.")
    while True:
        schedule.run_pending()
        time.sleep(1)
