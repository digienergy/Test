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
os.makedirs(log_dir, exist_ok=True)

# 配置 TimedRotatingFileHandler，設置為每天旋轉一次
log_filename = os.path.join(log_dir, "script")
handler = TimedRotatingFileHandler(
    log_filename, when="midnight", interval=1, backupCount=7
)
handler.suffix = "%Y-%m-%d.log"  # 日誌文件的後綴為日期
handler.terminator = "\n"  # 禁用緩衝，立即寫入
handler.setLevel(logging.DEBUG)  # 設置日誌的級別
formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'
)
handler.setFormatter(formatter)

# 創建 logger 並設置日誌格式與級別
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

# 添加終端輸出
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# 資料庫連接設置
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
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
                models.SolarPreprocessData.電網A相電壓,
                models.SolarPreprocessData.電網B相電壓,
                models.SolarPreprocessData.電網C相電壓,
                models.SolarPreprocessData.電網A相電流,
                models.SolarPreprocessData.電網B相電流,
                models.SolarPreprocessData.電網C相電流,
                models.SolarPreprocessData.MPPT1輸入電壓,
                models.SolarPreprocessData.MPPT2輸入電壓,
                models.SolarPreprocessData.MPPT3輸入電壓,
                models.SolarPreprocessData.MPPT1輸入電流,
                models.SolarPreprocessData.MPPT2輸入電流,
                models.SolarPreprocessData.MPPT3輸入電流
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

    voltage_a = data[9]
    voltage_b = data[9]
    voltage_c = data[9]
    current_a = data[9]
    current_b = data[9]
    current_c = data[9]
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
                    func.max(models.SolarPreprocessData.timestamp).label("latest_time")
                )
                .filter(models.SolarPreprocessData.dataloggerSN == '10132230202639')
                .group_by(models.SolarPreprocessData.modbus_addr)
                .subquery()
            )

            results = (
                session.query(
                    models.SolarPreprocessData.timestamp,
                    func.coalesce(models.SolarPreprocessData.累積發電量,0).label('累積發電量'),
                    func.coalesce(models.SolarPreprocessData.當日發電量,0).label('當日發電量'),
                    models.SolarPreprocessData.dataloggerSN,
                    models.SolarPreprocessData.modbus_addr,
                    func.coalesce(models.SolarPreprocessData.輸入功率,0).label('輸入功率'),
                    func.coalesce(models.SolarPreprocessData.MPPT1輸入電壓,0).label('MPPT1輸入電壓'),
                    func.coalesce(models.SolarPreprocessData.MPPT2輸入電壓,0).label('MPPT2輸入電壓'),
                    func.coalesce(models.SolarPreprocessData.MPPT3輸入電壓,0).label('MPPT3輸入電壓'),
                    func.coalesce(models.SolarPreprocessData.MPPT1輸入電流,0).label('MPPT1輸入電流'),
                    func.coalesce(models.SolarPreprocessData.MPPT2輸入電流,0).label('MPPT2輸入電流'),
                    func.coalesce(models.SolarPreprocessData.MPPT3輸入電流,0).label('MPPT3輸入電流'),
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
                    & (models.SolarPreprocessData.timestamp == latest_time_subquery.c.latest_time)
                )
                .distinct()  
                .all()
            )

            for i in results :
                i_list = list(i)
                data = calculate_ac_reactivate_power(i_list)
                if data[2] :
                    standard_coal, co2_reduction, equivalent_trees = calculate_environmental_benefits(data[2])
                else:
                    data[2]=0
                    standard_coal, co2_reduction, equivalent_trees = calculate_environmental_benefits(data[2])

                data.append(standard_coal)
                data.append(co2_reduction)
                data.append(equivalent_trees)
                clean_data.append(data)    
        logging.info("successfully got record from preprocess_main_data")
   
        return clean_data

    except Exception as e:
        logging.debug(f"Error getting record from preprocess_main_data: {e}")

def insert_miaoli_energy_summary(datas):
    try:
        if datas:
                   
            with Session() as session:
                #print(len(datas))
                for data in datas:  
                #     print(data)
                    new_record = models.EnergySummary(
                        total_generation=round(data[1], 2) if data[1] is not None else 0.0,
                        daily_generation=round(data[2], 2) if data[2] is not None else 0.0,
                        dataloggerSN=data[3],
                        modbus_addr=data[4],
                        ac_reactive_power=round(data[5], 2) if data[5] is not None else 0.0,
                        mppt1_v=round(data[6], 2) if data[6] is not None else 0.0,
                        mppt2_v=round(data[7], 2) if data[7] is not None else 0.0,
                        mppt3_v=round(data[8], 2) if data[8] is not None else 0.0,
                        mppt1_c=round(data[9], 2) if data[9] is not None else 0.0,
                        mppt2_c=round(data[10], 2) if data[10] is not None else 0.0,
                        mppt3_c=round(data[11], 2) if data[11] is not None else 0.0,
                        grid_a_v=round(data[12], 2) if data[12] is not None else 0.0,
                        grid_b_v=round(data[13], 2) if data[13] is not None else 0.0,
                        grid_c_v=round(data[14], 2) if data[14] is not None else 0.0,
                        grid_a_c=round(data[15], 2) if data[15] is not None else 0.0,
                        grid_b_c=round(data[16], 2) if data[16] is not None else 0.0,
                        grid_c_c=round(data[17], 2) if data[17] is not None else 0.0,
                        standard_coal_saved=data[9],
                        co2_reduction=data[10],
                        equivalent_trees=data[11],
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
                    total_generation=round(data[1], 2) if data[1] is not None else 0.0,
                    daily_generation=round(data[2], 2) if data[2] is not None else 0.0,
                    dataloggerSN=data[3],
                    modbus_addr=data[4],
                    ac_reactive_power=round(data[5], 2) if data[5] is not None else 0.0,
                    grid_a_v=round(data[6], 2) if data[6] is not None else 0.0,
                    grid_b_v=round(data[7], 2) if data[7] is not None else 0.0,
                    grid_c_v=round(data[8], 2) if data[8] is not None else 0.0,
                    grid_a_c=round(data[9], 2) if data[9] is not None else 0.0,
                    grid_b_c=round(data[10], 2) if data[10] is not None else 0.0,
                    grid_c_c=round(data[11], 2) if data[11] is not None else 0.0,
                    mppt1_v=round(data[12], 2) if data[12] is not None else 0.0,
                    mppt2_v=round(data[13], 2) if data[13] is not None else 0.0,
                    mppt3_v=round(data[14], 2) if data[14] is not None else 0.0,
                    mppt1_c=round(data[15], 2) if data[15] is not None else 0.0,
                    mppt2_c=round(data[16], 2) if data[16] is not None else 0.0,
                    mppt3_c=round(data[17], 2) if data[17] is not None else 0.0,
                    standard_coal_saved=data[18] ,
                    co2_reduction=data[19],
                    equivalent_trees=data[20],
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
        logging.debug(f"Error getting {dataloggerSNs} equipment data: {e}")

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
                    models.SolarPreprocessData.timestamp,
                )
                .filter(models.SolarPreprocessData.dataloggerSN == '10132230202639')
                .distinct(models.SolarPreprocessData.modbus_addr)
                .order_by(models.SolarPreprocessData.modbus_addr, models.SolarPreprocessData.timestamp.desc())
            ).all()
            
            for i in query:
                i_list = list(i)
                data = miaoli_state_and_alarm(i_list)
                results.append(data)

        return results
    except Exception as e:
        logging.debug(f"Error getting 10132230202639 equipment data: {e}")

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
            17: {"message": "Vac Fail", "description": "1. 安規設置錯誤\n2. 電電壓不穩定\n3. 交流線線徑過小或交流線較長導致阻值過高，壓降過高\n4. 交流線接線有誤，導致交流端電壓異常", "level": "Minor"},
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
                    "temperature": data[1] if data[1] is not None else 0.0,
                    "brand": data[2],
                    "device_type": data[3],
                    "modbus_addr": data[4],
                    "SN": data[5],
                    "state1": int(float(data[6])),
                    "alarm1": int(float(data[7])),
                }
                if record_dict["state1"] == 2:
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
                    new_record = models.Equipment(
                        dataloggerSN = data['dataloggerSN']  ,
                        temperature = data['temperature'] if data['temperature'] is not None else 0 ,
                        brand = data['brand'],
                        device_type = data['device_type'] ,
                        modbus_addr = data['modbus_addr'],
                        state1 = data['state1'] if data['state1'] is not None else 1,
                        alarm1 = data['state1'] if data['state1'] is not None else 0,
                        SN = data['SN'],
                    )    

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
                    hour_generation=round(data[1], 2) if data[1] is not None else 0.0,
                    modbus_addr=data[2],
                )
            else:
                new_record = models.EnergyHour(
                    dataloggerSN='10132230202714',
                    hour_generation=0,
                    modbus_addr=1,
                )

            session.add(new_record)
            session.commit()

            dataloggerSNs = ["00000000000002", "00000000000001", "00000000000000", "11111111111111", 
                             "33333333333333", "44444444444444", "55555555555555", 
                             "66666666666666", "77777777777777", "99999999999999", "88888888888888","777"]

            for dataloggerSN in dataloggerSNs:
                new_record = models.EnergyHour(
                    dataloggerSN=dataloggerSN,
                    hour_generation=random.randint(0, 20),
                    modbus_addr=1,
                )
                session.add(new_record)

            session.commit()
            logging.info("Successfully inserted records into EnergyHour.")

    except Exception as e:
        # 捕捉錯誤並記錄錯誤訊息
        logging.debug(f"Error inserting record into EnergyHour: {e}")

def get_miaoli_energy_hour():
    try:
        # 使用 with 語法來管理 session
        with Session() as session:
            now = datetime.now()
            one_hour_ago = now - timedelta(hours=1)
            clean_data = []
            for modbus_addr in range(1,22):
                query = (
                    session.query(
                        models.SolarPreprocessData.time,
                        models.SolarPreprocessData.當日發電量,
                        models.SolarPreprocessData.dataloggerSN,
                        models.SolarPreprocessData.modbus_addr,
                        models.SolarPreprocessData.SN
                    )
                    .filter(models.SolarPreprocessData.time >= one_hour_ago)
                    .filter(models.SolarPreprocessData.dataloggerSN == '10132230202639')
                    .filter(models.SolarPreprocessData.modbus_addr == modbus_addr)
                    .order_by(models.SolarPreprocessData.time.desc())
                )
                results = query.all()

                if results:
                    data=[]
                    data.append(results[0][2])
                    # 計算���時發電量差異
                    start_value = 0
                    end_value = 0
                    for i in range(0,len(results)):
                        if results[i][1] :
                            start_value = results[i][1]
                            break
                    for i in range(len(results)-1,-1,-1):
                        if results[i][1] :
                            end_value = results[i][1]
                            break

                    if  start_value - end_value >= 0:
                        data.append(start_value - end_value)
                    else:
                        data.append(0)
                    data.append(results[0][3])
                    data.append(results[0][4])

                clean_data.append(data)

        logging.info("Successfully get energy data.")
        return clean_data

    except Exception as e:
        # 捕捉錯誤並記錄錯誤訊息
        logging.debug(f"Error occurred while retrieving energy data: {e}")

def insert_miaoli_energy_hour(datas):
    try:
        with Session() as session:
            if datas:
                for data in datas :
    
                    new_record = models.EnergyHour(
                        dataloggerSN=data[0],
                        hour_generation=round(data[1], 2) if data[1] is not None else 0.0,
                        modbus_addr=data[2],
                        SN = data[3],
                    )
                    session.add(new_record)
                    session.commit()
            else:
                for modbus_addr in range(1,22):
                    new_record = models.EnergyHour(
                        dataloggerSN='10132230202639',
                        hour_generation=0,
                        modbus_addr = modbus_addr,
                        SN = '',
                    )
                    session.add(new_record)
                    session.commit()
            logging.info("Successfully inserted records into EnergyHour.")

    except Exception as e:
        # 捕捉錯誤並記錄錯誤訊息
        logging.debug(f"Error inserting record into EnergyHour: {e}")

def get_miaoli_energy_day():
    clean_data = []
    now = datetime.now()
    today_start = datetime(now.year, now.month, now.day)  # 获取当天开始时间

    try:
        # 使用 with 语法来管理 session
        with Session() as session:
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
                    models.SolarPreprocessData.dataloggerSN,
                    models.SolarPreprocessData.當日發電量,
                    models.SolarPreprocessData.modbus_addr,
                    models.SolarPreprocessData.SN,
                )
                .join(
                    latest_time_subquery,
                    (models.SolarPreprocessData.modbus_addr == latest_time_subquery.c.modbus_addr)
                    & (models.SolarPreprocessData.time == latest_time_subquery.c.latest_time)
                )
                .distinct()  
                .all()
            )

            for result in results:
                data=[]
                data.append('10132230202639')
                data.append(result[2])
                data.append(result[3])
                data.append(result[4])
                clean_data.append(data)

            logging.info("get energy_day successful")
            return clean_data

    except Exception as e:
        logging.debug(f"Error getting energy_day data: {e}")

def insert_miaoli_energy_day(datas):
    try:
        with Session() as session:
            if datas:
                
                for data in datas:
                    
                    new_record = models.EnergyDay(
                        dataloggerSN=data[0],
                        day_generation=round(data[1], 2) if data[1] is not None else 0.0,
                        modbus_addr=data[2],
                        SN=data[3],
                    )
                    session.add(new_record)
                    session.commit()
            else:
                for modbus_addr in range(1,22) :
                    new_record = models.EnergyDay(
                        dataloggerSN='10132230202639',
                        day_generation=0,
                        modbus_addr=modbus_addr,
                    )
                    session.add(new_record)
                    session.commit()
            logging.info("Inserting record into EnergyDay")

    except Exception as e:
        logging.debug(f"Error inserting record into EnergyDay: {e}")


def get_energy_day():
    clean_data = []
    now = datetime.now()
    today_start = datetime(now.year, now.month, now.day)  # 获取当天开始时间

    try:
        # 使用 with 语法来管理 session
        with Session() as session:
            latest_time_subquery = (
                session.query(
                    models.SolarPreprocessData.modbus_addr,
                    func.max(models.SolarPreprocessData.time).label("latest_time")
                )
                .filter(models.SolarPreprocessData.dataloggerSN == '10132230202714',
                        models.SolarPreprocessData.time>= today_start)
                .group_by(models.SolarPreprocessData.modbus_addr)
                .subquery()
            )

            results = (
                session.query(
                    models.SolarPreprocessData.time,
                    models.SolarPreprocessData.dataloggerSN,
                    models.SolarPreprocessData.當日發電量,
                    models.SolarPreprocessData.modbus_addr,
                    models.SolarPreprocessData.SN,
                )
                .join(
                    latest_time_subquery,
                    (models.SolarPreprocessData.modbus_addr == latest_time_subquery.c.modbus_addr)
                    & (models.SolarPreprocessData.time == latest_time_subquery.c.latest_time)
                )
                .distinct()  
                .all()
            )

            for result in results:
                data=[]
                data.append(result[1])
                data.append(result[2])
                data.append(result[3])
                data.append(result[4])

            logging.info("get energy_day successful")
            return data

    except Exception as e:
        logging.debug(f"Error getting energy_day data: {e}")

def insert_energy_day(data):
    try:
        with Session() as session:
            if data:
                new_record = models.EnergyDay(
                    dataloggerSN=data[0],
                    day_generation=round(data[1], 2) if data[1] is not None else 0.0,
                    modbus_addr=data[2],
                    SN=data[3],

                )
            else:
                new_record = models.EnergyDay(
                    dataloggerSN='10132230202714',
                    day_generation=0,
                    modbus_addr=1,
                    SN='6050KMTN22AR0010',
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
                    SN='6050KMTN22AR0010'
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
        

def insert_energy_monthly(data_list):
    try:
        with Session() as session:
            # 插入主记录
            if data_list:
                for data in data_list :
                    new_record = models.EnergyMonth(
                        dataloggerSN=data['dataloggerSN'],
                        month_generation=round(data['cumulative_energy'], 2) ,
                        modbus_addr=data['modbus_addr']
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
                     "11111111111111", "33333333333333","10132230202639",
                     "44444444444444", "55555555555555", "66666666666666",
                     "77777777777777", "99999999999999", "88888888888888", "10132230202714","777"]
    
    url = "https://opendata.cwa.gov.tw/api/v1/rest/datastore/O-A0001-001?Authorization=CWA-B16BBF2C-E747-4E39-BF07-286710733FAE"
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()  # 將回應轉為 JSON 格式
            weather = data["records"]["Station"][181]["WeatherElement"]["Weather"]
            weather = weather_exchange(weather)
            start_of_day = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)  # 今天 00:00:00
            end_of_day = start_of_day + timedelta(days=1) - timedelta(seconds=1)  # 今天 23:59:59

            with Session() as session:
                for dataloggerSN in dataloggerSNs:
                    if dataloggerSN == "10132230202639" :
                        records = session.query(models.EnergyHour) \
                                    .filter(models.EnergyHour.dataloggerSN == "10132230202639") \
                                    .filter(models.EnergyHour.timestamp >= start_of_day)\
                                    .filter(models.EnergyHour.timestamp <= end_of_day)\
                                    .order_by(models.EnergyHour.modbus_addr,desc(models.EnergyHour.timestamp))\
                                    .distinct(models.EnergyHour.modbus_addr)\
                                    .all()
                            
                        for record in records:
                            # Each record is the full EnergyHour object
                            record.weather = weather
                            session.commit() 
                    else:
                        record = session.query(models.EnergyHour)\
                                        .filter(models.EnergyHour.dataloggerSN == dataloggerSN) \
                                        .filter(models.EnergyHour.timestamp >= start_of_day)\
                                        .filter(models.EnergyHour.timestamp <= end_of_day)\
                                        .order_by(desc(models.EnergyHour.timestamp)).first()
                        if record:
                            record.weather = weather
                            session.commit()
            time.sleep(10)
            logging.info(f"Successful update weather into EnergyHour")
       
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
                     "11111111111111", "33333333333333","10132230202639",
                     "44444444444444", "55555555555555", "66666666666666",
                     "77777777777777", "99999999999999", "88888888888888", "10132230202714","777"]

    global weather_data   
    
    try:
        with Session() as session:
            for dataloggerSN in dataloggerSNs:
                if dataloggerSN == "10132230202639" :
                        latest_records_subquery = (
                            session.query(
                                models.EnergyDay.modbus_addr,
                                func.max(models.EnergyDay.timestamp).label("latest_timestamp"),
                            )
                            .filter(models.EnergyDay.dataloggerSN == "10132230202639")
                            .group_by(models.EnergyDay.modbus_addr)
                            .subquery()
                        )

                        # 主查詢：通過最新 timestamp 獲取完整記錄
                        records = (
                            session.query(models.EnergyDay)
                            .join(
                                latest_records_subquery,
                                (models.EnergyDay.modbus_addr == latest_records_subquery.c.modbus_addr) &
                                (models.EnergyDay.timestamp == latest_records_subquery.c.latest_timestamp),
                            )
                            .filter(models.EnergyDay.dataloggerSN == "10132230202639")  # 再次確保 dataloggerSN 條件
                            .order_by(models.EnergyDay.modbus_addr)  # 可根據需求排序
                            .all()
                        )

                        for record in records :

                            record.weather = weather_data   
                        session.commit() 
                else:
                    record = session.query(models.EnergyDay). \
                                    filter(models.EnergyDay.dataloggerSN == dataloggerSN). \
                                    order_by(desc(models.EnergyDay.timestamp)).first()
                
                    if record:
                        record.weather = weather_data
                        session.commit()

        logging.info("Successful update weather_data into EnergyDay")

    except Exception as e:
        logging.debug(f"Error updating weather_data into EnergyDay: {e}")


def process_data(datas):
    with Session() as session:
        for data in datas:
            try:

                new_record = models.EnergyHour(
                    dataloggerSN=data[0],
                    hour_generation=data[1],  # 使用已計算好的小時發電量
                    modbus_addr=data[2],
                    SN=data[3],
                    timestamp=data[4]+timedelta(minutes=59)
                )
                session.add(new_record)
                session.commit()
                logging.info(f"Successfully inserted hour generation for {data[0]} at {data[4]}")
            
            except Exception as e:
                logging.error(f"Error processing data: {e}")
                session.rollback()
                continue

def handle_missing_data(missing_hours):
    clean_data = []
    with Session() as session:
        for missing_hour in missing_hours:
            try:
                dataloggerSN = missing_hour[0]
                modbus_addr = missing_hour[1]
                timestamp = missing_hour[2]
                start_time = timestamp
                end_time = timestamp + timedelta(hours=1)
                
                results = session.query(models.SolarPreprocessData.dataloggerSN,
                                        models.SolarPreprocessData.modbus_addr,
                                        models.SolarPreprocessData.SN,
                                        models.SolarPreprocessData.當日發電量,
                                        models.SolarPreprocessData.time)\
                                .filter(
                                    models.SolarPreprocessData.dataloggerSN == dataloggerSN,
                                    models.SolarPreprocessData.modbus_addr == modbus_addr,
                                    models.SolarPreprocessData.time >= start_time,
                                    models.SolarPreprocessData.time < end_time )\
                                .order_by(desc(models.SolarPreprocessData.time))\
                                .all()
                if results:
                    data = []
                    data.append(dataloggerSN)
                    # 計算小時發電量差異
                    start_value = 0
                    end_value = 0

                    for i in range(0, len(results)):
                        if results[i][3]:
                            start_value = round(results[i][3], 3)
                            break
                    for i in range(len(results)-1, -1, -1):  # 修正這裡的範圍
                        if results[i][3]:
                            end_value = round(results[i][3], 3)
                            break
                    if start_value - end_value >= 0:
                        data.append(start_value - end_value)
                    else:
                        data.append(0)
                    data.append(modbus_addr)
                    data.append(results[0][2])
                    data.append(timestamp)
                    clean_data.append(data)

            except Exception as e:
                print(f"Error querying SolarPreprocessData: {e}")
                
        # 將 process_data 移到迴圈外
        if clean_data:
            process_data(clean_data)

def check_hour_generation():
    # 使用 with 管理資料庫會話
    with Session() as session:
        try:
            start_date = datetime.today().date()
            end_date = datetime.today().date()
            #start_date = datetime(2024, 12, 17)
            #end_date = datetime(2024, 12, 17)
            missing_hours = []

            current_date = start_date
            while current_date <= end_date:
                # print(current_date)# 每小時 06:00 到 06:59 (6:00 AM to 6:59 AM)
                for hour in range(6, 18):  # Only the hour between 06:00 and 06:59
                    # 生成每小時的開始時間和結束時間
                    start_time = datetime.combine(current_date, datetime.min.time()) + timedelta(hours=hour, minutes=0)
                    end_time = start_time + timedelta(minutes=60)  # ends at 06:59

                    # 查詢該時間範圍內是否有數值，忽略秒數
                    for modbus in range(1,2):
                        exists = session.query(models.EnergyHour).filter(
                            models.EnergyHour.timestamp >= start_time.replace(second=0, microsecond=0),
                            models.EnergyHour.timestamp < end_time.replace(second=0, microsecond=0),
                            models.EnergyHour.hour_generation.isnot(None),
                            models.EnergyHour.dataloggerSN == '10132230202714',
                            models.EnergyHour.modbus_addr == modbus,
                        ).first()

                        if not exists:
                            missing_hours.append(['10132230202714',modbus,start_time])

                current_date += timedelta(days=1)
            logging.info("End in the check:",missing_hours)
            if missing_hours:
                handle_missing_data(missing_hours)
            else:
                logging.info("All 6:00 AM to 6:59 AM minutes have valid data for 2024-12-16.")

        except Exception as e:
            logging.debug(f"Error: {e}")

def process_day_data(datas):
    with Session() as session:
        for data in datas:
            try:
                #print(data)
                new_record = models.EnergyDay(
                    dataloggerSN=data[0],
                    modbus_addr=data[1],  # 使用已計算好的小時發電量
                    SN=data[2],
                    day_generation=data[3],
                    timestamp=data[4].replace(hour=21, minute=0, second=0, microsecond=0)
                )

                session.add(new_record)
                session.commit()
                logging.info(f"Successfully inserted day generation for {data[0]} at {data[4]}")
            
            except Exception as e:
                logging.error(f"Error processing data: {e}")
                session.rollback()
                continue

def handle_missing_day_data(missing_days):
    clean_data = []
    with Session() as session:
        for missing_day in missing_days:
            try:
                dataloggerSN = missing_day[0]
                modbus_addr = missing_day[1]
                timestamp = missing_day[2]
                start_time = timestamp
                end_time = timestamp + timedelta(days=1)
                
                results = (
                    session.query(
                        models.SolarPreprocessData.dataloggerSN,
                        models.SolarPreprocessData.modbus_addr,
                        models.SolarPreprocessData.SN,
                        models.SolarPreprocessData.當日發電量,
                        models.SolarPreprocessData.time,
                    )
                    .filter(
                        models.SolarPreprocessData.dataloggerSN == dataloggerSN,
                        models.SolarPreprocessData.modbus_addr == modbus_addr,
                        models.SolarPreprocessData.time >= start_time,
                        models.SolarPreprocessData.time < end_time,
                        models.SolarPreprocessData.當日發電量 != 0,  # 當日發電量不為 0
                    )
                    .order_by(desc(models.SolarPreprocessData.time))  # 按時間倒序排列
                    .limit(1)  # 只取最後一筆資料
                    .all()
                )
                if results:
                    for result in results :
                        data = []
                        data.append(dataloggerSN)
                        data.append(modbus_addr)
                        data.append(result[2])
                        data.append(result[3])
                        data.append(result[4])
                        clean_data.append(data)

            except Exception as e:
                print(f"Error querying SolarPreprocessData: {e}")
           
        # 將 process_data 移到迴圈外
        #print(clean_data)
        if clean_data:
             process_day_data(clean_data)

def check_day_generation():
    # 使用 with 管理資料庫會話
    with Session() as session:
        try:
            start_date = datetime.today().date()
            end_date = datetime.today().date()
            #start_date = datetime(2024, 12, 1)
            #end_date = datetime(2024, 12, 18)
            missing_days = []

            current_date = start_date
            while current_date <= end_date:
                start_time = datetime.combine(current_date, datetime.min.time()) + timedelta(hours=21)
                end_time = start_time + timedelta(hours=1)  

                # 查詢該時間範圍內是否有數值，忽略秒數
                for modbus in range(1,2):
                    
                    exists = session.query(models.EnergyDay).filter(
                            models.EnergyDay.timestamp >= start_time.replace(second=0, microsecond=0),
                            models.EnergyDay.timestamp < end_time.replace(second=0, microsecond=0),
                            models.EnergyDay.day_generation.isnot(None),
                            models.EnergyDay.dataloggerSN == '10132230202714',
                            models.EnergyDay.modbus_addr == modbus,
                        ).first()

                    if not exists:
                        missing_days.append(['10132230202714',modbus,start_time])

                current_date += timedelta(days=1)
            
            if missing_days:
                #print(missing_days)
                #logging.info(len(missing_days))
                handle_missing_day_data(missing_days)
            else:
                logging.info("All days have valid data ")

        except Exception as e:
            logging.debug(f" check_day_generation Error: {e}")

def is_within_restricted_hours():
    #Check if the current time is within restricted hours (18:00 - 6:00).
    hour = datetime.now().hour
    if hour <= 5 or hour >= 18:
        return True
    return False

# 定時任務函數
def scheduled_equipment():
    if is_within_restricted_hours():
        print("Skipping scheduled_equipment due to restricted hours.")
        return
    logging.info("scheduled_equipment started.")
    data = get_equipment()
    insert_equipment(data)
    logging.info("scheduled_equipment end")

def scheduled_energy_summary():
    if is_within_restricted_hours():
        print("Skipping scheduled_equipment due to restricted hours.")
        return
    logging.info("scheduled_energy_summary started.")
    data = get_energy_summary()
    insert_energy_summary(data)
    logging.info("scheduled_energy_summary end")

def scheduled_miaoli_energy_summary():
    if is_within_restricted_hours():
        print("Skipping scheduled_equipment due to restricted hours.")
        return    
    logging.info("scheduled_miaoli_energy_summary started.")
    data = get_miaoli_energy_summary()
    insert_miaoli_energy_summary(data)
    logging.info("scheduled_miaoli_energy_summary end")

def scheduled_miaoli_energy_hour():
    if is_within_restricted_hours():
        print("Skipping scheduled_miaoli_energy_hour due to restricted hours.")
        return    
    logging.info("scheduled_miaoli_energy_hour started.")
    data = get_miaoli_energy_hour()
    insert_miaoli_energy_hour(data)
    logging.info("scheduled_miaoli_energy_hour end")

def scheduled_miaoli_energy_day():

    logging.info("scheduled_miaoli_energy_day started.")
    data = get_miaoli_energy_day()
    insert_miaoli_energy_day(data)
    logging.info("scheduled_miaoli_energy_day end")

def scheduled_miaoli_equipment():
    if is_within_restricted_hours():
        print("Skipping scheduled_equipment due to restricted hours.")
        return
    logging.info("scheduled_miaoli_equipments started.")
    data = get_miaoli_equipment()
    insert_miaoli_equipment(data)
    logging.info("scheduled_miaoli_equipments end")

def scheduled_energy_hour():
    if is_within_restricted_hours():
        print("Skipping scheduled_equipment due to restricted hours.")
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
schedule.every().hour.at(":59").do(scheduled_miaoli_energy_hour)   
schedule.every().hour.at(":59").do(scheduled_energy_hour)
schedule.every().day.at("20:00").do(check_hour_generation)
schedule.every().day.at("21:10").do(check_day_generation)
#schedule.every(1).seconds.do(scheduled_energy_hour)
#schedule.every(1).seconds.do(scheduled_energy_day)
schedule.every().day.at("21:00").do(scheduled_miaoli_energy_day)
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
