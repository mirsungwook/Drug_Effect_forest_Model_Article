import os
import xml.etree.ElementTree as ET
import mysql.connector
import logging
import time
from datetime import datetime

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 날짜 형식 검사 및 변환 함수
def format_date(date_str):
    if not date_str:
        return None

    # YYYY 형식인 경우 (예: 2012)
    if len(date_str) == 4 and date_str.isdigit():
        return f"{date_str}-01-01"  # YYYY-MM-DD 형식으로 변환 (연도만 있는 경우)

    # YYYYMM 형식인 경우 (예: 202111)
    if len(date_str) == 6 and date_str.isdigit():
        return f"{date_str[:4]}-{date_str[4:6]}-01"  # YYYY-MM-DD 형식으로 변환

    # YYYYMMDD 형식인 경우 (예: 20211115)
    if len(date_str) == 8 and date_str.isdigit():
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"  # YYYY-MM-DD 형식으로 변환

    # 이미 YYYY-MM-DD 형식인 경우
    try:
        datetime.strptime(date_str, "%Y-%m-%d")  # 형식 검사
        return date_str
    except ValueError:
        pass

    # 그 외의 경우 (잘못된 형식)
    return None  # 또는 기본값(예: '0000-00-00')을 반환

# MySQL 연결 설정
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'tjddnr6124',
    'database': 'faers_db',
}

# DB 연결
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

CHUNK_SIZE = 10000  # 더 작은 청크 크기로 설정

# 테이블 생성
def create_table():
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS faers_data (
            receivedate DATE,
            occurcountry VARCHAR(100),
            serious VARCHAR(10),
            seriousnessdeath VARCHAR(10),

            patientonsetage INT,
            patientsex VARCHAR(10),
            patientweight FLOAT,

            reactionmeddrapt VARCHAR(255),
            reactionoutcome VARCHAR(255),

            drugcharacterization VARCHAR(100),
            medicinalproduct VARCHAR(255),
            drugbatchnumb VARCHAR(100),
            drugstartdate DATE,
            drugenddate DATE,
            drugtreatmentduration INT,
            drugtreatmentdurationunit VARCHAR(50),
            drugcumulativedosagenumb FLOAT,
            drugcumulativedosageunit VARCHAR(50),
            drugrecurrence VARCHAR(50),
            drugrecuraction VARCHAR(255),

            drugdosageform VARCHAR(100)
        )
    """)
    conn.commit()
    print("✅ Table 'faers_data' successfully created.")

# 청크 단위 데이터 삽입
def insert_chunk_data(batch_data):
    max_retries = 3  # 최대 재시도 횟수
    retry_delay = 5  # 재시도 간 지연 시간(초)

    for attempt in range(max_retries):
        try:
            for i in range(0, len(batch_data), CHUNK_SIZE):
                chunk = batch_data[i:i + CHUNK_SIZE]
                logging.info(f"Processing chunk: {chunk[:1]}")  # 첫 번째 데이터 샘플 로깅
                cursor.executemany(
                    """
                    INSERT INTO faers_data (
                        receivedate, occurcountry, serious, seriousnessdeath,
                        patientonsetage, patientsex, patientweight,
                        reactionmeddrapt, reactionoutcome,
                        drugcharacterization, medicinalproduct, drugbatchnumb,
                        drugstartdate, drugenddate, drugtreatmentduration,
                        drugtreatmentdurationunit, drugcumulativedosagenumb,
                        drugcumulativedosageunit, drugrecurrence, drugrecuraction,
                        drugdosageform
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s
                    )
                    """, chunk
                )
                conn.commit()
                logging.info(f"✅ {len(chunk)} records successfully inserted.")
            break  # 성공 시 루프 종료
        except mysql.connector.Error as e:
            logging.error(f"❌ Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                logging.info(f"🔄 Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                conn.reconnect()  # 연결 재시도
            else:
                logging.error(f"🔴 All attempts failed. Sample data: {chunk[:5]}")
                logging.error(f"🔴 Sample data count: {len(chunk[0])}")

# XML 데이터 삽입 함수
def insert_data(file_path):
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return

    print(f"[INFO] Processing file: {file_path}")
    tree = ET.parse(file_path)
    root = tree.getroot()

    batch_data = []

    # XML 구조에 맞게 데이터 파싱
    for record in root.findall('.//safetyreport'):
        receivedate = record.findtext('receiptdate', default=None)
        occurcountry = record.findtext('occurcountry', default=None)
        serious = record.findtext('serious', default=None)
        seriousnessdeath = record.findtext('seriousnessdeath', default=None)

        # 환자 정보
        patient_info = record.find('.//patient')
        patientonsetage = patient_info.findtext('patientonsetage', default=None)
        patientsex = patient_info.findtext('patientsex', default=None)
        patientweight = patient_info.findtext('patientweight', default=None)

        # 부작용 반응 정보
        for reaction in record.findall('.//reaction'):
            reactionmeddrapt = reaction.findtext('reactionmeddrapt', default=None)
            reactionoutcome = reaction.findtext('reactionoutcome', default=None)

            # 약물 정보
            for drug in record.findall('.//drug'):
                batch_data.append((
                    receivedate, occurcountry, serious, seriousnessdeath,
                    patientonsetage, patientsex, patientweight,
                    reactionmeddrapt, reactionoutcome,
                    drug.findtext('drugcharacterization', default=None),
                    drug.findtext('medicinalproduct', default=None),
                    drug.findtext('drugbatchnumb', default=None),
                    format_date(drug.findtext('drugstartdate', default=None)),  # 형식 변환
                    format_date(drug.findtext('drugenddate', default=None)),    # 형식 변환
                    drug.findtext('drugtreatmentduration', default=None),
                    drug.findtext('drugtreatmentdurationunit', default=None),
                    drug.findtext('drugcumulativedosagenumb', default=None),
                    drug.findtext('drugcumulativedosageunit', default=None),
                    drug.findtext('drugrecurrence', default=None),
                    drug.findtext('drugrecuraction', default=None),
                    drug.findtext('drugdosageform', default=None)
                ))

    if batch_data:
        insert_chunk_data(batch_data)

if __name__ == "__main__":
    create_table()

    while True:
        file_path = input("Enter the path of the XML file to process (or 'exit' to quit): ")
        if file_path.lower() == 'exit':
            break

        insert_data(file_path)

    cursor.close()
    conn.close()
    print("✅ All XML data successfully loaded into MySQL database!")