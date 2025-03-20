import xml.etree.ElementTree as ET

# XML 파일 파싱 및 narrative 내용 확인
def parse_xml_and_show_narratives(xml_file_path):
    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        for safety_report in root.findall('.//safetyreport'):
            for narrative in safety_report.findall('.//narrativeincludeclinical'):
                text = narrative.text.strip() if narrative.text else "❌ No Data Found"
                patient_id = safety_report.find('.//safetyreportid').text.strip()
                print(f"🔎 PatientID {patient_id} ➡️ Narrative Data: {text}")

    except Exception as e:
        print(f"❌ Error parsing {xml_file_path}: {e}")

# 경로 설정 및 실행
file_path = r"C:\Users\최성욱\Desktop\faers_data\faers_xml_2023Q1\XML\1_ADR23Q1.xml"
parse_xml_and_show_narratives(file_path)
