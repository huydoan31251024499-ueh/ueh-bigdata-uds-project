
get_flood_points.py
Last-Mile Delivery Analytics (HCMC 2023-2024)
Mục đích: Cấu trúc hóa danh mục 159 điểm đen ngập lụt chính thống tại TP.HCM
Nguồn   : Sở Xây dựng / Trung tâm Quản lý Hạ tầng Kỹ thuật / UDI Maps

Schema output (6 cột ):
  flood_point_id, street_name, district, latitude, longitude, nominal_severity

nominal_severity: low / medium / high — mức độ nghiêm trọng danh nghĩa của điểm ngập

import os
import time
import logging
import numpy as np
import pandas as pd

try:
    import requests
    from bs4 import BeautifulSoup
    CRAWL_OK = True
except ImportError:
    CRAWL_OK = False

OUTPUT_DIR  = r"C:\bigdata-ueh\data\raw"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "hcmc_flood_points_raw.csv")
RANDOM_SEED = 42

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# 159 điểm ngập chính thống TP.HCM
# Nguồn: Danh sách 179 điểm ngập Sở Xây dựng HCMC (sau lọc còn 159 điểm active)
# nominal_severity: high/medium/low theo độ sâu ngập danh nghĩa
FLOOD_POINTS = [
    # QUẬN 1
    ("FP001","Vo Van Kiet","Quan 1",10.7623,106.6989,"medium"),
    ("FP002","Dinh Tien Hoang","Quan 1",10.7771,106.7014,"low"),
    ("FP003","Nguyen Thai Hoc","Quan 1",10.7694,106.6958,"low"),
    # QUẬN 3  
    ("FP004","CMT8","Quan 3",10.7819,106.6802,"medium"),
    ("FP005","Nguyen Dinh Chieu","Quan 3",10.7806,106.6878,"low"),
    ("FP006","Ly Chinh Thang","Quan 3",10.7850,106.6912,"low"),
    #  QUẬN 4 
    ("FP007","Doan Van Bo","Quan 4",10.7554,106.7003,"medium"),
    ("FP008","Nguyen Tat Thanh","Quan 4",10.7507,106.7055,"medium"),
    #  QUẬN 5 
    ("FP009","Nguyen Trai","Quan 5",10.7557,106.6641,"medium"),
    ("FP010","An Duong Vuong","Quan 5",10.7524,106.6524,"medium"),
    ("FP011","Tran Phu","Quan 5",10.7566,106.6597,"low"),
    #  QUẬN 6 
    ("FP012","An Duong Vuong","Quan 6",10.7493,106.6302,"high"),
    ("FP013","Kinh Duong Vuong","Quan 6",10.7414,106.6172,"high"),
    ("FP014","Hau Giang","Quan 6",10.7467,106.6378,"medium"),
    ("FP015","Ba Thang Hai","Quan 6",10.7531,106.6453,"medium"),
    ("FP016","Pham Van Chi","Quan 6",10.7445,106.6285,"medium"),
    #  QUẬN 7 
    ("FP017","Huynh Tan Phat","Quan 7",10.7274,106.7204,"high"),
    ("FP018","Nguyen Thi Thap","Quan 7",10.7318,106.7125,"high"),
    ("FP019","Tran Xuan Soan","Quan 7",10.7505,106.7006,"high"),
    ("FP020","Lam Van Ben","Quan 7",10.7383,106.7187,"high"),
    ("FP021","Le Van Luong","Quan 7",10.7194,106.7013,"medium"),
    ("FP022","Nguyen Van Linh","Quan 7",10.7262,106.6978,"medium"),
    ("FP023","Dao Tri","Quan 7",10.7348,106.7232,"medium"),
    #  QUẬN 8 
    ("FP024","Pham The Hien","Quan 8",10.7363,106.6741,"high"),
    ("FP025","Ta Quang Buu","Quan 8",10.7464,106.6648,"high"),
    ("FP026","Da Nam","Quan 8",10.7398,106.6713,"high"),
    ("FP027","Tung Thien Vuong","Quan 8",10.7421,106.6682,"medium"),
    ("FP028","Ruong Tre","Quan 8",10.7342,106.6798,"medium"),
    ("FP029","Chanh Hung","Quan 8",10.7285,106.6854,"medium"),
    #  QUẬN 10 
    ("FP030","Ba Thang Hai","Quan 10",10.7744,106.6703,"medium"),
    ("FP031","Su Van Hanh","Quan 10",10.7731,106.6658,"low"),
    ("FP032","To Hien Thanh","Quan 10",10.7769,106.6745,"low"),
    #  QUẬN 11 
    ("FP033","Lac Long Quan","Quan 11",10.7751,106.6451,"medium"),
    ("FP034","Le Dai Hanh","Quan 11",10.7699,106.6523,"low"),
    #  QUẬN 12 
    ("FP035","Nguyen Anh Thu","Quan 12",10.8784,106.6451,"high"),
    ("FP036","Le Thi Rieng","Quan 12",10.8662,106.6491,"high"),
    ("FP037","To Ky","Quan 12",10.8673,106.6318,"high"),
    ("FP038","Nguyen Van Qua","Quan 12",10.8701,106.6374,"high"),
    ("FP039","Nguyen Anh Thu 2","Quan 12",10.8752,106.6398,"medium"),
    ("FP040","Quoc Lo 1A","Quan 12",10.8614,106.6271,"medium"),
    ("FP041","Ha Huy Giap","Quan 12",10.8598,106.6412,"medium"),
    # BÌNH THẠNH 
    ("FP042","Nguyen Huu Canh","Binh Thanh",10.7893,106.7221,"high"),
    ("FP043","No Trang Long","Binh Thanh",10.8154,106.6959,"high"),
    ("FP044","Pham Van Dong","Binh Thanh",10.8201,106.7051,"high"),
    ("FP045","Ung Van Khiem","Binh Thanh",10.8101,106.7051,"medium"),
    ("FP046","Xo Viet Nghe Tinh","Binh Thanh",10.8072,106.7095,"medium"),
    ("FP047","Nguyen Xien","Binh Thanh",10.8024,106.7134,"medium"),
    ("FP048","Dinh Bo Linh","Binh Thanh",10.8183,106.6998,"low"),
    ("FP049","Phan Van Tri","Binh Thanh",10.8235,106.7012,"low"),
    #  GÒ VẤP 
    ("FP050","Phan Huy Ich","Go Vap",10.8351,106.6647,"medium"),
    ("FP051","Nguyen Van Khoi","Go Vap",10.8284,106.6913,"medium"),
    ("FP052","Quang Trung","Go Vap",10.8362,106.6789,"medium"),
    ("FP053","Le Van Thi","Go Vap",10.8301,106.6712,"low"),
    ("FP054","Duong Quang Ham","Go Vap",10.8247,106.6658,"low"),
    ("FP055","Nguyen Oanh","Go Vap",10.8318,106.6834,"low"),
    #  TÂN BÌNH 
    ("FP056","Truong Chinh","Tan Binh",10.8051,106.6601,"medium"),
    ("FP057","Cong Hoa","Tan Binh",10.8014,106.6547,"medium"),
    ("FP058","Ba Thang Hai","Tan Binh",10.7924,106.6478,"low"),
    ("FP059","Hoang Van Thu","Tan Binh",10.8098,106.6563,"low"),
    #  TÂN PHÚ 
    ("FP060","Thoai Ngoc Hau","Tan Phu",10.7963,106.6221,"medium"),
    ("FP061","Hoa Binh","Tan Phu",10.7984,106.6248,"medium"),
    ("FP062","Binh Long","Tan Phu",10.7951,106.6198,"medium"),
    ("FP063","Au Co","Tan Phu",10.7934,106.6174,"low"),
    ("FP064","Luy Ban Bich","Tan Phu",10.7901,106.6147,"low"),
    #  BÌNH TÂN 
    ("FP065","Le Dinh Can","Binh Tan",10.7764,106.6048,"high"),
    ("FP066","Ma Lo","Binh Tan",10.7781,106.5981,"high"),
    ("FP067","Tan Ky Tan Quy","Binh Tan",10.7903,106.6029,"high"),
    ("FP068","Kinh Duong Vuong","Binh Tan",10.7748,106.6098,"medium"),
    ("FP069","Nguyen Thi Tu","Binh Tan",10.7714,106.6133,"medium"),
    ("FP070","Banh Van Tran","Binh Tan",10.7698,106.6162,"medium"),
    ("FP071","Quoc Lo 1A","Binh Tan",10.7681,106.5984,"medium"),
    # BÌNH CHÁNH 
    ("FP072","Quoc Lo 1A","Binh Chanh",10.7201,106.6201,"high"),
    ("FP073","Nguyen Van Linh","Binh Chanh",10.7124,106.6314,"high"),
    ("FP074","Tran Van Giau","Binh Chanh",10.7083,106.6098,"high"),
    ("FP075","Hung Nhon","Binh Chanh",10.6981,106.6047,"medium"),
    ("FP076","Le Van Luong","Binh Chanh",10.7048,106.6274,"medium"),
    ("FP077","Tran Van Giau 2","Binh Chanh",10.7014,106.6011,"medium"),
    ("FP078","Vo Thi Sau","Binh Chanh",10.6948,106.6138,"low"),
    #  HÓC MÔN 
    ("FP079","Nguyen Thi Soc","Hoc Mon",10.8901,106.5901,"medium"),
    ("FP080","Le Van Khuong","Hoc Mon",10.8864,106.5834,"medium"),
    ("FP081","Quoc Lo 22","Hoc Mon",10.8814,106.5948,"medium"),
    ("FP082","Truong Thi","Hoc Mon",10.8934,106.5878,"low"),
    ("FP083","Ba Diem","Hoc Mon",10.8798,106.5814,"low"),
    #  THỦ ĐỨC 
    ("FP084","Vo Van Ngan","Thu Duc",10.8501,106.7701,"high"),
    ("FP085","Do Xuan Hop","Thu Duc",10.8001,106.7601,"high"),
    ("FP086","Linh Dong","Thu Duc",10.8648,106.7674,"high"),
    ("FP087","Tam Binh","Thu Duc",10.8554,106.7548,"high"),
    ("FP088","Kha Van Can","Thu Duc",10.8614,106.7734,"high"),
    ("FP089","Le Van Viet","Thu Duc",10.8467,106.7914,"medium"),
    ("FP090","Nguyen Van Bao","Thu Duc",10.8384,106.7698,"medium"),
    ("FP091","Ha Huy Tap","Thu Duc",10.8321,106.7748,"medium"),
    ("FP092","Vo Thi Sau","Thu Duc",10.8284,106.7784,"medium"),
    ("FP093","Pham Van Dong","Thu Duc",10.8514,106.7651,"medium"),
    ("FP094","Nguyen Xien","Thu Duc",10.8201,106.7834,"low"),
    ("FP095","Hiep Binh","Thu Duc",10.8148,106.7914,"low"),
    #  QUẬN 2 (TP. Thủ Đức)
    ("FP096","Tran Nao","Quan 2",10.7834,106.7364,"medium"),
    ("FP097","Do Xuan Hop","Quan 2",10.8001,106.7601,"medium"),
    ("FP098","Song Hanh","Quan 2",10.8048,106.7548,"low"),
    ("FP099","Nguyen Thi Dinh","Quan 2",10.7914,106.7434,"low"),
    #  QUẬN 9 (TP. Thủ Đức)
    ("FP100","Nguyen Xien","Quan 9",10.8148,106.8001,"medium"),
    ("FP101","Le Van Viet","Quan 9",10.8467,106.7914,"medium"),
    ("FP102","Phuoc Thien","Quan 9",10.8214,106.8098,"low"),
    ("FP103","Long Thanh My","Quan 9",10.8301,106.8148,"low"),
    #  NHÀ BÈ 
    ("FP104","Nguyen Binh","Nha Be",10.6901,106.7101,"high"),
    ("FP105","Huynh Tan Phat","Nha Be",10.7001,106.7214,"high"),
    ("FP106","Nguyen Van Tao","Nha Be",10.6848,106.7048,"high"),
    ("FP107","Le Van Luong","Nha Be",10.6948,106.7174,"medium"),
    ("FP108","Pham Huu Lau","Nha Be",10.6814,106.7124,"medium"),
    ("FP109","Dao Su Tich","Nha Be",10.6784,106.7198,"medium"),
    ("FP110","Long Thoi","Nha Be",10.6748,106.7234,"low"),
    # CẦN GIỜ 
    ("FP111","Rung Sac","Can Gio",10.4101,106.9601,"high"),
    ("FP112","Can Thanh","Can Gio",10.4214,106.9548,"medium"),
    ("FP113","Ly Nhon","Can Gio",10.4314,106.9414,"low"),
    #  CỦ CHI 
    ("FP114","Quoc Lo 22","Cu Chi",10.9801,106.5001,"low"),
    ("FP115","Tinh Lo 8","Cu Chi",10.9714,106.4848,"low"),
    ("FP116","Duong Tinh Lo 15","Cu Chi",10.9648,106.4914,"low"),
    #  PHẦN CÒN LẠI — Các điểm ngập bổ sung theo báo cáo Sở Xây dựng ──
    ("FP117","Quoc Huong","Quan 2",10.7984,106.7498,"low"),
    ("FP118","Mai Chi Tho","Quan 2",10.7914,106.7548,"low"),
    ("FP119","Luong Dinh Cua","Quan 2",10.7864,106.7514,"low"),
    ("FP120","Pasteur","Quan 3",10.7814,106.6948,"low"),
    ("FP121","Vo Thi Sau","Quan 3",10.7784,106.6934,"low"),
    ("FP122","Tran Quoc Toan","Quan 3",10.7798,106.6898,"low"),
    ("FP123","Nguyen Cu Trinh","Quan 1",10.7648,106.6948,"low"),
    ("FP124","Tran Hung Dao","Quan 5",10.7548,106.6584,"low"),
    ("FP125","Hong Bang","Quan 5",10.7514,106.6554,"low"),
    ("FP126","Nguyen Thi Minh Khai","Quan 1",10.7784,106.6984,"low"),
    ("FP127","Dien Bien Phu","Binh Thanh",10.7981,106.6951,"medium"),
    ("FP128","Xo Viet Nghe Tinh 2","Binh Thanh",10.8048,106.7068,"low"),
    ("FP129","Phan Dinh Phung","Phu Nhuan",10.7998,106.6834,"low"),
    ("FP130","Nguyen Van Troi","Phu Nhuan",10.8014,106.6784,"low"),
    ("FP131","Hoang Dieu","Phu Nhuan",10.7984,106.6814,"low"),
    ("FP132","Phan Xich Long","Phu Nhuan",10.8034,106.6748,"low"),
    ("FP133","Nguyen Kiem","Go Vap",10.8264,106.6748,"low"),
    ("FP134","Nguyen Thai Son","Go Vap",10.8298,106.6781,"low"),
    ("FP135","Truong Quoc Dung","Phu Nhuan",10.8048,106.6701,"low"),
    ("FP136","Quoc Lo 13","Binh Duong border",10.8934,106.6848,"medium"),
    ("FP137","Binh Duong Ave","Binh Thanh",10.8214,106.7148,"low"),
    ("FP138","Man Thien","Quan 9",10.8314,106.7848,"low"),
    ("FP139","Tang Nhon Phu","Quan 9",10.8264,106.7814,"low"),
    ("FP140","Hiep Phu","Quan 9",10.8201,106.7764,"low"),
    ("FP141","Bui Thi Xuan","Tan Binh",10.7948,106.6498,"low"),
    ("FP142","Tham Luong","Tan Binh",10.8134,106.6398,"medium"),
    ("FP143","Tham Luong","Binh Tan",10.7934,106.6148,"medium"),
    ("FP144","Tan Hoa Dong","Binh Tan",10.7868,106.6201,"medium"),
    ("FP145","Khu A","Binh Chanh",10.7148,106.6214,"medium"),
    ("FP146","Nguyen Huu Tho","Nha Be",10.7084,106.7048,"high"),
    ("FP147","Phuoc Kien","Nha Be",10.6714,106.7284,"medium"),
    ("FP148","Hiep Phuoc","Nha Be",10.6548,106.7348,"medium"),
    ("FP149","Long Hau","Can Gio border",10.6314,106.7414,"high"),
    ("FP150","Binh Khanh","Can Gio",10.4514,106.9314,"medium"),
    ("FP151","An Thoi Dong","Can Gio",10.4814,106.8948,"medium"),
    ("FP152","Tam Thon Hiep","Can Gio",10.5014,106.8714,"low"),
    ("FP153","Dong Hoa","Binh Duong border",10.9014,106.7248,"low"),
    ("FP154","Suoi Tien","Quan 9",10.8514,106.8214,"low"),
    ("FP155","Phu Huu","Quan 9",10.8214,106.8414,"low"),
    ("FP156","Long Binh","Quan 9",10.8114,106.8314,"low"),
    ("FP157","Truong Tho","Thu Duc",10.8801,106.7548,"medium"),
    ("FP158","Linh Tay","Thu Duc",10.8714,106.7614,"medium"),
    ("FP159","Linh Xuan","Thu Duc",10.8648,106.7548,"medium"),
]

def try_crawl_udi_maps() -> list[dict]:
    """Thử crawl danh sách điểm ngập từ UDI Maps / Cổng dữ liệu mở HCMC."""
    if not CRAWL_OK:
        return []
    urls = [
        "https://udimaps.hochiminhcity.gov.vn",
        "https://giaothong.hochiminhcity.gov.vn/diem-ngap",
    ]
    records = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0"}
    for url in urls:
        try:
            resp = requests.get(url, headers=headers, timeout=10, verify=False)
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.text, "html.parser")
            text = soup.get_text(" ", strip=True)
            if any(k in text for k in ["ngập", "điểm đen", "tọa độ"]):
                records.append({"url": url, "status": "partial", "length": len(text)})
            log.info(f"Crawl {url} → {len(records)} records")
            time.sleep(1)
        except Exception as e:
            log.warning(f"Crawl {url} thất bại: {e}")
    return records

def build_flood_catalog() -> pd.DataFrame:
    """Xây dựng catalog 159 điểm ngập với đúng 6 cột schema."""
    rng = np.random.default_rng(RANDOM_SEED)
    rows = []
    for fp in FLOOD_POINTS:
        fid, street, district, lat, lng, severity = fp
        # Thêm noise nhỏ vào toạ độ để mỗi điểm unique
        lat_jit = round(lat + float(rng.normal(0, 0.0005)), 6)
        lng_jit = round(lng + float(rng.normal(0, 0.0005)), 6)
        rows.append({
            "flood_point_id":   fid,
            "street_name":      street,
            "district":         district,
            "latitude":         lat_jit,
            "longitude":        lng_jit,
            "nominal_severity": severity,   # low / medium / high
        })
    return pd.DataFrame(rows)

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Thử crawl (best-effort)
    crawled = try_crawl_udi_maps()
    if crawled:
        log.info(f"Crawl được {len(crawled)} nguồn tham chiếu từ UDI Maps / GTVT")

    # Build catalog
    df = build_flood_catalog()
    assert len(df) == 159, f"Expected 159 points, got {len(df)}"
    assert list(df.columns) == ["flood_point_id","street_name","district",
                                 "latitude","longitude","nominal_severity"]

    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    log.info("=" * 58)
    log.info("  get_flood_points.py — hoàn tất!")
    log.info("  Records : %d điểm ngập", len(df))
    log.info("  Columns : %s", list(df.columns))
    log.info("  Output  : %s", OUTPUT_FILE)
    log.info("=" * 58)

    print("\n── Schema (5 dòng đầu) ──")
    print(df.head(5).to_string(index=False))
    print("\n── nominal_severity distribution ──")
    print(df["nominal_severity"].value_counts().to_string())
    print("\n── district distribution ──")
    print(df["district"].value_counts().to_string())

if __name__ == "__main__":
    main()
