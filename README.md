# Đồ Án Big Data: Phân Tích E-Commerce Olist
# Pipeline: CSV → HDFS → PySpark (ETL + ML) → MongoDB → Flask + Chart.js

## Mô Tả Dự Án

Hệ thống phân tích dữ liệu thương mại điện tử Brazil (Olist) sử dụng pipeline Big Data:
- **Data Lake**: HDFS lưu trữ dữ liệu thô (9 file CSV)
- **Processing**: PySpark ETL + 3 ML models (K-Means, Random Forest, Linear Regression)
- **Data Warehouse**: MongoDB lưu kết quả đã xử lý
- **Visualization**: Flask + Chart.js dashboard (6 trang)

## Cấu Trúc Thư Mục

```
olist-bigdata-project/
├── data/                    # CSV files từ Kaggle
├── hdfs_scripts/            # Scripts HDFS
├── spark_jobs/              # PySpark ETL + ML
├── webapp/                  # Flask dashboard
├── notebooks/               # Jupyter notebooks
├── docs/                    # Báo cáo, sơ đồ
├── requirements.txt
└── README.md
```

## Yêu Cầu Hệ Thống

- Python 3.8+
- Java 11 (JDK)
- Apache Spark 3.5.x
- Apache Hadoop 3.3.x (HDFS)
- MongoDB 7.0+ (Community Server)
- `winutils.exe` (cho Windows)

## 🚀 Hướng Dẫn Cài Đặt

### 1. Cài đặt dependencies Python

```bash
pip install -r requirements.txt
```

### 2. Cấu hình biến môi trường (Windows)

```powershell
# Đổi path cho đúng máy bạn
[System.Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Java\jdk-11", "User")
[System.Environment]::SetEnvironmentVariable("SPARK_HOME", "C:\spark", "User")
[System.Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
[System.Environment]::SetEnvironmentVariable("PYSPARK_PYTHON", "python", "User")
```

### 3. Download dữ liệu

Tải dataset Olist từ Kaggle:
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Giải nén tất cả 9 file CSV vào thư mục `data/`.

### 4. Upload dữ liệu lên HDFS

```bash
# Tạo thư mục HDFS
powershell -ExecutionPolicy Bypass -File "C:\olist-bigdata-project\hdfs_scripts\create_hdfs_dirs.ps1"

# Upload CSV files
powershell -ExecutionPolicy Bypass -File "C:\olist-bigdata-project\hdfs_scripts\upload_to_hdfs.ps1"
```

### 5. Chạy PySpark Pipeline

cd C:\olist-bigdata-project
```bash
# Bước 1: ETL
python spark_jobs/etl.py

# Bước 2: EDA (tùy chọn)
python spark_jobs/eda.py

# Bước 3: Train ML models
python spark_jobs/ml_models.py

# Bước 4: Export kết quả → MongoDB
python spark_jobs/export_to_mongo.py
```

### 6. Chạy Flask Dashboard

```bash
cd C:\olist-bigdata-project\webapp
python app.py
```

Mở browser → http://localhost:5000

## Dataset

**Brazilian E-Commerce Public Dataset by Olist** (~100K đơn hàng, 2016–2018)

| File | Rows | Mô tả |
|---|---|---|
| olist_orders_dataset.csv | ~99K | Đơn hàng |
| olist_order_items_dataset.csv | ~113K | Chi tiết sản phẩm |
| olist_order_payments_dataset.csv | ~104K | Thanh toán |
| olist_order_reviews_dataset.csv | ~100K | Đánh giá |
| olist_customers_dataset.csv | ~99K | Khách hàng |
| olist_products_dataset.csv | ~33K | Sản phẩm |
| olist_sellers_dataset.csv | ~3K | Người bán |
| olist_geolocation_dataset.csv | ~1M | Vị trí địa lý |
| product_category_name_translation.csv | ~71 | Dịch danh mục |

## ML Models

1. **Customer Segmentation** (K-Means) — Phân nhóm khách hàng theo RFM
2. **Churn Prediction** (Random Forest + Logistic Regression) — Dự đoán khách hàng rời bỏ
3. **Review Score Prediction** (Random Forest + Linear Regression) — Dự đoán điểm đánh giá


