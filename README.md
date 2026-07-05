# Olist E-Commerce Big Data Analytics Pipeline

![Python](https://img.shields.io/badge/Python-3.12+-blue.svg)
![Hadoop](https://img.shields.io/badge/Hadoop-3.x-yellow.svg)
![Spark](https://img.shields.io/badge/Spark-3.5.x-orange.svg)
![MongoDB](https://img.shields.io/badge/MongoDB-Latest-green.svg)
![Flask](https://img.shields.io/badge/Flask-Web_Framework-black.svg)

Dự án xây dựng một hệ thống xử lý Dữ liệu Lớn (Big Data) end-to-end cho tập dữ liệu thương mại điện tử Olist. Hệ thống bao quát toàn bộ quy trình từ khâu thu thập dữ liệu (Ingestion), xây dựng Hồ dữ liệu (Data Lake) với kiến trúc Medallion, xử lý song song, ứng dụng Học máy (Machine Learning) phân tán, và cuối cùng là trực quan hóa dữ liệu trên Dashboard thời gian thực.

---

## 1. Giới thiệu Dataset (Olist Store)
Dự án sử dụng bộ dữ liệu công khai **[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)** từ Kaggle.
Olist là nền tảng thương mại điện tử lớn nhất tại Brazil. Tập dữ liệu này chứa thông tin của **hơn 100.000 đơn hàng** được đặt trong khoảng thời gian từ năm 2016 đến 2018.

Dữ liệu được chia thành 9 file CSV riêng biệt, bao gồm các thông tin:
*   Đơn hàng & Trạng thái giao hàng (`olist_orders_dataset.csv`)
*   Chi tiết sản phẩm trong đơn (`olist_order_items_dataset.csv`)
*   Thanh toán (`olist_order_payments_dataset.csv`)
*   Đánh giá của khách hàng (`olist_order_reviews_dataset.csv`)
*   Khách hàng (`olist_customers_dataset.csv`)
*   Người bán (`olist_sellers_dataset.csv`)
*   Sản phẩm (`olist_products_dataset.csv`)
*   Vị trí địa lý (`olist_geolocation_dataset.csv`)
*   Dịch tên danh mục (`product_category_name_translation.csv`)

---

## 2. Kiến trúc Hệ thống

Hệ thống được thiết kế theo các tiêu chuẩn Big Data hiện đại nhất:
*   **Data Lake (HDFS):** Áp dụng **Medallion Architecture** (Bronze ➔ Silver ➔ Gold) để lưu trữ và phân tầng dữ liệu theo mức độ sạch/giá trị.
*   **Xử lý phân tán (Apache Spark):** Đảm nhiệm vai trò ETL (Extract, Transform, Load) khổng lồ, join 9 bảng lại với nhau.
*   **Machine Learning (Spark MLlib):**
    *   *K-Means:* Phân cụm khách hàng (Segmentation) dựa trên mô hình RFM.
    *   *Random Forest / Logistic Regression:* Dự đoán xác suất khách hàng rời bỏ (Churn Prediction).
*   **Data Warehouse (MongoDB):** Áp dụng mô hình **Denormalized Star Schema** kết hợp JSON Schema Validation. Mọi thông tin (Items, Customer, Payment, Review) được nhúng (Embed) vào bảng `orders` để tối ưu hóa tốc độ đọc (Read-heavy).
*   **Trực quan hóa (Flask + Chart.js):** Dashboard hiển thị số liệu kinh doanh, biểu đồ doanh thu và kết quả phân tích AI trực quan.

---

## 3. Yêu cầu Hệ thống (System Requirements)

Để chạy dự án trên môi trường Local (Windows), bạn cần cài đặt sẵn:
1.  **Hệ điều hành:** Windows (với winutils.exe hỗ trợ Hadoop).
2.  **Java:** JDK 8 hoặc JDK 11 (yêu cầu bắt buộc cho Hadoop và Spark).
3.  **Python:** Phiên bản `3.12+`.
4.  **Hadoop:** Phiên bản `3.x` (Đảm bảo HDFS đã được start tại `hdfs://localhost:9000`).
5.  **Apache Spark:** Phiên bản `3.5.x` (Đã cấu hình các biến môi trường `SPARK_HOME`, `HADOOP_HOME`, `PYSPARK_PYTHON`).
6.  **MongoDB:** Đang chạy tại `localhost:27017` (Cài đặt MongoDB Community Server & MongoDB Compass).

---

## 4. Hướng dẫn Cài đặt (Installation)

**Bước 1: Clone dự án và truy cập thư mục**
```bash
cd olist-bigdata-project
```

**Bước 2: Tạo môi trường ảo Python (Khuyến nghị)**
```bash
python -m venv venv
venv\Scripts\activate
```

**Bước 3: Cài đặt các thư viện phụ thuộc**
```bash
pip install -r requirements.txt
```

---

## 5. Hướng dẫn Chạy Hệ thống (Run Pipeline)

Quy trình Big Data Pipeline bắt buộc phải chạy theo thứ tự tuần tự để dữ liệu được luân chuyển từ Raw (CSV) đến Dashboard.

*(Lưu ý: Trước khi chạy, đảm bảo Hadoop HDFS và MongoDB đã được bật)*

### Bước 5.1: Thu thập và Validate Dữ liệu (Ingestion)
Đọc 9 file CSV, kiểm tra tính hợp lệ (Data Quality check), và tải lên HDFS `Bronze Layer` (`/user/bigdata/olist/bronze/`).
```bash
python spark_jobs/ingestion.py
```

### Bước 5.2: Xử lý ETL (Silver & Gold Layers)
Dùng Spark dọn dẹp dữ liệu, join các bảng, loại bỏ ngoại lệ và lưu xuống HDFS `Silver Layer` và `Gold Layer`.
```bash
python spark_jobs/etl.py
```

### Bước 5.3: Huấn luyện Machine Learning
Chạy thuật toán phân cụm khách hàng và dự đoán tỷ lệ rời bỏ. Kết quả được lưu tiếp vào `Gold Layer`.
```bash
python spark_jobs/ml_models.py
```

### Bước 5.4: Xuất dữ liệu sang MongoDB
Đẩy toàn bộ dữ liệu sạch và kết quả ML từ HDFS sang MongoDB `olist_dw`. Đồng thời áp dụng Schema Validators cho database.
```bash
python spark_jobs/export_to_mongo.py
```

### Bước 5.5: Khởi động Web Dashboard
Khởi chạy Flask server.
```bash
python webapp/app.py
```
👉 Mở trình duyệt và truy cập: **`http://127.0.0.1:5000`**

---

## 6. Cấu trúc Thư mục

```text
olist-bigdata-project/
├── data/                  # Nơi chứa các file dữ liệu CSV gốc (Tải từ Kaggle)
├── spark_jobs/            # Toàn bộ mã nguồn Big Data / Spark
│   ├── ingestion.py       # Tải & validate dữ liệu lên HDFS Bronze
│   ├── etl.py             # Dọn dẹp & biến đổi dữ liệu (Silver/Gold)
│   ├── ml_models.py       # Spark MLlib (KMeans, RandomForest)
│   ├── eda.py             # Phân tích khám phá trên Terminal (Tùy chọn)
│   └── export_to_mongo.py # Xuất dữ liệu sang NoSQL
├── webapp/                # Mã nguồn Flask Web Dashboard
│   ├── app.py             # Server Flask xử lý API và Route
│   ├── config.py          # File cấu hình kết nối DB
│   ├── templates/         # Giao diện HTML
│   └── static/            # CSS và JS (Chart.js)
├── requirements.txt       # Danh sách thư viện Python
└── README.md              # File hướng dẫn
```

---
*Developed for Big Data Systems Project*
