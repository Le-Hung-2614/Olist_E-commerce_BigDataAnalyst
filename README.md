# Hệ Thống Phân Tích Dữ Liệu Lớn Sàn Thương Mại Điện Tử Olist (Olist Big Data Analytics)

Dự án Big Data mô phỏng toàn bộ quy trình xây dựng kho dữ liệu (Data Warehouse) và hệ thống học máy (Machine Learning) cho tập dữ liệu thương mại điện tử Olist của Brazil. Hệ thống cung cấp một bảng điều khiển (Dashboard) trực quan để phân tích doanh thu, sản phẩm, phân khúc khách hàng và dự đoán tỷ lệ rời bỏ (Churn Prediction).

---

## Cấu Trúc Thư Mục (Directory Structure)

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

## Kiến Trúc Dữ Liệu (Data Architecture Layers)

Hệ thống được thiết kế theo tiêu chuẩn Data Lake / Data Warehouse với 3 phân lớp (Medallion Architecture):

1. **Bronze Layer (Dữ liệu thô):**
   * Đọc trực tiếp các file `.csv` từ thư mục `data/` bằng PySpark.
   * Dữ liệu giữ nguyên bản chất thô (chứa các giá trị Null, kiểu dữ liệu ngày tháng bị sai dạng chuỗi...).

2. **Silver Layer (Dữ liệu sạch):**
   * Xử lý định dạng lại toàn bộ thời gian (timestamps).
   * Lọc và loại bỏ các giá trị dị biệt (Outliers), xử lý dữ liệu bị thiếu (Missing values).
   * Dịch tên danh mục sản phẩm (Categories) từ tiếng Bồ Đào Nha sang tiếng Anh.
   * Nối (Join) các bảng lại với nhau để tạo ra bộ dữ liệu trung tâm `merged_orders`.

3. **Gold Layer (Dữ liệu phục vụ nghiệp vụ):**
   * Thực hiện các hàm tổng hợp (Aggregations).
   * Chạy thuật toán K-Means Clustering trên PySpark để phân cụm khách hàng theo mô hình RFM (Recency, Frequency, Monetary).
   * Tính toán các chỉ số Vận chuyển (Logistics), Đánh giá (Reviews) và bản đồ phân bổ (Geospatial).
   * Xuất toàn bộ dữ liệu Gold này đẩy thẳng vào **MongoDB** để Web Dashboard hiển thị tốc độ cao (ms).

---

## ⚙️ Quy Trình Hoạt Động (Workflow)

1. **Data Ingestion (Thu thập):** Apache Spark đọc hàng triệu bản ghi từ các file CSV.
2. **Data Pipeline (Xử lý):** File `data_processing.py` chạy qua các pipeline làm sạch dữ liệu, tạo bảng kết hợp và lưu các báo cáo tổng hợp vào MongoDB.
3. **Machine Learning Training:** File `ml_models.py` lấy dữ liệu sạch, sử dụng `RandomForestClassifier` và `LogisticRegression` để học quy luật rời bỏ của khách hàng. Nó xuất ra file `churn_prediction.joblib`.
4. **Data Serving & Visualization:** Web App (`Flask`) kết nối vào MongoDB để đổ dữ liệu ra các biểu đồ Chart.js tuyệt đẹp và tải mô hình `.joblib` lên để cho phép dự đoán trực tiếp ngay trên web.

---

## 🗄 Lược Đồ Dữ Liệu (Database Schema)

Dữ liệu gốc bao gồm 9 bảng (tables) quan hệ chính:
- **`Customers`**: `customer_id`, `customer_unique_id`, `zip_code_prefix`, `city`, `state`.
- **`Orders`**: `order_id`, `customer_id`, `order_status`, `purchase_timestamp`, `delivered_customer_date`...
- **`Order_Items`**: `order_id`, `order_item_id`, `product_id`, `seller_id`, `price`, `freight_value`.
- **`Products`**: `product_id`, `product_category_name`, `product_weight_g`...
- **`Order_Payments`**: `order_id`, `payment_sequential`, `payment_type`, `payment_installments`, `payment_value`.
- **`Order_Reviews`**: `review_id`, `order_id`, `review_score` (1-5), `review_comment_message`.
- **`Sellers`**: `seller_id`, `seller_zip_code_prefix`, `seller_city`, `seller_state`.
- **`Geolocation`**: `geolocation_zip_code_prefix`, `geolocation_lat`, `geolocation_lng`, `geolocation_state`.

> Trên **MongoDB**, dữ liệu được nén lại thành các collection: `orders`, `customers`, `products`, `sellers` và đặc biệt là collection `aggregations` (lưu mọi chỉ số Dashboard đã tính sẵn để tải siêu tốc).

---

## 🛠 Yêu Cầu Cài Đặt (Prerequisites)

Để chạy hệ thống này, máy tính của bạn cần cài đặt:
1. **Python 3.10+**
2. **Java 8 hoặc Java 11** (Bắt buộc để chạy được Apache Spark)
3. **MongoDB** (Phiên bản Community Server, chạy ở port mặc định `27017`)
4. **Hadoop / Winutils** (Nếu bạn dùng Windows để chạy Spark cục bộ)

Các thư viện Python cần cài đặt:
```bash
pip install pyspark pymongo pandas numpy scikit-learn joblib Flask
```

---

# Hướng Dẫn Cách Chạy (How to Run)

## 1. Set biến môi trường (mỗi lần mở terminal mới)
$env:JAVA_HOME = "C:\Java\jdk1.8.0_491"
$env:HADOOP_HOME = "C:\hadoop"

## 2. Khởi động HDFS
& C:\hadoop\sbin\start-dfs.cmd

## 3. Đợi ~15-30 giây cho NameNode khởi động xong

## 4. Kiểm tra HDFS đã chạy chưa
& hdfs dfs -ls /

## 5. Nếu thành công → chạy file tạo thư mục vào HDFS
cd C:\Users\Admin\.gemini\antigravity\scratch\olist-bigdata-project
python spark_jobs/ingestion.py

## 7. Chạy ETL
python spark_jobs/etl.py

## 8. Chạy Model
python spark_jobs/ml_models.py

## 9. Upload dữ liệu lên database MongoDB
python spark_jobs/export_to_mongo.py

## 10. Chạy Dashboard Web
Khởi động hệ thống Flask Backend:

cd webapp
python app.py

Mở trình duyệt web và truy cập vào địa chỉ:
**http://127.0.0.1:5000 hoặc http://127.0.0.1:5000**

---

## ✨ Các Tính Năng Của Dashboard

1. **Tổng quan (Overview):** KPIs tổng như Doanh thu, Số đơn hàng, Giá trị trung bình đơn.
2. **Phân Khúc Khách Hàng (Customer Segmentation):** Mô hình RFM chia khách hàng thành VIP, Loyal, At Risk... 
3. **Mô Hình ML (Churn Prediction):** Trình diễn ma trận nhầm lẫn, biến số quan trọng và Giao diện cho phép giả lập thông tin khách hàng để AI dự đoán "Tỷ lệ rời bỏ" theo thời gian thực.
4. **Sản Phẩm & Giao Vận (Products & Logistics):** Tỷ lệ giao hàng trễ, bản đồ thời gian giao hàng, phân tích ảnh hưởng của phí ship đến tỷ lệ hủy đơn.
5. **Bản Đồ Doanh Thu (Geospatial):** Heatmap doanh thu phân bổ dọc theo các bang của Brazil (SP, RJ, MG...).
