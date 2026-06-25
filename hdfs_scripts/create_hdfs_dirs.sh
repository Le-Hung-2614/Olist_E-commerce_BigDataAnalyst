#!/bin/bash
# =============================================================================
# Tạo cấu trúc thư mục HDFS cho dự án Olist
# Chạy: bash hdfs_scripts/create_hdfs_dirs.sh
# =============================================================================

echo "🗂️ Tạo cấu trúc thư mục HDFS..."

# Thư mục gốc dự án
hdfs dfs -mkdir -p /user/bigdata/olist

# Thư mục raw data (9 file CSV)
hdfs dfs -mkdir -p /user/bigdata/olist/raw/orders
hdfs dfs -mkdir -p /user/bigdata/olist/raw/order_items
hdfs dfs -mkdir -p /user/bigdata/olist/raw/order_payments
hdfs dfs -mkdir -p /user/bigdata/olist/raw/order_reviews
hdfs dfs -mkdir -p /user/bigdata/olist/raw/customers
hdfs dfs -mkdir -p /user/bigdata/olist/raw/products
hdfs dfs -mkdir -p /user/bigdata/olist/raw/sellers
hdfs dfs -mkdir -p /user/bigdata/olist/raw/geolocation
hdfs dfs -mkdir -p /user/bigdata/olist/raw/category_translation

# Thư mục processed data (sau ETL)
hdfs dfs -mkdir -p /user/bigdata/olist/processed/merged_orders
hdfs dfs -mkdir -p /user/bigdata/olist/processed/rfm_features
hdfs dfs -mkdir -p /user/bigdata/olist/processed/customer_features

# Thư mục ML models
hdfs dfs -mkdir -p /user/bigdata/olist/models/kmeans_segmentation
hdfs dfs -mkdir -p /user/bigdata/olist/models/churn_classifier
hdfs dfs -mkdir -p /user/bigdata/olist/models/review_predictor

echo "✅ Đã tạo xong cấu trúc thư mục HDFS!"
echo ""
echo "📂 Kiểm tra:"
hdfs dfs -ls -R /user/bigdata/olist/
