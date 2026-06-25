#!/bin/bash
# =============================================================================
# Upload 9 file CSV từ thư mục data/ lên HDFS
# Chạy: bash hdfs_scripts/upload_to_hdfs.sh
# Yêu cầu: Đã download dataset Olist vào thư mục data/
# =============================================================================

DATA_DIR="./data"
HDFS_BASE="/user/bigdata/olist/raw"

echo "📤 Bắt đầu upload dữ liệu lên HDFS..."
echo "📂 Thư mục nguồn: $DATA_DIR"
echo "📂 Thư mục đích: $HDFS_BASE"
echo ""

# Kiểm tra thư mục data có tồn tại không
if [ ! -d "$DATA_DIR" ]; then
    echo "❌ Không tìm thấy thư mục $DATA_DIR"
    echo "   Hãy download dataset Olist từ Kaggle và giải nén vào thư mục data/"
    exit 1
fi

# Upload từng file CSV
upload_file() {
    local file=$1
    local hdfs_dir=$2
    
    if [ -f "$DATA_DIR/$file" ]; then
        echo "  📄 Uploading $file → $HDFS_BASE/$hdfs_dir/"
        hdfs dfs -put -f "$DATA_DIR/$file" "$HDFS_BASE/$hdfs_dir/"
        echo "     ✅ Done"
    else
        echo "  ⚠️ Không tìm thấy $file — bỏ qua"
    fi
}

upload_file "olist_orders_dataset.csv" "orders"
upload_file "olist_order_items_dataset.csv" "order_items"
upload_file "olist_order_payments_dataset.csv" "order_payments"
upload_file "olist_order_reviews_dataset.csv" "order_reviews"
upload_file "olist_customers_dataset.csv" "customers"
upload_file "olist_products_dataset.csv" "products"
upload_file "olist_sellers_dataset.csv" "sellers"
upload_file "olist_geolocation_dataset.csv" "geolocation"
upload_file "product_category_name_translation.csv" "category_translation"

echo ""
echo "✅ Upload hoàn tất!"
echo ""
echo "📊 Kiểm tra dung lượng trên HDFS:"
hdfs dfs -du -h $HDFS_BASE
