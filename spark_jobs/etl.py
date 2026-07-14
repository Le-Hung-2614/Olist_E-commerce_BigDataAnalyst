"""
=============================================================================
ETL Pipeline - Olist E-Commerce Big Data Project
=============================================================================
Mô tả: Pipeline ETL chính để xử lý dữ liệu Olist Brazilian E-Commerce.
- Đọc 9 file CSV từ HDFS
- Join tất cả bảng thành merged_orders
- Làm sạch dữ liệu (xóa trùng lặp, xử lý null, parse ngày tháng)
- Feature engineering (delivery_days, RFM, churn label)
- Lưu dữ liệu đã xử lý lên HDFS
=============================================================================
"""

import logging
import sys
import os
from datetime import datetime

# Set PYSPARK_PYTHON bằng FULL PATH, dùng Python 3.13.
PYTHON_PATH = "/usr/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYTHON_PATH
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_PATH
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Ép timezone JVM về UTC - tránh lỗi pytz.exceptions.UnknownTimeZoneError
# khi Windows trả múi giờ hệ thống dạng "GMT+07:00" (không chuẩn IANA).
os.environ["TZ"] = "UTC"

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, FloatType
)
from pyspark.sql.window import Window

# ============================================================================
# Cấu hình logging
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("OlistETL")

# ============================================================================
# HDFS Paths - Medallion Architecture (Bronze / Silver / Gold)
# ============================================================================
HDFS_BRONZE = "hdfs://localhost:9000/user/bigdata/olist/bronze"
HDFS_SILVER = "hdfs://localhost:9000/user/bigdata/olist/silver"
HDFS_GOLD = "hdfs://localhost:9000/user/bigdata/olist/gold"


# ============================================================================
# Schema Validation
# ============================================================================
def validate_dataframe_schema(df, expected_columns, label):
    """Validate DataFrame schema truoc khi ghi HDFS."""
    actual_cols = set(df.columns)
    expected_cols = set(expected_columns)
    missing = expected_cols - actual_cols
    if missing:
        raise ValueError(
            f"Schema validation FAILED for {label}: missing columns {missing}"
        )
    row_count = df.count()
    null_report = []
    for col in expected_columns[:10]:  # check top 10 columns
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            null_pct = round(null_count / row_count * 100, 2) if row_count else 0
            null_report.append(f"    {col}: {null_count:,} nulls ({null_pct}%)")
    if null_report:
        logger.info(f"  Schema validation for {label} - null report:")
        for line in null_report:
            logger.info(line)
    logger.info(
        f"  Schema validation PASSED for {label}: "
        f"{len(actual_cols)} columns, {row_count:,} rows"
    )


def create_spark_session():
    """Tạo SparkSession chạy ở chế độ local."""
    logger.info("Đang khởi tạo SparkSession...")
    spark = (
        SparkSession.builder
        .appName("Olist_ETL_Pipeline")
        # Giới hạn còn 4 core thay vì local[*] (toàn bộ core) để tránh
        # tranh chấp RAM giữa quá nhiều thread song song trên máy 8GB.
        .master("local[4]")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        # Tối ưu cho local mode
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.pyspark.python", PYTHON_PATH)
        .config("spark.pyspark.driver.python", PYTHON_PATH)
        # Ép bind đúng địa chỉ loopback - tránh Spark tự dò nhầm sang
        # card mạng ảo (VMware/Hyper-V) gây socket bất ổn.
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        # Tăng timeout cho máy yếu xử lý chậm hơn bình thường.
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        # Ép session timezone UTC - tránh lỗi pytz khi convert timestamp.
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("SparkSession đã khởi tạo thành công.")
    return spark


# ============================================================================
# BƯỚC 1: Đọc dữ liệu từ HDFS
# ============================================================================

def load_raw_data(spark):
    """
    Đọc 9 file CSV từ HDFS.
    Trả về dictionary chứa tất cả DataFrame thô.
    """
    logger.info("=" * 60)
    logger.info("BƯỚC 1: Đọc dữ liệu thô từ HDFS")
    logger.info("=" * 60)

    # Danh sách tên file CSV trên HDFS (thư mục con/tên file)
    csv_files = {
        "orders":       "orders/olist_orders_dataset.csv",
        "order_items":  "order_items/olist_order_items_dataset.csv",
        "payments":     "order_payments/olist_order_payments_dataset.csv",
        "reviews":      "order_reviews/olist_order_reviews_dataset.csv",
        "customers":    "customers/olist_customers_dataset.csv",
        "products":     "products/olist_products_dataset.csv",
        "sellers":      "sellers/olist_sellers_dataset.csv",
        "geolocation":  "geolocation/olist_geolocation_dataset.csv",
        "category_translation": "category_translation/product_category_name_translation.csv",
    }

    dataframes = {}
    for name, filename in csv_files.items():
        path = f"{HDFS_BRONZE}/{filename}"
        logger.info(f"  Đang đọc: {filename}")
        try:
            df = (
                spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .option("encoding", "UTF-8")
                .csv(path)
            )
            record_count = df.count()
            col_count = len(df.columns)
            dataframes[name] = df
            logger.info(f"    -> {name}: {record_count:,} dòng, {col_count} cột")
        except Exception as e:
            logger.error(f"    LỖI khi đọc {filename}: {e}")
            raise

    logger.info(f"Đã đọc thành công {len(dataframes)} bảng dữ liệu.")
    return dataframes


# ============================================================================
# BƯỚC 2: Join tất cả bảng thành merged_orders
# ============================================================================

def join_tables(dataframes):
    """
    Join tất cả bảng thành một DataFrame tổng hợp (merged_orders).
    Chiến lược join:
      orders -> order_items -> products -> category_translation
      orders -> customers
      orders -> payments (đã tổng hợp)
      orders -> reviews
      order_items -> sellers
    """
    logger.info("=" * 60)
    logger.info("BƯỚC 2: Join tất cả bảng dữ liệu")
    logger.info("=" * 60)

    orders = dataframes["orders"]
    order_items = dataframes["order_items"]
    payments = dataframes["payments"]
    reviews = dataframes["reviews"]
    customers = dataframes["customers"]
    products = dataframes["products"]
    sellers = dataframes["sellers"]
    category_translation = dataframes["category_translation"]

    # --- Bước 2a: Tổng hợp payments theo order_id ---
    # Một đơn hàng có thể có nhiều phương thức thanh toán
    logger.info("  2a. Tổng hợp payments theo order_id...")
    payments_agg = (
        payments
        .groupBy("order_id")
        .agg(
            F.sum("payment_value").alias("total_payment_value"),
            F.max("payment_installments").alias("max_installments"),
            F.count("*").alias("payment_count"),
            # Lấy phương thức thanh toán chính (giá trị lớn nhất)
            F.first("payment_type").alias("payment_type"),
            F.collect_set("payment_type").alias("payment_types_used"),
        )
    )
    logger.info(f"    -> payments_agg: {payments_agg.count():,} dòng")

    # --- Bước 2b: Tổng hợp reviews theo order_id ---
    # Một đơn hàng thường có 1 review, nhưng xử lý trường hợp có nhiều
    logger.info("  2b. Tổng hợp reviews theo order_id...")
    reviews_agg = (
        reviews
        .groupBy("order_id")
        .agg(
            F.avg("review_score").alias("review_score"),
            F.first("review_comment_title").alias("review_comment_title"),
            F.first("review_comment_message").alias("review_comment_message"),
            F.first("review_creation_date").alias("review_creation_date"),
            F.count("*").alias("review_count"),
        )
    )
    logger.info(f"    -> reviews_agg: {reviews_agg.count():,} dòng")

    # --- Bước 2c: Dịch tên danh mục sản phẩm sang tiếng Anh ---
    logger.info("  2c. Dịch tên danh mục sản phẩm...")
    products_translated = products.join(
        category_translation,
        on="product_category_name",
        how="left"
    )

    # --- Bước 2d: Join tất cả ---
    logger.info("  2d. Thực hiện join chính...")

    # Join order_items với products (đã dịch)
    items_with_products = order_items.join(
        products_translated,
        on="product_id",
        how="left"
    )

    # Join items_with_products với sellers
    items_with_sellers = items_with_products.join(
        sellers.select(
            F.col("seller_id"),
            F.col("seller_city"),
            F.col("seller_state"),
            F.col("seller_zip_code_prefix"),
        ),
        on="seller_id",
        how="left"
    )

    # Tổng hợp order_items theo order_id để có thông tin cấp đơn hàng
    logger.info("  2e. Tổng hợp order_items theo order_id...")
    items_agg = (
        items_with_sellers
        .groupBy("order_id")
        .agg(
            F.count("*").alias("total_items"),
            F.sum("price").alias("total_price"),
            F.sum("freight_value").alias("total_freight_value"),
            F.countDistinct("product_id").alias("unique_products"),
            F.countDistinct("seller_id").alias("unique_sellers"),
            # Lấy danh mục sản phẩm phổ biến nhất trong đơn hàng
            F.first("product_category_name_english").alias("main_category_english"),
            F.first("product_category_name").alias("main_category"),
            F.first("seller_city").alias("seller_city"),
            F.first("seller_state").alias("seller_state"),
            F.avg("product_weight_g").alias("avg_product_weight_g"),
            F.avg("product_length_cm").alias("avg_product_length_cm"),
            F.avg("product_height_cm").alias("avg_product_height_cm"),
            F.avg("product_width_cm").alias("avg_product_width_cm"),
            F.avg("product_photos_qty").alias("avg_product_photos_qty"),
        )
    )

    # Join chính: orders -> customers -> items_agg -> payments_agg -> reviews_agg
    logger.info("  2f. Thực hiện join cuối cùng...")
    merged = (
        orders
        .join(customers, on="customer_id", how="left")
        .join(items_agg, on="order_id", how="left")
        .join(payments_agg, on="order_id", how="left")
        .join(reviews_agg, on="order_id", how="left")
    )

    record_count = merged.count()
    logger.info(f"  -> merged_orders: {record_count:,} dòng, {len(merged.columns)} cột")
    logger.info(f"  -> Danh sách cột: {merged.columns}")

    return merged


# ============================================================================
# BƯỚC 3: Làm sạch dữ liệu
# ============================================================================

def clean_data(df):
    """
    Làm sạch dữ liệu:
    - Xóa bản ghi trùng lặp
    - Parse cột ngày tháng sang timestamp
    - Xử lý giá trị null
    - Lọc bỏ các đơn hàng không hợp lệ
    """
    logger.info("=" * 60)
    logger.info("BƯỚC 3: Làm sạch dữ liệu")
    logger.info("=" * 60)

    initial_count = df.count()

    # --- 3a: Xóa trùng lặp theo order_id ---
    logger.info("  3a. Xóa bản ghi trùng lặp...")
    df = df.dropDuplicates(["order_id"])
    after_dedup = df.count()
    logger.info(f"    -> Đã xóa {initial_count - after_dedup:,} bản ghi trùng")

    # --- 3b: Parse các cột ngày tháng ---
    logger.info("  3b. Parse cột ngày tháng...")
    date_columns = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for col_name in date_columns:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.to_timestamp(F.col(col_name), "yyyy-MM-dd HH:mm:ss")
            )
            logger.info(f"    -> Đã parse: {col_name}")

    # --- 3c: Xử lý giá trị null ---
    logger.info("  3c. Xử lý giá trị null...")

    # Thống kê null trước khi xử lý
    null_counts = {}
    for col_name in df.columns:
        null_count = df.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            null_counts[col_name] = null_count
    logger.info(f"    -> Các cột có null: {null_counts}")

    # Điền giá trị mặc định cho các cột số
    numeric_fill = {
        "total_price": 0.0,
        "total_freight_value": 0.0,
        "total_payment_value": 0.0,
        "total_items": 0,
        "unique_products": 0,
        "unique_sellers": 0,
        "review_score": 0.0,
        "max_installments": 1,
        "payment_count": 0,
        "review_count": 0,
        "avg_product_weight_g": 0.0,
        "avg_product_length_cm": 0.0,
        "avg_product_height_cm": 0.0,
        "avg_product_width_cm": 0.0,
        "avg_product_photos_qty": 0.0,
    }
    df = df.fillna(numeric_fill)

    # Điền giá trị mặc định cho các cột chuỗi
    string_fill = {
        "main_category_english": "unknown",
        "main_category": "desconhecido",
        "payment_type": "unknown",
        "review_comment_title": "",
        "review_comment_message": "",
        "seller_city": "unknown",
        "seller_state": "unknown",
    }
    df = df.fillna(string_fill)

    # --- 3d: Lọc bỏ đơn hàng không hợp lệ ---
    logger.info("  3d. Lọc bỏ đơn hàng không hợp lệ...")
    # Chỉ giữ các đơn hàng có ngày mua hàng
    df = df.filter(F.col("order_purchase_timestamp").isNotNull())
    # Lọc bỏ đơn bị hủy không có thông tin
    df = df.filter(
        (F.col("order_status") != "canceled") |
        (F.col("total_price") > 0)
    )

    final_count = df.count()
    logger.info(f"  -> Sau làm sạch: {final_count:,} dòng (đã loại {initial_count - final_count:,})")

    return df


# ============================================================================
# BƯỚC 4: Feature Engineering
# ============================================================================

def engineer_features(df):
    """
    Tạo các đặc trưng (features) mới:
    - delivery_days: số ngày giao hàng thực tế
    - estimated_vs_actual: chênh lệch giữa dự kiến và thực tế (ngày)
    - freight_ratio: tỷ lệ phí vận chuyển / tổng giá trị
    - Các đặc trưng thời gian (giờ, ngày trong tuần, tháng, quý)
    """
    logger.info("=" * 60)
    logger.info("BƯỚC 4: Feature Engineering")
    logger.info("=" * 60)

    # --- 4a: Thời gian giao hàng (delivery_days) ---
    logger.info("  4a. Tính thời gian giao hàng...")
    df = df.withColumn(
        "delivery_days",
        F.when(
            F.col("order_delivered_customer_date").isNotNull(),
            F.datediff(
                F.col("order_delivered_customer_date"),
                F.col("order_purchase_timestamp")
            )
        ).otherwise(F.lit(None))
    )

    # --- 4b: Chênh lệch dự kiến vs thực tế ---
    logger.info("  4b. Tính chênh lệch giao hàng dự kiến vs thực tế...")
    df = df.withColumn(
        "estimated_vs_actual",
        F.when(
            F.col("order_delivered_customer_date").isNotNull() &
            F.col("order_estimated_delivery_date").isNotNull(),
            F.datediff(
                F.col("order_estimated_delivery_date"),
                F.col("order_delivered_customer_date")
            )
        ).otherwise(F.lit(None))
    )
    # Giá trị dương = giao sớm hơn dự kiến, âm = giao trễ

    # --- 4c: Tỷ lệ phí vận chuyển ---
    logger.info("  4c. Tính tỷ lệ phí vận chuyển...")
    df = df.withColumn(
        "freight_ratio",
        F.when(
            F.col("total_price") > 0,
            F.round(F.col("total_freight_value") / F.col("total_price"), 4)
        ).otherwise(F.lit(0.0))
    )

    # --- 4d: Tổng giá trị đơn hàng (giá + phí ship) ---
    logger.info("  4d. Tính tổng giá trị đơn hàng...")
    df = df.withColumn(
        "order_value",
        F.col("total_price") + F.col("total_freight_value")
    )

    # --- 4e: Các đặc trưng thời gian ---
    logger.info("  4e. Tạo các đặc trưng thời gian...")
    df = (
        df
        .withColumn("purchase_hour", F.hour("order_purchase_timestamp"))
        .withColumn("purchase_dayofweek", F.dayofweek("order_purchase_timestamp"))
        .withColumn("purchase_month", F.month("order_purchase_timestamp"))
        .withColumn("purchase_year", F.year("order_purchase_timestamp"))
        .withColumn("purchase_quarter", F.quarter("order_purchase_timestamp"))
        .withColumn(
            "purchase_year_month",
            F.date_format("order_purchase_timestamp", "yyyy-MM")
        )
    )

    # --- 4f: Đánh giá chất lượng giao hàng ---
    logger.info("  4f. Phân loại chất lượng giao hàng...")
    df = df.withColumn(
        "delivery_status",
        F.when(F.col("estimated_vs_actual").isNull(), "not_delivered")
         .when(F.col("estimated_vs_actual") >= 0, "on_time")
         .otherwise("late")
    )

    # --- 4g: Phân loại review score ---
    logger.info("  4g. Phân loại mức độ hài lòng...")
    df = df.withColumn(
        "satisfaction_level",
        F.when(F.col("review_score") >= 4, "satisfied")
         .when(F.col("review_score") == 3, "neutral")
         .when(F.col("review_score") >= 1, "dissatisfied")
         .otherwise("no_review")
    )

    logger.info(f"  -> Tổng số cột sau feature engineering: {len(df.columns)}")
    return df


# ============================================================================
# BƯỚC 5: Tính RFM (Recency, Frequency, Monetary) cho từng khách hàng
# ============================================================================

def calculate_rfm(df, spark):
    """
    Tính chỉ số RFM cho từng khách hàng:
    - Recency: số ngày kể từ lần mua cuối
    - Frequency: tổng số đơn hàng
    - Monetary: tổng giá trị chi tiêu

    Trả về: (merged_df với RFM, rfm_df riêng)
    """
    logger.info("=" * 60)
    logger.info("BƯỚC 5: Tính RFM cho từng khách hàng")
    logger.info("=" * 60)

    # Chỉ tính RFM cho đơn hàng đã giao thành công
    delivered_orders = df.filter(F.col("order_status") == "delivered")

    # Tìm ngày mua hàng gần nhất trong toàn bộ dataset (dùng làm mốc)
    max_date = delivered_orders.agg(
        F.max("order_purchase_timestamp")
    ).collect()[0][0]
    logger.info(f"  Ngày mua hàng gần nhất (mốc): {max_date}")

    # Tính RFM cho từng customer_unique_id
    rfm_df = (
        delivered_orders
        .groupBy("customer_unique_id")
        .agg(
            # Recency: số ngày từ lần mua cuối đến ngày mốc
            F.datediff(
                F.lit(max_date),
                F.max("order_purchase_timestamp")
            ).alias("recency"),
            # Frequency: số đơn hàng
            F.countDistinct("order_id").alias("frequency"),
            # Monetary: tổng giá trị chi tiêu
            F.sum("total_payment_value").alias("monetary"),
            # Thêm các chỉ số bổ sung
            F.avg("review_score").alias("avg_review_score"),
            F.avg("delivery_days").alias("avg_delivery_days"),
            F.avg("total_items").alias("avg_items_per_order"),
            F.first("customer_state").alias("customer_state"),
            F.first("customer_city").alias("customer_city"),
            F.max("order_purchase_timestamp").alias("last_purchase_date"),
            F.min("order_purchase_timestamp").alias("first_purchase_date"),
        )
    )

    # Tính thêm: thời gian là khách hàng (ngày)
    rfm_df = rfm_df.withColumn(
        "customer_tenure_days",
        F.datediff(F.col("last_purchase_date"), F.col("first_purchase_date"))
    )

    # Phân hạng RFM bằng ntile (chia thành 4 nhóm)
    logger.info("  Phân hạng RFM (1-4)...")
    r_window = Window.orderBy(F.col("recency").asc())   # Recency thấp = tốt
    f_window = Window.orderBy(F.col("frequency").desc())  # Frequency cao = tốt
    m_window = Window.orderBy(F.col("monetary").desc())   # Monetary cao = tốt

    rfm_df = (
        rfm_df
        .withColumn("r_score", F.ntile(4).over(r_window))
        .withColumn("f_score", F.ntile(4).over(f_window))
        .withColumn("m_score", F.ntile(4).over(m_window))
    )

    # Tạo RFM score tổng hợp
    rfm_df = rfm_df.withColumn(
        "rfm_score",
        F.col("r_score") + F.col("f_score") + F.col("m_score")
    )

    rfm_count = rfm_df.count()
    logger.info(f"  -> Đã tính RFM cho {rfm_count:,} khách hàng")

    # Thống kê RFM
    rfm_df.select("recency", "frequency", "monetary", "rfm_score").describe().show()

    # Join RFM ngược lại vào merged_orders
    logger.info("  Join RFM vào merged_orders...")
    df = df.join(
        rfm_df.select(
            "customer_unique_id", "recency", "frequency", "monetary",
            "r_score", "f_score", "m_score", "rfm_score",
            "avg_review_score", "avg_delivery_days", "customer_tenure_days",
        ),
        on="customer_unique_id",
        how="left"
    )

    return df, rfm_df


# ============================================================================
# BƯỚC 6: Tạo nhãn Churn (khách hàng rời bỏ)
# ============================================================================

def create_churn_label(df):
    """
    Tạo nhãn churn cho khách hàng:
    - churn = 1: khách hàng không mua trong 90 ngày cuối cùng
    - churn = 0: khách hàng vẫn còn hoạt động
    Mốc thời gian: ngày mua hàng gần nhất trong dataset
    """
    logger.info("=" * 60)
    logger.info("BƯỚC 6: Tạo nhãn Churn")
    logger.info("=" * 60)

    # Lấy ngày mua hàng gần nhất trong dataset
    max_date = df.agg(
        F.max("order_purchase_timestamp")
    ).collect()[0][0]

    logger.info(f"  Ngày mốc: {max_date}")
    logger.info("  Ngưỡng churn: 90 ngày không mua hàng")

    # Tạo nhãn churn dựa trên recency
    df = df.withColumn(
        "churn",
        F.when(F.col("recency") > 90, 1).otherwise(0)
    )

    # Thống kê churn
    churn_stats = df.groupBy("churn").count()
    logger.info("  Phân bố churn:")
    churn_stats.show()

    # Tính tỷ lệ churn
    total = df.count()
    churn_count = df.filter(F.col("churn") == 1).count()
    churn_rate = (churn_count / total * 100) if total > 0 else 0
    logger.info(f"  -> Tỷ lệ churn: {churn_rate:.2f}%")

    return df


# ============================================================================
# BƯỚC 7: Lưu dữ liệu đã xử lý lên HDFS
# ============================================================================

def _verify_hdfs_write(spark, hdfs_path, label):
    """
    Xác nhận một thư mục Parquet trên HDFS thực sự có file dữ liệu bên
    trong, không chỉ là thư mục rỗng.

    Lý do cần bước này: nếu Python worker bị chết giữa chừng lúc Spark
    đang ghi (ví dụ do thiếu thư viện, lỗi môi trường...), Spark có thể
    đã tạo xong thư mục đích nhưng KHÔNG kịp ghi file part-*.parquet vào
    trong, và đôi khi không raise lỗi rõ ràng ra ngoài. Đọc thử lại ngay
    sau khi ghi giúp phát hiện sớm tình huống "ghi thành công giả" này,
    thay vì để đến tận lúc chạy ml_models mới phát hiện.
    """
    try:
        check_df = spark.read.parquet(hdfs_path)
        count = check_df.count()
        if count == 0:
            raise RuntimeError(
                f"Thư mục {hdfs_path} tồn tại nhưng đọc lại được 0 dòng "
                f"- nghi ngờ ghi thất bại giữa chừng."
            )
        logger.info(f"  -> Xác nhận {label}: đọc lại được {count:,} dòng từ HDFS. OK.")
        return True
    except Exception as e:
        logger.error(
            f"  XÁC NHẬN THẤT BẠI cho {label} tại {hdfs_path}: {e}\n"
            f"  -> Khả năng cao Spark write đã bị gián đoạn giữa chừng "
            f"(worker process chết) và thư mục HDFS đang RỖNG. Cần chạy "
            f"lại bước ghi này."
        )
        return False


def save_processed_data(merged_df, rfm_df, spark):
    """
    Lưu dữ liệu đã xử lý lên HDFS dưới dạng Parquet.
    - merged_orders: dữ liệu tổng hợp đầy đủ
    - rfm_customers: dữ liệu RFM theo khách hàng

    Sau mỗi lần ghi, đọc lại ngay để xác nhận file thực sự có dữ liệu
    (xem _verify_hdfs_write) - tránh tình trạng thư mục HDFS trông như
    đã ghi xong (có Last Modified time) nhưng thực chất rỗng 0B do
    worker chết giữa chừng mà không báo lỗi rõ ràng.
    """
    logger.info("=" * 60)
    logger.info("BƯỚC 7: Lưu dữ liệu lên HDFS")
    logger.info("=" * 60)

    # --- Schema Validation truoc khi ghi ---
    validate_dataframe_schema(
        merged_df,
        ["order_id", "customer_unique_id", "total_price",
         "delivery_days", "review_score", "order_status"],
        "merged_orders (Silver)"
    )
    validate_dataframe_schema(
        rfm_df,
        ["customer_unique_id", "recency", "frequency", "monetary",
         "r_score", "f_score", "m_score"],
        "rfm_customers (Gold)"
    )

    # Luu merged_orders -> Silver
    merged_path = f"{HDFS_SILVER}/merged_orders"
    logger.info(f"  Dang luu merged_orders -> {merged_path}")
    (
        merged_df
        .coalesce(4)
        .write
        .mode("overwrite")
        .parquet(merged_path)
    )
    logger.info("  -> Lenh ghi merged_orders da chay xong, dang xac nhan...")
    if not _verify_hdfs_write(spark, merged_path, "merged_orders"):
        raise RuntimeError(
            "Ghi merged_orders len HDFS that bai (thu muc rong sau khi "
            "ghi). Kiem tra lai cau hinh PYSPARK_PYTHON / PATH truoc khi "
            "chay lai."
        )

    # Luu rfm_customers -> Gold
    rfm_path = f"{HDFS_GOLD}/rfm_customers"
    logger.info(f"  Dang luu rfm_customers -> {rfm_path}")
    (
        rfm_df
        .coalesce(2)
        .write
        .mode("overwrite")
        .parquet(rfm_path)
    )
    logger.info("  -> Lenh ghi rfm_customers da chay xong, dang xac nhan...")
    if not _verify_hdfs_write(spark, rfm_path, "rfm_customers"):
        raise RuntimeError(
            "Ghi rfm_customers len HDFS that bai (thu muc rong sau khi "
            "ghi). Kiem tra lai cau hinh PYSPARK_PYTHON / PATH truoc khi "
            "chay lai."
        )

    return merged_path, rfm_path


# ============================================================================
# MAIN: Chạy toàn bộ pipeline
# ============================================================================

def run_etl_pipeline():
    """
    Chạy toàn bộ ETL pipeline từ đầu đến cuối.
    Trả về: (spark, merged_df, rfm_df)
    """
    start_time = datetime.now()
    logger.info("*" * 60)
    logger.info("BẮT ĐẦU ETL PIPELINE - Olist E-Commerce")
    logger.info(f"Thời gian bắt đầu: {start_time}")
    logger.info("*" * 60)

    try:
        # Bước 0: Khởi tạo Spark
        spark = create_spark_session()

        # Bước 1: Đọc dữ liệu thô
        dataframes = load_raw_data(spark)

        # Bước 2: Join tất cả bảng
        merged_df = join_tables(dataframes)

        # Bước 3: Làm sạch dữ liệu
        merged_df = clean_data(merged_df)

        # Bước 4: Feature Engineering
        merged_df = engineer_features(merged_df)

        # Bước 5: Tính RFM
        merged_df, rfm_df = calculate_rfm(merged_df, spark)

        # Bước 6: Tạo nhãn Churn
        merged_df = create_churn_label(merged_df)

        # Bước 7: Lưu dữ liệu
        merged_path, rfm_path = save_processed_data(merged_df, rfm_df, spark)

        # Tóm tắt kết quả
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("*" * 60)
        logger.info("ETL PIPELINE HOÀN THÀNH!")
        logger.info(f"  Thời gian chạy: {duration:.1f} giây")
        logger.info(f"  Merged orders: {merged_df.count():,} dòng")
        logger.info(f"  RFM customers: {rfm_df.count():,} dòng")
        logger.info(f"  Silver: {HDFS_SILVER}")
        logger.info(f"  Gold: {HDFS_GOLD}")
        logger.info("*" * 60)

        return spark, merged_df, rfm_df

    except Exception as e:
        logger.error(f"LỖI PIPELINE: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    spark, merged_df, rfm_df = run_etl_pipeline()

    # Hiển thị schema cuối cùng
    logger.info("\n=== SCHEMA CỦA MERGED_ORDERS ===")
    merged_df.printSchema()

    # Hiển thị mẫu dữ liệu
    logger.info("\n=== MẪU DỮ LIỆU MERGED_ORDERS ===")
    merged_df.select(
        "order_id", "customer_unique_id", "order_status",
        "total_price", "delivery_days", "review_score",
        "recency", "frequency", "monetary", "churn"
    ).show(10, truncate=False)

    # Dừng SparkSession
    spark.stop()
    logger.info("SparkSession đã dừng.")
