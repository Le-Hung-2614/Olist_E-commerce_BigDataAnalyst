"""
=============================================================================
Export to MongoDB - Olist E-Commerce Big Data Project
=============================================================================
Mô tả: Xuất dữ liệu đã xử lý và kết quả phân tích từ HDFS sang MongoDB.
- Sử dụng mongo-spark-connector để kết nối PySpark với MongoDB
- Xuất 5 collections: orders, customers, products, sellers, aggregations
- Database: olist_dw trên MongoDB localhost:27017
=============================================================================
"""

import logging
import sys
import os
import json
from datetime import datetime

import pandas as pd
from pymongo import MongoClient

# Set PYSPARK_PYTHON bằng FULL PATH, dùng Python 3.12 (đã xác nhận có đủ
# pyspark + pandas + pyarrow trên máy này).
PYTHON_PATH = "/usr/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYTHON_PATH
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_PATH
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["TZ"] = "UTC"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql.window import Window

# ============================================================================
# Cấu hình
# ============================================================================
# Fix Unicode cho Windows console (cp1252 khong ho tro tieng Viet)
import io
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'buffer'):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("OlistMongo")

# Cấu hình MongoDB
MONGO_HOST = "localhost"
MONGO_PORT = "27017"
MONGO_DATABASE = "olist_dw"
MONGO_URI = f"mongodb://{MONGO_HOST}:{MONGO_PORT}/{MONGO_DATABASE}"
# pymongo.MongoClient không cần (và không nên) có tên database trong
# URI khi sẽ chọn database riêng qua client[MONGO_DATABASE] sau đó.
MONGO_URI_BASE = f"mongodb://{MONGO_HOST}:{MONGO_PORT}/"

# HDFS Paths - Medallion Architecture (Bronze / Silver / Gold)
HDFS_BRONZE = "hdfs://localhost:9000/user/bigdata/olist/bronze"
HDFS_SILVER = "hdfs://localhost:9000/user/bigdata/olist/silver"
HDFS_GOLD = "hdfs://localhost:9000/user/bigdata/olist/gold"


def create_spark_session():
    """
    Tạo SparkSession.

    CHỈ dùng để đọc từ HDFS và transform dữ liệu (DataFrame API) - không
    dùng để ghi trực tiếp vào MongoDB nữa. Việc ghi MongoDB được tách
    riêng bằng pandas + pymongo (xem write_to_mongodb), vì native
    "df.write.format('mongodb')" từng có rủi ro lỗi socket giống các
    lần Spark MLlib gặp trước đây (Python worker giao tiếp qua socket
    nội bộ JVM<->Python, dễ gãy trên máy 8GB RAM Windows).
    """
    logger.info("Đang khởi tạo SparkSession (chỉ để đọc/transform từ HDFS)...")
    spark = (
        SparkSession.builder
        .appName("Olist_Export_MongoDB")
        .master("local[4]")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .config("spark.pyspark.python", PYTHON_PATH)
        .config("spark.pyspark.driver.python", PYTHON_PATH)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession đã khởi tạo thành công.")
    return spark


def _spark_df_to_pandas_safe(spark_df):
    from pyspark.sql.types import TimestampType, DateType, StructType

    ts_cols = [
        f.name for f in spark_df.schema.fields
        if isinstance(f.dataType, (TimestampType, DateType))
    ]
    struct_cols = [
        f.name for f in spark_df.schema.fields
        if isinstance(f.dataType, StructType)
    ]

    df_to_convert = spark_df
    for c in ts_cols:
        df_to_convert = df_to_convert.withColumn(c, F.col(c).cast("string"))

    pdf = df_to_convert.toPandas()

    for c in ts_cols:
        pdf[c] = pd.to_datetime(pdf[c], errors="coerce")

    for c in struct_cols:
        pdf[c] = pdf[c].apply(lambda r: r.asDict(recursive=True) if r is not None else None)

    return pdf


def _write_records_to_mongodb(records, collection_name, mode="overwrite"):
    """
    Ghi một list[dict] Python thuần vào MongoDB collection, không cần
    đi qua Spark/pandas. Dùng cho dữ liệu nhỏ đã có sẵn dạng Python
    (vd: tổng hợp kết quả ML chỉ vài dòng).
    """
    record_count = len(records)
    logger.info(f"  Đang ghi {record_count:,} documents -> collection '{collection_name}'...")

    if record_count == 0:
        logger.warning(f"  Collection '{collection_name}': không có dữ liệu, bỏ qua.")
        return 0

    try:
        client = MongoClient(MONGO_URI_BASE, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client[MONGO_DATABASE]
        collection = db[collection_name]

        if mode == "overwrite":
            collection.delete_many({})

        collection.insert_many(records)
        logger.info(f"  -> Đã ghi thành công: {collection_name} ({record_count:,} documents)")
        client.close()
    except Exception as e:
        logger.error(f"  LỖI khi ghi {collection_name}: {e}")
        raise

    return record_count


def write_to_mongodb(df, collection_name, mode="overwrite"):
    """
    Ghi một Spark DataFrame vào MongoDB collection.

    Cách làm: convert sang pandas an toàn (_spark_df_to_pandas_safe),
    rồi dùng pymongo để insert trực tiếp - KHÔNG dùng
    "df.write.format('mongodb')" của Spark nữa, để tránh rủi ro lỗi
    socket JVM<->Python từng gặp nhiều lần với Spark MLlib.
    """
    pdf = _spark_df_to_pandas_safe(df)
    record_count = len(pdf)
    logger.info(f"  Đang ghi {record_count:,} documents -> collection '{collection_name}'...")

    if record_count == 0:
        logger.warning(f"  Collection '{collection_name}': không có dữ liệu, bỏ qua.")
        return 0

    # Convert NaN/NaT thành None để MongoDB lưu đúng dạng null thay vì
    # lỗi serialize (pymongo không hiểu NaN của pandas/numpy).
    records = json.loads(pdf.to_json(orient="records", date_format="iso"))

    try:
        client = MongoClient(MONGO_URI_BASE, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")  # xác nhận kết nối thật, không chỉ tạo object
        db = client[MONGO_DATABASE]
        collection = db[collection_name]

        if mode == "overwrite":
            collection.delete_many({})
        # mode == "append": không xóa gì, chỉ insert thêm

        if records:
            collection.insert_many(records)

        logger.info(f"  -> Đã ghi thành công: {collection_name} ({record_count:,} documents)")
        client.close()
    except Exception as e:
        logger.error(f"  LỖI khi ghi {collection_name}: {e}")
        raise

    return record_count


def load_all_data(spark):
    """Đọc tất cả dữ liệu cần thiết từ HDFS."""
    logger.info("=" * 60)
    logger.info("ĐỌC DỮ LIỆU TỪ HDFS")
    logger.info("=" * 60)

    data = {}

    # Đọc dữ liệu đã xử lý
    data["merged_orders"] = spark.read.parquet(f"{HDFS_SILVER}/merged_orders")
    logger.info(f"  merged_orders: {data['merged_orders'].count():,} dòng")

    data["rfm_customers"] = spark.read.parquet(f"{HDFS_GOLD}/rfm_customers")
    logger.info(f"  rfm_customers: {data['rfm_customers'].count():,} dòng")

    # Đọc kết quả phân khúc (nếu có)
    try:
        data["customer_segments"] = spark.read.parquet(
            f"{HDFS_GOLD}/customer_segments"
        )
        logger.info(f"  customer_segments: {data['customer_segments'].count():,} dòng")
    except Exception:
        logger.warning("  Không tìm thấy customer_segments. Bỏ qua.")
        data["customer_segments"] = None

    # Đọc kết quả dự đoán churn (nếu có)
    try:
        data["churn_predictions"] = spark.read.parquet(
            f"{HDFS_GOLD}/churn_predictions"
        )
        logger.info(f"  churn_predictions: {data['churn_predictions'].count():,} dòng")
    except Exception:
        logger.warning("  Không tìm thấy churn_predictions. Bỏ qua.")
        data["churn_predictions"] = None

    # Đọc dữ liệu thô bổ sung
    # LƯU Ý: mỗi file CSV gốc nằm trong MỘT SUBFOLDER riêng theo tên
    # (vd /raw/customers/olist_customers_dataset.csv), không nằm thẳng
    # trong /raw/ - đây là cấu trúc thật đã xác nhận qua Hadoop UI.
    data["raw_order_items"] = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{HDFS_BRONZE}/order_items/olist_order_items_dataset.csv")
    )
    data["raw_products"] = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{HDFS_BRONZE}/products/olist_products_dataset.csv")
    )
    data["raw_sellers"] = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{HDFS_BRONZE}/sellers/olist_sellers_dataset.csv")
    )
    data["raw_category_translation"] = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{HDFS_BRONZE}/category_translation/product_category_name_translation.csv")
    )

    return data


# ============================================================================
# COLLECTION 1: ORDERS
# ============================================================================

def export_orders(data):
    """
    Xuất collection orders: mỗi document là một đơn hàng
    với thông tin khách hàng, sản phẩm, thanh toán, đánh giá nhúng bên trong.
    """
    logger.info("=" * 60)
    logger.info("COLLECTION 1: ORDERS")
    logger.info("=" * 60)

    merged = data["merged_orders"]

    # Chọn các cột cần thiết và tổ chức thành document
    orders_doc = merged.select(
        # Thông tin đơn hàng
        F.col("order_id"),
        F.col("order_status"),
        F.col("order_purchase_timestamp"),
        F.col("order_approved_at"),
        F.col("order_delivered_carrier_date"),
        F.col("order_delivered_customer_date"),
        F.col("order_estimated_delivery_date"),

        # Nhúng thông tin khách hàng
        F.struct(
            F.col("customer_id"),
            F.col("customer_unique_id"),
            F.col("customer_city"),
            F.col("customer_state"),
            F.col("customer_zip_code_prefix"),
        ).alias("customer"),

        # Nhúng thông tin sản phẩm/đơn hàng
        F.struct(
            F.col("total_items"),
            F.col("total_price"),
            F.col("total_freight_value"),
            F.col("unique_products"),
            F.col("unique_sellers"),
            F.col("main_category_english").alias("category"),
            F.col("seller_city"),
            F.col("seller_state"),
        ).alias("items"),

        # Nhúng thông tin thanh toán
        F.struct(
            F.col("total_payment_value"),
            F.col("payment_type"),
            F.col("max_installments").alias("installments"),
            F.col("payment_count"),
        ).alias("payment"),

        # Nhúng thông tin đánh giá
        F.struct(
            F.col("review_score"),
            F.col("review_comment_title"),
            F.col("review_comment_message"),
            F.col("review_count"),
            F.col("satisfaction_level"),
        ).alias("review"),

        # Các đặc trưng đã tính
        F.col("delivery_days"),
        F.col("estimated_vs_actual"),
        F.col("freight_ratio"),
        F.col("order_value"),
        F.col("delivery_status"),
        F.col("purchase_hour"),
        F.col("purchase_dayofweek"),
        F.col("purchase_month"),
        F.col("purchase_year"),
        F.col("purchase_year_month"),
    )

    write_to_mongodb(orders_doc, "orders")
    return orders_doc.count()


# ============================================================================
# COLLECTION 2: CUSTOMERS
# ============================================================================

def export_customers(data):
    """
    Xuất collection customers: mỗi document là một khách hàng
    với RFM scores, segment, churn probability.
    """
    logger.info("=" * 60)
    logger.info("COLLECTION 2: CUSTOMERS")
    logger.info("=" * 60)

    rfm = data["rfm_customers"]

    # Bắt đầu với RFM data
    customer_doc = rfm.select(
        "customer_unique_id",
        "customer_state",
        "customer_city",
        "recency",
        "frequency",
        "monetary",
        "r_score",
        "f_score",
        "m_score",
        "rfm_score",
        "avg_review_score",
        "avg_delivery_days",
        "customer_tenure_days",
        "last_purchase_date",
        "first_purchase_date",
    )

    # Join với customer_segments (nếu có)
    if data["customer_segments"] is not None:
        segments = data["customer_segments"].select(
            "customer_unique_id",
            "cluster",
            "segment_name",
        )
        customer_doc = customer_doc.join(segments, on="customer_unique_id", how="left")
    else:
        customer_doc = (
            customer_doc
            .withColumn("cluster", F.lit(None).cast("int"))
            .withColumn("segment_name", F.lit("Unknown"))
        )

    # Join với churn_predictions (nếu có)
    # ml_models.py đã lưu churn_probability dạng float (không phải vector)
    if data["churn_predictions"] is not None:
        churn = data["churn_predictions"].select(
            "customer_unique_id",
            F.col("prediction").alias("churn_prediction"),
            F.col("churn_probability"),
        )
        customer_doc = customer_doc.join(churn, on="customer_unique_id", how="left")
    else:
        customer_doc = (
            customer_doc
            .withColumn("churn_prediction", F.lit(None).cast("double"))
            .withColumn("churn_probability", F.lit(None).cast("double"))
        )

    # Điền giá trị mặc định
    customer_doc = customer_doc.fillna({
        "segment_name": "Unknown",
        "churn_prediction": 0.0,
        "churn_probability": 0.0,
    })

    write_to_mongodb(customer_doc, "customers")
    return customer_doc.count()


# ============================================================================
# COLLECTION 3: PRODUCTS
# ============================================================================

def export_products(data):
    """
    Xuất collection products: thống kê tổng hợp cho từng sản phẩm.
    """
    logger.info("=" * 60)
    logger.info("COLLECTION 3: PRODUCTS")
    logger.info("=" * 60)

    order_items = data["raw_order_items"]
    products = data["raw_products"]
    merged = data["merged_orders"]
    category_translation = data["raw_category_translation"]

    # Tổng hợp thống kê sản phẩm từ order_items
    product_stats = (
        order_items
        .groupBy("product_id")
        .agg(
            F.count("*").alias("total_orders"),
            F.sum("price").alias("total_revenue"),
            F.avg("price").alias("avg_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.sum("freight_value").alias("total_freight"),
            F.avg("freight_value").alias("avg_freight"),
            F.countDistinct("order_id").alias("unique_orders"),
            F.countDistinct("seller_id").alias("seller_count"),
        )
    )

    # Tính review trung bình cho mỗi sản phẩm (từ merged_orders)
    product_reviews = (
        merged
        .filter(F.col("review_score") > 0)
        .join(
            order_items.select("order_id", "product_id"),
            on="order_id",
            how="inner"
        )
        .groupBy("product_id")
        .agg(
            F.avg("review_score").alias("avg_review_score"),
            F.count("review_score").alias("review_count"),
        )
    )

    # Join thông tin sản phẩm gốc
    products_with_translation = products.join(
        category_translation, on="product_category_name", how="left"
    )

    product_doc = (
        products_with_translation
        .join(product_stats, on="product_id", how="left")
        .join(product_reviews, on="product_id", how="left")
        .select(
            "product_id",
            F.col("product_category_name").alias("category_pt"),
            F.col("product_category_name_english").alias("category_en"),
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
            "total_orders",
            "total_revenue",
            "avg_price",
            "min_price",
            "max_price",
            "total_freight",
            "avg_freight",
            "unique_orders",
            "seller_count",
            "avg_review_score",
            "review_count",
        )
        .fillna({
            "total_orders": 0,
            "total_revenue": 0.0,
            "avg_price": 0.0,
            "category_en": "unknown",
            "avg_review_score": 0.0,
            "review_count": 0,
        })
    )

    write_to_mongodb(product_doc, "products")
    return product_doc.count()


# ============================================================================
# COLLECTION 4: SELLERS
# ============================================================================

def export_sellers(data):
    """
    Xuất collection sellers: thống kê tổng hợp cho từng người bán.
    """
    logger.info("=" * 60)
    logger.info("COLLECTION 4: SELLERS")
    logger.info("=" * 60)

    order_items = data["raw_order_items"]
    sellers = data["raw_sellers"]
    merged = data["merged_orders"]

    # Tổng hợp thống kê người bán từ order_items
    seller_stats = (
        order_items
        .groupBy("seller_id")
        .agg(
            F.count("*").alias("total_items_sold"),
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("price").alias("total_revenue"),
            F.avg("price").alias("avg_price"),
            F.sum("freight_value").alias("total_freight"),
            F.avg("freight_value").alias("avg_freight"),
            F.countDistinct("product_id").alias("unique_products"),
        )
    )

    # Tính metrics từ merged_orders (delivery, review)
    seller_metrics = (
        merged
        .filter(F.col("seller_state").isNotNull())
        .join(
            order_items.select("order_id", "seller_id"),
            on="order_id",
            how="inner"
        )
        .groupBy("seller_id")
        .agg(
            F.avg("review_score").alias("avg_review_score"),
            F.avg("delivery_days").alias("avg_delivery_days"),
            F.countDistinct("customer_unique_id").alias("unique_customers"),
            F.min("order_purchase_timestamp").alias("first_sale_date"),
            F.max("order_purchase_timestamp").alias("last_sale_date"),
        )
    )

    # Join tất cả
    seller_doc = (
        sellers
        .join(seller_stats, on="seller_id", how="left")
        .join(seller_metrics, on="seller_id", how="left")
        .select(
            "seller_id",
            "seller_city",
            "seller_state",
            "seller_zip_code_prefix",
            "total_items_sold",
            "total_orders",
            "total_revenue",
            "avg_price",
            "total_freight",
            "avg_freight",
            "unique_products",
            "avg_review_score",
            "avg_delivery_days",
            "unique_customers",
            "first_sale_date",
            "last_sale_date",
        )
        .fillna({
            "total_items_sold": 0,
            "total_orders": 0,
            "total_revenue": 0.0,
            "avg_price": 0.0,
            "avg_review_score": 0.0,
            "avg_delivery_days": 0.0,
            "unique_customers": 0,
        })
    )

    write_to_mongodb(seller_doc, "sellers")
    return seller_doc.count()


# ============================================================================
# COLLECTION 5: AGGREGATIONS (dữ liệu tổng hợp cho dashboard)
# ============================================================================

def export_aggregations(data, spark, ml_results=None):
    """
    Xuất collection aggregations: dữ liệu tổng hợp sẵn cho dashboard.
    Bao gồm nhiều sub-documents:
    - monthly_revenue
    - category_stats
    - state_stats
    - hourly_orders (giờ × ngày trong tuần)
    - payment_methods
    - model_performance
    - segment_stats
    """
    logger.info("=" * 60)
    logger.info("COLLECTION 5: AGGREGATIONS")
    logger.info("=" * 60)

    merged = data["merged_orders"]
    total_exported = 0

    # ---- 5a: Monthly Revenue ----
    logger.info("  5a. Tổng hợp doanh thu theo tháng...")
    monthly_revenue = (
        merged
        .groupBy("purchase_year_month")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("total_payment_value").alias("total_revenue"),
            F.avg("total_payment_value").alias("avg_order_value"),
            F.countDistinct("customer_unique_id").alias("unique_customers"),
            F.avg("review_score").alias("avg_review_score"),
            F.avg("delivery_days").alias("avg_delivery_days"),
        )
        .withColumn("agg_type", F.lit("monthly_revenue"))
        .orderBy("purchase_year_month")
    )
    write_to_mongodb(monthly_revenue, "aggregations", mode="overwrite")
    total_exported += monthly_revenue.count()

    # ---- 5b: Category Stats ----
    logger.info("  5b. Tổng hợp thống kê theo danh mục sản phẩm...")
    category_stats = (
        merged
        .filter(F.col("main_category_english") != "unknown")
        .groupBy("main_category_english")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("total_payment_value").alias("total_revenue"),
            F.avg("total_payment_value").alias("avg_order_value"),
            F.avg("review_score").alias("avg_review_score"),
            F.avg("delivery_days").alias("avg_delivery_days"),
            F.countDistinct("customer_unique_id").alias("unique_customers"),
            F.sum("total_items").alias("total_items_sold"),
        )
        .withColumn("agg_type", F.lit("category_stats"))
        .orderBy(F.col("total_revenue").desc())
    )
    write_to_mongodb(category_stats, "aggregations", mode="append")
    total_exported += category_stats.count()

    # ---- 5c: State Stats ----
    logger.info("  5c. Tổng hợp thống kê theo bang...")
    state_stats = (
        merged
        .groupBy("customer_state")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("total_payment_value").alias("total_revenue"),
            F.avg("total_payment_value").alias("avg_order_value"),
            F.countDistinct("customer_unique_id").alias("unique_customers"),
            F.avg("review_score").alias("avg_review_score"),
            F.avg("delivery_days").alias("avg_delivery_days"),
        )
        .withColumn("agg_type", F.lit("state_stats"))
        .orderBy(F.col("total_revenue").desc())
    )
    write_to_mongodb(state_stats, "aggregations", mode="append")
    total_exported += state_stats.count()

    # ---- 5d: Hourly Orders (giờ × ngày trong tuần) ----
    logger.info("  5d. Tổng hợp đơn hàng theo giờ × ngày trong tuần...")
    hourly_orders = (
        merged
        .groupBy("purchase_dayofweek", "purchase_hour")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("total_payment_value").alias("total_revenue"),
        )
        .withColumn("agg_type", F.lit("hourly_orders"))
        .orderBy("purchase_dayofweek", "purchase_hour")
    )
    write_to_mongodb(hourly_orders, "aggregations", mode="append")
    total_exported += hourly_orders.count()

    # ---- 5e: Payment Methods ----
    logger.info("  5e. Tổng hợp phương thức thanh toán...")
    payment_methods = (
        merged
        .groupBy("payment_type")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("total_payment_value").alias("total_revenue"),
            F.avg("total_payment_value").alias("avg_payment_value"),
            F.avg("max_installments").alias("avg_installments"),
        )
        .withColumn("agg_type", F.lit("payment_methods"))
        .orderBy(F.col("order_count").desc())
    )
    write_to_mongodb(payment_methods, "aggregations", mode="append")
    total_exported += payment_methods.count()

    # ---- 5f: Model Performance ----
    logger.info("  5f. Xuất kết quả hiệu năng mô hình ML...")
    if ml_results:
        perf_rows = []
        for model_key, model_data in ml_results.items():
            # Bo qua cac key khong phai model (timestamp, duration_seconds, ...)
            if not isinstance(model_data, dict):
                continue
            perf_rows.append({
                "model_name": model_data.get("model_name", model_key),
                "best_model": model_data.get("best_model", "N/A"),
                "metrics_json": json.dumps(model_data, default=str),
                "agg_type": "model_performance",
            })
    else:
        logger.info("    Không có kết quả ML để xuất. Tạo placeholder...")
        perf_rows = [{
            "model_name": "N/A",
            "best_model": "N/A",
            "metrics_json": "{}",
            "agg_type": "model_performance",
        }]

    # Ghi thẳng bằng pymongo - dữ liệu đã là Python list/dict nhỏ
    # (vài dòng), không cần đi qua Spark DataFrame cho phần này.
    _write_records_to_mongodb(perf_rows, "aggregations", mode="append")
    total_exported += len(perf_rows)

    # ---- 5g: Segment Stats ----
    logger.info("  5g. Tổng hợp thống kê phân khúc khách hàng...")
    if data["customer_segments"] is not None:
        segment_stats = (
            data["customer_segments"]
            .groupBy("segment_name")
            .agg(
                F.count("*").alias("customer_count"),
                F.avg("recency").alias("avg_recency"),
                F.avg("frequency").alias("avg_frequency"),
                F.avg("monetary").alias("avg_monetary"),
            )
            .withColumn("agg_type", F.lit("segment_stats"))
            .orderBy(F.col("avg_monetary").desc())
        )
        write_to_mongodb(segment_stats, "aggregations", mode="append")
        total_exported += segment_stats.count()
    else:
        logger.info("    Không có dữ liệu phân khúc. Bỏ qua.")

    logger.info(f"  -> Tổng số documents aggregations: {total_exported:,}")
    return total_exported


# ============================================================================
# MONGODB SCHEMA VALIDATORS (Star Schema Enforcement)
# ============================================================================

def setup_mongodb_schema_validators():
    """
    Tao JSON Schema validators cho cac collection MongoDB.
    Schema pattern: Denormalized Star Schema
      - orders:      Fact table (embedded customer, items, payment, review)
      - customers:   Dimension table (RFM + segment + churn)
      - products:    Dimension table (product stats)
      - sellers:     Dimension table (seller stats)
      - aggregations: Pre-computed Gold layer (no strict schema)
    """
    from pymongo import MongoClient
    client = MongoClient(MONGO_URI_BASE)
    db = client[MONGO_DATABASE]

    validators = {
        "orders": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["order_id", "order_status"],
                "properties": {
                    "order_id": {
                        "bsonType": "string",
                        "description": "Primary key - unique order identifier"
                    },
                    "order_status": {
                        "bsonType": "string",
                        "description": "Order status (delivered, shipped, etc.)"
                    },
                    "customer": {
                        "bsonType": "object",
                        "description": "Embedded customer dimension",
                        "properties": {
                            "customer_id": {"bsonType": "string"},
                            "customer_unique_id": {"bsonType": "string"},
                            "customer_city": {"bsonType": ["string", "null"]},
                            "customer_state": {"bsonType": ["string", "null"]}
                        }
                    },
                    "items": {
                        "bsonType": "object",
                        "description": "Embedded order items"
                    },
                    "payment": {
                        "bsonType": "object",
                        "description": "Embedded payment info"
                    },
                    "review": {
                        "bsonType": ["object", "null"],
                        "description": "Embedded review data"
                    },
                    "order_value": {
                        "bsonType": ["double", "int", "long", "null"],
                        "description": "Total order value (measure)"
                    },
                    "delivery_days": {
                        "bsonType": ["double", "int", "long", "null"],
                        "description": "Days to deliver (measure)"
                    }
                }
            }
        },
        "customers": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["customer_unique_id"],
                "properties": {
                    "customer_unique_id": {
                        "bsonType": "string",
                        "description": "Primary key - unique customer ID"
                    },
                    "customer_state": {"bsonType": ["string", "null"]},
                    "customer_city": {"bsonType": ["string", "null"]},
                    "recency": {
                        "bsonType": ["double", "int", "long", "null"],
                        "description": "RFM - days since last purchase"
                    },
                    "frequency": {
                        "bsonType": ["double", "int", "long", "null"],
                        "description": "RFM - number of purchases"
                    },
                    "monetary": {
                        "bsonType": ["double", "int", "long", "null"],
                        "description": "RFM - total spend"
                    },
                    "segment_name": {
                        "bsonType": ["string", "null"],
                        "description": "Customer segment (Champions, Loyal, etc.)"
                    },
                    "churn_prediction": {
                        "bsonType": ["double", "int", "long", "null"],
                        "description": "ML churn prediction (0 or 1)"
                    },
                    "churn_probability": {
                        "bsonType": ["double", "int", "long", "null"],
                        "description": "ML churn probability [0,1]"
                    }
                }
            }
        },
        "products": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["product_id"],
                "properties": {
                    "product_id": {
                        "bsonType": "string",
                        "description": "Primary key"
                    },
                    "category_en": {"bsonType": ["string", "null"]},
                    "total_orders": {"bsonType": ["double", "int", "long", "null"]},
                    "total_revenue": {"bsonType": ["double", "int", "long", "null"]},
                    "avg_price": {"bsonType": ["double", "int", "long", "null"]},
                    "avg_review_score": {"bsonType": ["double", "int", "long", "null"]}
                }
            }
        },
        "sellers": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["seller_id"],
                "properties": {
                    "seller_id": {
                        "bsonType": "string",
                        "description": "Primary key"
                    },
                    "seller_city": {"bsonType": ["string", "null"]},
                    "seller_state": {"bsonType": ["string", "null"]},
                    "total_items_sold": {"bsonType": ["double", "int", "long", "null"]},
                    "total_revenue": {"bsonType": ["double", "int", "long", "null"]}
                }
            }
        },
    }

    logger.info("=" * 60)
    logger.info("SETUP MONGODB SCHEMA VALIDATORS")
    logger.info("=" * 60)

    for collection_name, validator in validators.items():
        try:
            # Thu collMod truoc (collection da ton tai)
            db.command("collMod", collection_name,
                       validator=validator,
                       validationLevel="moderate",
                       validationAction="warn")
            logger.info(f"  {collection_name}: Schema validator UPDATED")
        except Exception:
            try:
                # Collection chua ton tai -> tao moi voi validator
                db.create_collection(collection_name, validator=validator)
                logger.info(f"  {collection_name}: Schema validator CREATED")
            except Exception as e2:
                logger.warning(f"  {collection_name}: Could not set validator: {e2}")

    # aggregations: no strict schema (different agg_types have different fields)
    logger.info("  aggregations: No strict schema (varied agg_type rows)")
    logger.info("  Schema validators setup complete.")
    client.close()


# ============================================================================
# MAIN: Chay xuat du lieu sang MongoDB
# ============================================================================


def run_export(ml_results=None):
    """
    Chạy toàn bộ quá trình xuất dữ liệu sang MongoDB.

    Tham số:
        ml_results: dict chứa kết quả ML (từ ml_models.py), có thể None
    """
    start_time = datetime.now()
    logger.info("*" * 60)
    logger.info("BAT DAU XUAT DU LIEU SANG MONGODB")
    logger.info(f"Thoi gian: {start_time}")
    logger.info(f"MongoDB URI: {MONGO_URI}")
    logger.info("*" * 60)

    try:
        # Setup MongoDB Schema Validators
        setup_mongodb_schema_validators()

        # Khoi tao Spark
        spark = create_spark_session()

        # Đọc dữ liệu
        data = load_all_data(spark)

        # Cache dữ liệu chính
        data["merged_orders"].cache()
        logger.info("Đã cache merged_orders.")

        # Xuất từng collection
        export_summary = {}

        # Collection 1: Orders
        export_summary["orders"] = export_orders(data)

        # Collection 2: Customers
        export_summary["customers"] = export_customers(data)

        # Collection 3: Products
        export_summary["products"] = export_products(data)

        # Collection 4: Sellers
        export_summary["sellers"] = export_sellers(data)

        # Collection 5: Aggregations
        export_summary["aggregations"] = export_aggregations(
            data, spark, ml_results
        )

        # Tóm tắt
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("\n" + "*" * 60)
        logger.info("XUẤT DỮ LIỆU HOÀN THÀNH!")
        logger.info(f"  Thời gian chạy: {duration:.1f} giây")
        logger.info(f"  MongoDB: {MONGO_URI}")
        logger.info("  Tóm tắt:")
        for collection, count in export_summary.items():
            logger.info(f"    {collection}: {count:,} documents")
        logger.info("*" * 60)

        # Giải phóng cache
        data["merged_orders"].unpersist()

        spark.stop()
        return export_summary

    except Exception as e:
        logger.error(f"LỖI EXPORT: {e}")
        import traceback
        traceback.print_exc()
        raise


def _load_ml_results_from_file():
    """
    Đọc kết quả ML thật từ file JSON (do ml_models_simple.py lưu lại
    sau khi train xong), để export_to_mongo.py có dữ liệu Model
    Performance thật khi chạy ĐỘC LẬP - thay vì luôn là placeholder
    rỗng (model_name="N/A") như trước đây.
    """
    results_json_path = os.path.join("tmp_models", "ml_results.json")
    if os.path.exists(results_json_path):
        try:
            with open(results_json_path, "r", encoding="utf-8") as f:
                results = json.load(f)
            logger.info(f"Đã đọc kết quả ML thật từ: {results_json_path}")
            return results
        except Exception as e:
            logger.warning(f"Không đọc được {results_json_path}: {e}. Dùng placeholder.")
            return None
    else:
        logger.warning(
            f"Không tìm thấy {results_json_path} - hãy chạy "
            "ml_models_simple.py trước để có kết quả ML thật. "
            "Dùng placeholder cho Model Performance."
        )
        return None


if __name__ == "__main__":
    # Tự động đọc kết quả ML thật từ file JSON (nếu đã chạy
    # ml_models_simple.py trước đó). Nếu chưa có, dùng placeholder.
    ml_results = _load_ml_results_from_file()
    summary = run_export(ml_results=ml_results)
    logger.info("Hoàn tất xuất dữ liệu sang MongoDB.")
