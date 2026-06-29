"""
=============================================================================
Exploratory Data Analysis (EDA) - Olist E-Commerce Big Data Project
=============================================================================
Mô tả: Phân tích khám phá dữ liệu Olist:
- Thống kê tổng quan
- Phân bố đơn hàng theo trạng thái, tháng, bang
- Phân tích review score, danh mục sản phẩm, phương thức thanh toán
- Phân tích thời gian giao hàng
- Lưu kết quả EDA dưới dạng JSON
=============================================================================
"""

import logging
import sys
import os
import json
import subprocess
import pandas as pd
from datetime import datetime


PYTHON_PATH = "C:/Users/Admin/AppData/Local/Programs/Python/Python312/python.exe"
os.environ["PYSPARK_PYTHON"] = PYTHON_PATH
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_PATH
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["TZ"] = "UTC"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================================
# Cấu hình logging
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("OlistEDA")

# ============================================================================
# Đường dẫn HDFS
# ============================================================================
HDFS_PROCESSED_PATH = "hdfs://localhost:9000/user/bigdata/olist/processed"
HDFS_EDA_PATH = "hdfs://localhost:9000/user/bigdata/olist/eda_results"
HDFS_EDA_PATH_CLI = "/user/bigdata/olist/eda_results"
HDFS_CLI_CMD = ["C:/hadoop/bin/hadoop.cmd", "dfs"]
LOCAL_EDA_DIR = "tmp_eda"


def _safe_toPandas(spark_df):
    """
    Convert Spark DataFrame sang pandas an toàn - ép timestamp thành
    string trước để tránh lỗi pytz UnknownTimeZoneError trên Windows.
    """
    from pyspark.sql.types import TimestampType, DateType
    ts_cols = [
        f.name for f in spark_df.schema.fields
        if isinstance(f.dataType, (TimestampType, DateType))
    ]
    df = spark_df
    for c in ts_cols:
        from pyspark.sql import functions as F
        df = df.withColumn(c, F.col(c).cast("string"))
    return df.toPandas()


def show_df(spark_df, label="", n=30):
    """In DataFrame qua pandas để tránh lỗi socket từ .show()."""
    if label:
        logger.info(f"  {label}")
    pdf = _safe_toPandas(spark_df.limit(n))
    print(pdf.to_string(index=False))


def create_spark_session():
    """Tạo SparkSession cho EDA."""
    logger.info("Đang khởi tạo SparkSession cho EDA...")
    spark = (
        SparkSession.builder
        .appName("Olist_EDA")
        .master("local[4]")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
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
    return spark


def load_processed_data(spark):
    """Đọc dữ liệu đã xử lý từ HDFS."""
    logger.info("Đang đọc dữ liệu merged_orders từ HDFS...")
    merged_df = spark.read.parquet(f"{HDFS_PROCESSED_PATH}/merged_orders")
    logger.info(f"  -> Đã đọc: {merged_df.count():,} dòng, {len(merged_df.columns)} cột")

    logger.info("Đang đọc dữ liệu rfm_customers từ HDFS...")
    rfm_df = spark.read.parquet(f"{HDFS_PROCESSED_PATH}/rfm_customers")
    logger.info(f"  -> Đã đọc: {rfm_df.count():,} dòng")

    return merged_df, rfm_df


# ============================================================================
# 1. THỐNG KÊ TỔNG QUAN
# ============================================================================

def summary_statistics(df):
    """
    In thống kê tổng quan cho các cột số quan trọng.
    """
    logger.info("=" * 60)
    logger.info("1. THỐNG KÊ TỔNG QUAN")
    logger.info("=" * 60)

    # Các cột số cần thống kê
    numeric_cols = [
        "total_price", "total_freight_value", "total_payment_value",
        "total_items", "delivery_days", "review_score",
        "freight_ratio", "order_value"
    ]

    # Lọc chỉ các cột tồn tại trong DataFrame
    existing_cols = [c for c in numeric_cols if c in df.columns]

    logger.info("  Thống kê mô tả (describe):")
    show_df(df.select(existing_cols).describe(), "Thống kê mô tả:")

    # Tính thêm percentile
    logger.info("  Phân vị (percentiles):")
    percentile_result = df.select(
        *[
            F.percentile_approx(c, [0.25, 0.5, 0.75]).alias(c)
            for c in existing_cols
        ]
    )
    show_df(percentile_result, "Phân vị:")

    # Tổng quan dataset
    total_orders = df.count()
    total_customers = df.select("customer_unique_id").distinct().count()
    total_revenue = df.agg(F.sum("total_payment_value")).collect()[0][0]
    avg_order_value = df.agg(F.avg("order_value")).collect()[0][0]
    date_range_min = df.agg(F.min("order_purchase_timestamp")).collect()[0][0]
    date_range_max = df.agg(F.max("order_purchase_timestamp")).collect()[0][0]

    summary = {
        "total_orders": total_orders,
        "total_customers": total_customers,
        "total_revenue": round(float(total_revenue), 2) if total_revenue else 0,
        "avg_order_value": round(float(avg_order_value), 2) if avg_order_value else 0,
        "date_range_start": str(date_range_min),
        "date_range_end": str(date_range_max),
    }

    logger.info("  Tổng quan:")
    for key, value in summary.items():
        logger.info(f"    {key}: {value}")

    return summary


# ============================================================================
# 2. PHÂN BỐ ĐƠN HÀNG THEO TRẠNG THÁI
# ============================================================================

def orders_by_status(df):
    """Đếm đơn hàng theo trạng thái (delivered, shipped, canceled, ...)."""
    logger.info("=" * 60)
    logger.info("2. PHÂN BỐ ĐƠN HÀNG THEO TRẠNG THÁI")
    logger.info("=" * 60)

    status_df = (
        df.groupBy("order_status")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("total_payment_value").alias("total_revenue"),
            F.avg("review_score").alias("avg_review_score"),
        )
        .withColumn(
            "percentage",
            F.round(F.col("order_count") / df.count() * 100, 2)
        )
        .orderBy(F.col("order_count").desc())
    )

    logger.info("  Phân bố đơn hàng theo trạng thái:")
    show_df(status_df, "Phân bố trạng thái:")

    # Chuyển sang dict để lưu JSON
    result = [row.asDict() for row in status_df.collect()]
    # Chuyển đổi các giá trị số thành kiểu Python thuần
    for r in result:
        for k, v in r.items():
            if v is not None and not isinstance(v, str):
                r[k] = float(v)
    return result


# ============================================================================
# 3. PHÂN BỐ ĐƠN HÀNG THEO THÁNG
# ============================================================================

def orders_by_month(df):
    """Đếm đơn hàng và doanh thu theo tháng."""
    logger.info("=" * 60)
    logger.info("3. PHÂN BỐ ĐƠN HÀNG THEO THÁNG")
    logger.info("=" * 60)

    monthly_df = (
        df.groupBy("purchase_year_month")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("total_payment_value").alias("monthly_revenue"),
            F.countDistinct("customer_unique_id").alias("unique_customers"),
            F.avg("review_score").alias("avg_review_score"),
            F.avg("delivery_days").alias("avg_delivery_days"),
        )
        .orderBy("purchase_year_month")
    )

    # Tính tăng trưởng so với tháng trước (MoM growth)
    window_spec = Window.orderBy("purchase_year_month")
    monthly_df = monthly_df.withColumn(
        "prev_month_revenue",
        F.lag("monthly_revenue").over(window_spec)
    ).withColumn(
        "mom_growth_pct",
        F.when(
            F.col("prev_month_revenue").isNotNull() & (F.col("prev_month_revenue") > 0),
            F.round(
                (F.col("monthly_revenue") - F.col("prev_month_revenue"))
                / F.col("prev_month_revenue") * 100, 2
            )
        ).otherwise(F.lit(None))
    ).drop("prev_month_revenue")

    logger.info("  Doanh thu và đơn hàng theo tháng:")
    show_df(monthly_df, "Doanh thu theo tháng:", n=30)

    result = [row.asDict() for row in monthly_df.collect()]
    for r in result:
        for k, v in r.items():
            if v is not None and not isinstance(v, str):
                r[k] = float(v)
    return result


# ============================================================================
# 4. PHÂN BỐ ĐƠN HÀNG THEO BANG
# ============================================================================

def orders_by_state(df):
    """Đếm đơn hàng và doanh thu theo bang (state)."""
    logger.info("=" * 60)
    logger.info("4. PHÂN BỐ ĐƠN HÀNG THEO BANG")
    logger.info("=" * 60)

    state_df = (
        df.groupBy("customer_state")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("total_payment_value").alias("total_revenue"),
            F.countDistinct("customer_unique_id").alias("unique_customers"),
            F.avg("review_score").alias("avg_review_score"),
            F.avg("delivery_days").alias("avg_delivery_days"),
            F.avg("order_value").alias("avg_order_value"),
        )
        .withColumn(
            "percentage",
            F.round(F.col("order_count") / df.count() * 100, 2)
        )
        .orderBy(F.col("order_count").desc())
    )

    logger.info("  Phân bố đơn hàng theo bang:")
    show_df(state_df, "Đơn hàng theo bang:", n=30)

    result = [row.asDict() for row in state_df.collect()]
    for r in result:
        for k, v in r.items():
            if v is not None and not isinstance(v, str):
                r[k] = float(v)
    return result


# ============================================================================
# 5. PHÂN BỐ ĐIỂM ĐÁNH GIÁ (REVIEW SCORE)
# ============================================================================

def review_score_distribution(df):
    """Phân tích phân bố điểm đánh giá (1-5 sao)."""
    logger.info("=" * 60)
    logger.info("5. PHÂN BỐ ĐIỂM ĐÁNH GIÁ")
    logger.info("=" * 60)

    # Chỉ phân tích các đơn có review
    reviewed_df = df.filter(F.col("review_score") > 0)

    review_dist = (
        reviewed_df
        .groupBy(F.col("review_score").cast("int").alias("review_score"))
        .agg(
            F.count("*").alias("count"),
            F.avg("delivery_days").alias("avg_delivery_days"),
            F.avg("order_value").alias("avg_order_value"),
            F.avg("freight_ratio").alias("avg_freight_ratio"),
        )
        .withColumn(
            "percentage",
            F.round(F.col("count") / reviewed_df.count() * 100, 2)
        )
        .orderBy("review_score")
    )

    logger.info("  Phân bố điểm đánh giá:")
    show_df(review_dist, "Phân bố review score:")

    # Tính điểm trung bình tổng
    avg_score = reviewed_df.agg(F.avg("review_score")).collect()[0][0]
    logger.info(f"  -> Điểm đánh giá trung bình: {avg_score:.2f}")

    # Phân tích theo delivery_status
    logger.info("  Điểm đánh giá theo tình trạng giao hàng:")
    review_by_delivery = (
        reviewed_df
        .groupBy("delivery_status")
        .agg(
            F.count("*").alias("count"),
            F.avg("review_score").alias("avg_review_score"),
            F.avg("delivery_days").alias("avg_delivery_days"),
        )
        .orderBy(F.col("avg_review_score").desc())
    )
    show_df(review_by_delivery, "Review theo ngày giao:")

    result = {
        "distribution": [row.asDict() for row in review_dist.collect()],
        "avg_score": round(float(avg_score), 2),
        "by_delivery_status": [row.asDict() for row in review_by_delivery.collect()],
    }
    # Chuyển đổi kiểu dữ liệu
    for section in ["distribution", "by_delivery_status"]:
        for r in result[section]:
            for k, v in r.items():
                if v is not None and not isinstance(v, str):
                    r[k] = float(v)
    return result


# ============================================================================
# 6. TOP 10 DANH MỤC SẢN PHẨM THEO DOANH THU
# ============================================================================

def top_categories_by_revenue(df):
    """Tìm top 10 danh mục sản phẩm theo doanh thu."""
    logger.info("=" * 60)
    logger.info("6. TOP 10 DANH MỤC SẢN PHẨM THEO DOANH THU")
    logger.info("=" * 60)

    category_df = (
        df.filter(F.col("main_category_english") != "unknown")
        .groupBy("main_category_english")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("total_payment_value").alias("total_revenue"),
            F.avg("total_payment_value").alias("avg_order_value"),
            F.avg("review_score").alias("avg_review_score"),
            F.avg("delivery_days").alias("avg_delivery_days"),
            F.sum("total_items").alias("total_items_sold"),
            F.countDistinct("customer_unique_id").alias("unique_customers"),
        )
        .orderBy(F.col("total_revenue").desc())
    )

    # Top 10
    top10 = category_df.limit(10)
    logger.info("  Top 10 danh mục sản phẩm theo doanh thu:")
    show_df(top10, "Top 10 danh mục:")

    # Bottom 10 (danh mục doanh thu thấp nhất)
    bottom10 = category_df.orderBy(F.col("total_revenue").asc()).limit(10)
    logger.info("  Bottom 10 danh mục sản phẩm theo doanh thu:")
    show_df(bottom10, "Bottom 10 danh mục:")

    # Tổng số danh mục
    total_categories = category_df.count()
    logger.info(f"  -> Tổng số danh mục: {total_categories}")

    result = {
        "top10": [row.asDict() for row in top10.collect()],
        "bottom10": [row.asDict() for row in bottom10.collect()],
        "total_categories": total_categories,
    }
    for section in ["top10", "bottom10"]:
        for r in result[section]:
            for k, v in r.items():
                if v is not None and not isinstance(v, str):
                    r[k] = float(v)
    return result


# ============================================================================
# 7. PHÂN BỐ PHƯƠNG THỨC THANH TOÁN
# ============================================================================

def payment_method_distribution(df):
    """Phân tích phân bố phương thức thanh toán."""
    logger.info("=" * 60)
    logger.info("7. PHÂN BỐ PHƯƠNG THỨC THANH TOÁN")
    logger.info("=" * 60)

    payment_df = (
        df.groupBy("payment_type")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("total_payment_value").alias("total_revenue"),
            F.avg("total_payment_value").alias("avg_payment_value"),
            F.avg("max_installments").alias("avg_installments"),
            F.avg("review_score").alias("avg_review_score"),
        )
        .withColumn(
            "percentage",
            F.round(F.col("order_count") / df.count() * 100, 2)
        )
        .orderBy(F.col("order_count").desc())
    )

    logger.info("  Phân bố phương thức thanh toán:")
    show_df(payment_df, "Phương thức thanh toán:")

    # Phân tích trả góp (installments)
    logger.info("  Phân bố số kỳ trả góp:")
    installment_df = (
        df.filter(F.col("payment_type") == "credit_card")
        .groupBy("max_installments")
        .agg(
            F.count("*").alias("order_count"),
            F.avg("total_payment_value").alias("avg_payment_value"),
        )
        .orderBy("max_installments")
    )
    show_df(installment_df, "Phân bố trả góp:", n=20)

    result = {
        "payment_methods": [row.asDict() for row in payment_df.collect()],
        "installments": [row.asDict() for row in installment_df.collect()],
    }
    for section in ["payment_methods", "installments"]:
        for r in result[section]:
            for k, v in r.items():
                if v is not None and not isinstance(v, str):
                    r[k] = float(v)
    return result


# ============================================================================
# 8. PHÂN TÍCH THỜI GIAN GIAO HÀNG
# ============================================================================

def delivery_time_analysis(df):
    """Phân tích chi tiết thời gian giao hàng."""
    logger.info("=" * 60)
    logger.info("8. PHÂN TÍCH THỜI GIAN GIAO HÀNG")
    logger.info("=" * 60)

    # Chỉ phân tích đơn đã giao
    delivered_df = df.filter(
        (F.col("order_status") == "delivered") &
        (F.col("delivery_days").isNotNull()) &
        (F.col("delivery_days") >= 0)
    )

    # Thống kê tổng quan giao hàng
    delivery_stats = delivered_df.agg(
        F.avg("delivery_days").alias("avg_delivery_days"),
        F.stddev("delivery_days").alias("std_delivery_days"),
        F.min("delivery_days").alias("min_delivery_days"),
        F.max("delivery_days").alias("max_delivery_days"),
        F.expr("percentile_approx(delivery_days, 0.5)").alias("median_delivery_days"),
        F.expr("percentile_approx(delivery_days, 0.95)").alias("p95_delivery_days"),
    ).collect()[0]

    logger.info("  Thống kê giao hàng:")
    logger.info(f"    Trung bình: {delivery_stats['avg_delivery_days']:.1f} ngày")
    logger.info(f"    Độ lệch chuẩn: {delivery_stats['std_delivery_days']:.1f} ngày")
    logger.info(f"    Trung vị: {delivery_stats['median_delivery_days']} ngày")
    logger.info(f"    P95: {delivery_stats['p95_delivery_days']} ngày")
    logger.info(f"    Min-Max: {delivery_stats['min_delivery_days']}-{delivery_stats['max_delivery_days']} ngày")

    # Phân bố thời gian giao hàng (theo khoảng)
    delivery_buckets = (
        delivered_df
        .withColumn(
            "delivery_bucket",
            F.when(F.col("delivery_days") <= 7, "0-7 ngày")
             .when(F.col("delivery_days") <= 14, "8-14 ngày")
             .when(F.col("delivery_days") <= 21, "15-21 ngày")
             .when(F.col("delivery_days") <= 30, "22-30 ngày")
             .otherwise("Trên 30 ngày")
        )
        .groupBy("delivery_bucket")
        .agg(
            F.count("*").alias("order_count"),
            F.avg("review_score").alias("avg_review_score"),
        )
        .withColumn(
            "percentage",
            F.round(F.col("order_count") / delivered_df.count() * 100, 2)
        )
        .orderBy("delivery_bucket")
    )

    logger.info("  Phân bố thời gian giao hàng:")
    show_df(delivery_buckets, "Phân loại giao hàng:")

    # Giao hàng theo bang
    logger.info("  Thời gian giao hàng theo bang (top 10 chậm nhất):")
    delivery_by_state = (
        delivered_df
        .groupBy("customer_state")
        .agg(
            F.count("*").alias("order_count"),
            F.avg("delivery_days").alias("avg_delivery_days"),
            F.expr("percentile_approx(delivery_days, 0.5)").alias("median_delivery_days"),
            F.avg("estimated_vs_actual").alias("avg_estimated_vs_actual"),
        )
        .orderBy(F.col("avg_delivery_days").desc())
    )
    show_df(delivery_by_state, "Giao hàng theo bang:", n=10)

    # Tỷ lệ giao đúng hạn
    on_time_count = delivered_df.filter(F.col("estimated_vs_actual") >= 0).count()
    total_delivered = delivered_df.count()
    on_time_rate = (on_time_count / total_delivered * 100) if total_delivered > 0 else 0
    logger.info(f"  -> Tỷ lệ giao đúng hạn: {on_time_rate:.1f}%")

    result = {
        "overall_stats": {
            "avg_delivery_days": round(float(delivery_stats["avg_delivery_days"]), 2),
            "std_delivery_days": round(float(delivery_stats["std_delivery_days"]), 2),
            "median_delivery_days": int(delivery_stats["median_delivery_days"]),
            "p95_delivery_days": int(delivery_stats["p95_delivery_days"]),
            "min_delivery_days": int(delivery_stats["min_delivery_days"]),
            "max_delivery_days": int(delivery_stats["max_delivery_days"]),
            "on_time_rate_pct": round(on_time_rate, 2),
        },
        "delivery_buckets": [row.asDict() for row in delivery_buckets.collect()],
        "by_state": [row.asDict() for row in delivery_by_state.collect()],
    }
    for section in ["delivery_buckets", "by_state"]:
        for r in result[section]:
            for k, v in r.items():
                if v is not None and not isinstance(v, str):
                    r[k] = float(v)
    return result


# ============================================================================
# 9. PHÂN TÍCH THEO GIỜ VÀ NGÀY TRONG TUẦN
# ============================================================================

def hourly_and_weekday_analysis(df):
    """Phân tích xu hướng mua hàng theo giờ và ngày trong tuần."""
    logger.info("=" * 60)
    logger.info("9. PHÂN TÍCH THEO GIỜ VÀ NGÀY TRONG TUẦN")
    logger.info("=" * 60)

    # Đơn hàng theo giờ
    hourly_df = (
        df.groupBy("purchase_hour")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("total_payment_value").alias("total_revenue"),
        )
        .orderBy("purchase_hour")
    )
    logger.info("  Đơn hàng theo giờ:")
    show_df(hourly_df, "Đơn theo giờ:", n=24)

    # Đơn hàng theo ngày trong tuần (1=CN, 2=T2, ..., 7=T7)
    weekday_names = {1: "CN", 2: "T2", 3: "T3", 4: "T4", 5: "T5", 6: "T6", 7: "T7"}
    weekday_df = (
        df.groupBy("purchase_dayofweek")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("total_payment_value").alias("total_revenue"),
        )
        .orderBy("purchase_dayofweek")
    )
    logger.info("  Đơn hàng theo ngày trong tuần:")
    show_df(weekday_df, "Đơn theo ngày tuần:")

    # Ma trận giờ × ngày (heatmap data)
    heatmap_df = (
        df.groupBy("purchase_dayofweek", "purchase_hour")
        .agg(F.count("*").alias("order_count"))
        .orderBy("purchase_dayofweek", "purchase_hour")
    )

    result = {
        "hourly": [row.asDict() for row in hourly_df.collect()],
        "weekday": [row.asDict() for row in weekday_df.collect()],
        "heatmap": [row.asDict() for row in heatmap_df.collect()],
    }
    for section in ["hourly", "weekday", "heatmap"]:
        for r in result[section]:
            for k, v in r.items():
                if v is not None and not isinstance(v, str):
                    r[k] = float(v)
    return result


# ============================================================================
# 10. PHÂN TÍCH RFM
# ============================================================================

def rfm_analysis(rfm_df):
    """Phân tích phân bố RFM."""
    logger.info("=" * 60)
    logger.info("10. PHÂN TÍCH RFM")
    logger.info("=" * 60)

    # Thống kê tổng quan RFM
    logger.info("  Thống kê RFM:")
    show_df(rfm_df.select("recency", "frequency", "monetary", "rfm_score").describe(), "Thống kê RFM:")

    # Phân bố frequency
    freq_dist = (
        rfm_df.groupBy("frequency")
        .agg(F.count("*").alias("customer_count"))
        .orderBy("frequency")
    )
    logger.info("  Phân bố tần suất mua hàng:")
    show_df(freq_dist, "Phân bố frequency:", n=20)

    # Phân bố rfm_score
    rfm_score_dist = (
        rfm_df.groupBy("rfm_score")
        .agg(
            F.count("*").alias("customer_count"),
            F.avg("monetary").alias("avg_monetary"),
        )
        .orderBy("rfm_score")
    )
    logger.info("  Phân bố RFM score:")
    show_df(rfm_score_dist, "Phân bố RFM score:", n=20)

    # Khách hàng mua lại (repeat customers)
    total_customers = rfm_df.count()
    repeat_customers = rfm_df.filter(F.col("frequency") > 1).count()
    repeat_rate = (repeat_customers / total_customers * 100) if total_customers > 0 else 0
    logger.info(f"  -> Tỷ lệ mua lại: {repeat_rate:.1f}% ({repeat_customers:,}/{total_customers:,})")

    result = {
        "frequency_distribution": [row.asDict() for row in freq_dist.collect()],
        "rfm_score_distribution": [row.asDict() for row in rfm_score_dist.collect()],
        "repeat_customer_rate_pct": round(repeat_rate, 2),
        "total_customers": total_customers,
        "repeat_customers": repeat_customers,
    }
    for section in ["frequency_distribution", "rfm_score_distribution"]:
        for r in result[section]:
            for k, v in r.items():
                if v is not None and not isinstance(v, str):
                    r[k] = float(v)
    return result


# ============================================================================
# LƯU KẾT QUẢ EDA RA JSON TRÊN HDFS
# ============================================================================

def save_eda_results(spark, all_results):
    """
    Lưu tất cả kết quả EDA dưới dạng JSON lên HDFS.

    Dùng cách ghi file local rồi hadoop.cmd dfs -put - KHÔNG dùng
    spark.createDataFrame() vì cách đó tạo socket JVM<->Python
    dễ gãy trên máy này (đã xác nhận từ các file trước).
    """
    import subprocess
    logger.info("=" * 60)
    logger.info("LƯU KẾT QUẢ EDA")
    logger.info("=" * 60)

    os.makedirs(LOCAL_EDA_DIR, exist_ok=True)

    def hdfs_put(local_path, hdfs_path):
        """Đẩy file local lên HDFS bằng hadoop.cmd dfs -put."""
        clean_env = os.environ.copy()
        clean_env.pop("TZ", None)
        local_abs = os.path.abspath(local_path).replace("\\", "/")

        subprocess.run(
            HDFS_CLI_CMD + ["-rm", "-r", "-f", hdfs_path],
            capture_output=True, text=True, env=clean_env
        )
        subprocess.run(
            HDFS_CLI_CMD + ["-mkdir", "-p", hdfs_path.rsplit("/", 1)[0]],
            capture_output=True, text=True, env=clean_env
        )
        result = subprocess.run(
            HDFS_CLI_CMD + ["-put", "-f", local_abs, hdfs_path],
            capture_output=True, text=True, timeout=120, env=clean_env
        )
        if result.returncode != 0:
            logger.warning(f"  Cảnh báo khi đẩy {hdfs_path}: {result.stderr[:200]}")
        else:
            logger.info(f"  -> Đã lưu: {hdfs_path}")

    # Lưu toàn bộ kết quả vào 1 file JSON
    json_str = json.dumps(all_results, ensure_ascii=False, indent=2, default=str)
    local_full = os.path.join(LOCAL_EDA_DIR, "eda_results.json")
    with open(local_full, "w", encoding="utf-8") as f:
        f.write(json_str)
    hdfs_put(local_full, f"{HDFS_EDA_PATH_CLI}/eda_results.json")

    # Lưu từng phần riêng biệt
    for key, value in all_results.items():
        if key == "summary":
            continue
        try:
            section_str = json.dumps(value, ensure_ascii=False, default=str)
            local_section = os.path.join(LOCAL_EDA_DIR, f"{key}.json")
            with open(local_section, "w", encoding="utf-8") as f:
                f.write(section_str)
            hdfs_put(local_section, f"{HDFS_EDA_PATH_CLI}/{key}.json")
        except Exception as e:
            logger.warning(f"  Không thể lưu {key}: {e}")

    logger.info("  Tất cả kết quả EDA đã được lưu thành công!")


# ============================================================================
# MAIN: Chạy toàn bộ EDA
# ============================================================================

def run_eda():
    """
    Chạy toàn bộ phân tích EDA.
    Trả về: dictionary chứa tất cả kết quả.
    """
    start_time = datetime.now()
    logger.info("*" * 60)
    logger.info("BẮT ĐẦU PHÂN TÍCH EDA - Olist E-Commerce")
    logger.info(f"Thời gian: {start_time}")
    logger.info("*" * 60)

    try:
        # Khởi tạo Spark
        spark = create_spark_session()

        # Đọc dữ liệu
        merged_df, rfm_df = load_processed_data(spark)

        # Cache dữ liệu để tăng tốc các phân tích
        merged_df.cache()
        rfm_df.cache()
        logger.info("Đã cache dữ liệu vào bộ nhớ.")

        # Chạy tất cả phân tích
        all_results = {}

        all_results["summary"] = summary_statistics(merged_df)
        all_results["orders_by_status"] = orders_by_status(merged_df)
        all_results["orders_by_month"] = orders_by_month(merged_df)
        all_results["orders_by_state"] = orders_by_state(merged_df)
        all_results["review_scores"] = review_score_distribution(merged_df)
        all_results["top_categories"] = top_categories_by_revenue(merged_df)
        all_results["payment_methods"] = payment_method_distribution(merged_df)
        all_results["delivery_analysis"] = delivery_time_analysis(merged_df)
        all_results["hourly_weekday"] = hourly_and_weekday_analysis(merged_df)
        all_results["rfm_analysis"] = rfm_analysis(rfm_df)

        # Lưu kết quả
        save_eda_results(spark, all_results)

        # Tóm tắt
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info("*" * 60)
        logger.info("EDA HOÀN THÀNH!")
        logger.info(f"  Thời gian chạy: {duration:.1f} giây")
        logger.info(f"  Số phân tích: {len(all_results)}")
        logger.info("*" * 60)

        # Giải phóng cache
        merged_df.unpersist()
        rfm_df.unpersist()

        spark.stop()
        return all_results

    except Exception as e:
        logger.error(f"LỖI EDA: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    results = run_eda()
    logger.info("Hoàn tất EDA. Kết quả đã được lưu trên HDFS.")
