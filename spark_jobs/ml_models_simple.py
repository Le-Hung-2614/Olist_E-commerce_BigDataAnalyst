"""
=============================================================================
Machine Learning Models - Olist E-Commerce Big Data Project (BẢN ĐƠN GIẢN)
=============================================================================
Mô tả: Huấn luyện 3 mô hình ML bằng scikit-learn:
  1. K-Means Customer Segmentation (phân cụm khách hàng theo RFM)
  2. Churn Prediction (dự đoán khách hàng rời bỏ - phân loại)
  3. Review Score Prediction (dự đoán điểm đánh giá - hồi quy)

KHÁC BIỆT SO VỚI BẢN PYSPARK MLLIB:
  - Spark CHỈ dùng để đọc/ghi dữ liệu từ HDFS (không train model bằng Spark).
  - Toàn bộ việc train model dùng scikit-learn, chạy trong MỘT process
    Python duy nhất - không tạo Python worker con, không có giao tiếp
    qua socket nội bộ JVM<->Python khi train -> tránh hẳn lỗi
    "Python worker exited unexpectedly" / WinError 10038 từng gặp với
    PySpark MLlib.
  - Với dữ liệu cỡ chục nghìn đến trăm nghìn dòng (như RFM/merged_orders
    ở đây), scikit-learn xử lý nhanh và ổn định hơn nhiều so với chạy
    Spark phân tán giả lập trên 1 máy local 8GB RAM.
=============================================================================
"""

import logging
import sys
import os
import subprocess
import json
from datetime import datetime

import numpy as np
import pandas as pd

from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    silhouette_score,
    roc_auc_score, average_precision_score, f1_score, accuracy_score,
    precision_score, recall_score, confusion_matrix,
    mean_squared_error, r2_score, mean_absolute_error,
)
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import joblib

# Set PYSPARK_PYTHON bằng FULL PATH - chỉ dùng cho phần đọc/ghi HDFS.
PYTHON_PATH = "C:/Users/Admin/AppData/Local/Programs/Python/Python313/python.exe"
os.environ["PYSPARK_PYTHON"] = PYTHON_PATH
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_PATH
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Ép timezone JVM/Spark dùng UTC thay vì lấy theo múi giờ hệ thống Windows.
# Lý do lỗi: Windows trả múi giờ hệ thống dạng "GMT+07:00", nhưng thư viện
# pytz mà pandas dùng để parse timezone KHÔNG hiểu định dạng này (chỉ hiểu
# dạng như "Etc/GMT-7" hoặc "Asia/Ho_Chi_Minh") -> lỗi
# "UnknownTimeZoneError: 'GMT+07:00'" khi Spark cố convert timestamp sang
# pandas. Ép về UTC giúp tránh hoàn toàn bước parse timezone gây lỗi này.
os.environ["TZ"] = "UTC"

from pyspark.sql import SparkSession

# ============================================================================
# Cấu hình
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("OlistML")

HDFS_PROCESSED_PATH = "hdfs://localhost:9000/user/bigdata/olist/processed"
HDFS_MODELS_PATH = "hdfs://localhost:9000/user/bigdata/olist/models"
# Dùng riêng cho lệnh dfs qua subprocess - bỏ phần
# "hdfs://localhost:9000" vì command line CLI tự nối theo fs.defaultFS
# đã cấu hình sẵn trong core-site.xml, không cần ghi lại URI đầy đủ.
HDFS_MODELS_PATH_CLI = "/user/bigdata/olist/models"

# Lệnh CLI dùng để thao tác với HDFS. Trên máy này, "hdfs.cmd" bị lỗi
# nội bộ ("Could not find or load main class version") - khả năng do
# bản cài Hadoop từng được vá/ghi đè không hoàn chỉnh. "hadoop.cmd dfs"
# là cách gọi tương đương cũ hơn và đã xác nhận hoạt động bình thường,
# nên dùng nó thay thế cho toàn bộ thao tác dfs trong file này.
HDFS_CLI_CMD = ["C:/hadoop/bin/hadoop.cmd", "dfs"]

# Thư mục local tạm để lưu model trước khi đẩy lên HDFS
LOCAL_TMP_DIR = "tmp_models"


def create_spark_session():
    """
    Tạo SparkSession - CHỈ dùng để đọc/ghi dữ liệu từ HDFS.
    Không dùng để train model nên không cần cấu hình nặng cho MLlib.
    """
    logger.info("Đang khởi tạo SparkSession (chỉ để đọc/ghi HDFS)...")
    spark = (
        SparkSession.builder
        .appName("Olist_ML_DataIO")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        # Ép session timezone của Spark về UTC chuẩn (IANA). Lý do: trên
        # Windows, JVM có thể trả về timezone hệ thống dưới dạng chuỗi
        # "GMT+07:00" - pandas/pytz KHÔNG hiểu định dạng này (chỉ hiểu tên
        # chuẩn IANA như "UTC", "Asia/Ho_Chi_Minh"), gây
        # pytz.exceptions.UnknownTimeZoneError khi toPandas() convert cột
        # timestamp. Set "UTC" tránh hẳn việc JVM phải tự dò timezone hệ
        # thống và sinh ra chuỗi lạ đó.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _spark_df_to_pandas_safe(spark_df):
    """
    Chuyển Spark DataFrame sang pandas một cách an toàn.

    Lý do cần hàm riêng: PySpark.toPandas() mặc định cố tự động convert
    các cột timestamp sang đúng timezone bằng tz_localize() - bước này
    có thể lỗi (AmbiguousTimeError / OverflowError...) tùy theo dữ liệu
    và cấu hình múi giờ của JVM trên máy Windows. Cách né: ép các cột
    timestamp/date thành string TRƯỚC khi gọi toPandas(), rồi parse lại
    ngày giờ bằng pandas (ổn định, không đụng tới logic tz của Spark).
    """
    from pyspark.sql.types import TimestampType, DateType
    from pyspark.sql import functions as F

    ts_cols = [
        f.name for f in spark_df.schema.fields
        if isinstance(f.dataType, (TimestampType, DateType))
    ]

    df_to_convert = spark_df
    for c in ts_cols:
        df_to_convert = df_to_convert.withColumn(c, F.col(c).cast("string"))

    pdf = df_to_convert.toPandas()

    # Parse lại các cột ngày giờ bằng pandas sau khi đã chuyển an toàn
    for c in ts_cols:
        pdf[c] = pd.to_datetime(pdf[c], errors="coerce")

    return pdf


def load_data(spark):
    """
    Đọc dữ liệu từ HDFS bằng Spark, rồi chuyển ngay sang pandas DataFrame.
    Sau bước này, Spark không còn được dùng nữa cho tới lúc ghi kết quả.
    """
    logger.info("Đang đọc dữ liệu từ HDFS...")

    merged_spark_df = spark.read.parquet(f"{HDFS_PROCESSED_PATH}/merged_orders")
    rfm_spark_df = spark.read.parquet(f"{HDFS_PROCESSED_PATH}/rfm_customers")

    logger.info("  Đang chuyển sang pandas DataFrame...")
    merged_df = _spark_df_to_pandas_safe(merged_spark_df)
    rfm_df = _spark_df_to_pandas_safe(rfm_spark_df)

    logger.info(f"  merged_orders: {len(merged_df):,} dòng")
    logger.info(f"  rfm_customers: {len(rfm_df):,} dòng")

    return merged_df, rfm_df


def _run_hdfs_cmd(args, timeout=120):
    """
    Chạy một lệnh HDFS CLI (qua HDFS_CLI_CMD) một cách an toàn.

    QUAN TRỌNG: loại bỏ biến môi trường "TZ" khỏi tiến trình con trước
    khi gọi. Lý do phát hiện được: code Python này tự set
    os.environ["TZ"] = "UTC" (để tránh lỗi pytz khi PySpark convert
    timestamp). Khi subprocess.run() kế thừa toàn bộ env hiện tại
    (mặc định), biến TZ lạ này được truyền luôn vào hadoop.cmd -> JVM
    của Hadoop trên Windows không hiểu đúng, khiến lệnh "-put" thoát ra
    với returncode=0 (coi như thành công) nhưng KHÔNG thực sự ghi file
    - đây là lý do log từng báo "đẩy lên HDFS thành công" nhưng web UI
    lại không thấy file. Dùng môi trường con riêng, sạch sẽ, không có
    TZ, để tránh lặp lại vấn đề này.
    """
    clean_env = os.environ.copy()
    clean_env.pop("TZ", None)

    result = subprocess.run(
        args, capture_output=True, text=True, timeout=timeout, env=clean_env
    )
    if result.stdout.strip():
        logger.info(f"    [hdfs stdout] {result.stdout.strip()}")
    if result.stderr.strip():
        logger.info(f"    [hdfs stderr] {result.stderr.strip()}")
    return result


def _hdfs_path_exists(hdfs_path):
    """
    Kiểm tra một path có thực sự tồn tại trên HDFS.

    KHÔNG dùng "-test -e" + tin vào returncode nữa - đã phát hiện thực
    tế trên máy này "-test -e" có thể trả về returncode=0 (báo "tồn
    tại") ngay cả khi file KHÔNG có trên HDFS (false positive, giống
    kiểu lỗi script .cmd trả mã thoát sai từng gặp với biến TZ). Thay
    vào đó, dùng "-ls" và tự kiểm tra xem output có thực sự liệt kê
    được tên file/path đó hay không - đây là cách duy nhất xác nhận
    bằng dữ liệu thật thay vì tin vào exit code của script Windows.
    """
    result = _run_hdfs_cmd(HDFS_CLI_CMD + ["-ls", hdfs_path])
    if result.returncode != 0:
        return False
    # "-ls" của một path không tồn tại sẽ không in dòng nào chứa chính
    # path đó (hoặc in lỗi "No such file or directory" trong stderr).
    output = result.stdout
    if "No such file or directory" in (result.stdout + result.stderr):
        return False
    target_name = hdfs_path.rstrip("/").rsplit("/", 1)[-1]
    return target_name in output


def save_pandas_as_parquet(spark, pdf, hdfs_path):
    """
    Ghi một pandas DataFrame lên HDFS dạng parquet.

    LÀM TRỰC TIẾP BẰNG PANDAS + subprocess "hdfs dfs -put", KHÔNG dùng
    spark.createDataFrame(pdf) nữa. Lý do: convert một pandas DataFrame
    lớn (vài chục nghìn dòng) ngược trở lại Spark DataFrame tạo ra một
    task rất to phải serialize qua socket Python<->JVM một lần - đây
    chính là chỗ máy hay bị "WinError 10038 / SocketTimeoutException"
    khi ghi. Ghi file local bằng pandas rồi đẩy thẳng lên HDFS qua dòng
    lệnh "hdfs dfs -put" ổn định hơn nhiều vì không đụng tới Python
    worker của Spark.
    """
    import re

    # hdfs CLI chấp nhận cả URI đầy đủ lẫn path tương đối, nhưng để nhất
    # quán và tránh phụ thuộc vào việc CLI có parse đúng URI hay không,
    # bóc bỏ phần "hdfs://host:port" nếu có, chỉ giữ lại path tương đối.
    hdfs_path = re.sub(r"^hdfs://[^/]+", "", hdfs_path)

    os.makedirs(LOCAL_TMP_DIR, exist_ok=True)
    local_file = os.path.join(LOCAL_TMP_DIR, os.path.basename(hdfs_path.rstrip("/")) + ".parquet")

    logger.info(f"  Đang ghi file parquet local: {local_file}")
    pdf.to_parquet(local_file, index=False)

    # QUAN TRỌNG: dùng đường dẫn TUYỆT ĐỐI cho file local khi gọi
    # "hadoop.cmd ... -put". Đã xác nhận thực tế trên máy này: bản cài
    # Hadoop hiện tại KHÔNG tự resolve được đường dẫn tương đối (báo
    # "No such file or directory" dù file tồn tại đúng chỗ), nhưng
    # chạy đúng với đường dẫn tuyệt đối dùng forward-slash.
    local_file_abs = os.path.abspath(local_file).replace("\\", "/")

    logger.info(f"  Đang đẩy lên HDFS: {hdfs_path}")
    try:
        # Xóa path cũ trên HDFS nếu có (tương đương write.mode("overwrite"))
        _run_hdfs_cmd(HDFS_CLI_CMD + ["-rm", "-r", "-f", hdfs_path])
        # Tạo thư mục cha trên HDFS nếu chưa có
        hdfs_parent = hdfs_path.rsplit("/", 1)[0]
        _run_hdfs_cmd(HDFS_CLI_CMD + ["-mkdir", "-p", hdfs_parent])
        _run_hdfs_cmd(HDFS_CLI_CMD + ["-mkdir", "-p", hdfs_path])
        target = f"{hdfs_path}/part-00000.parquet"
        result = _run_hdfs_cmd(HDFS_CLI_CMD + ["-put", "-f", local_file_abs, target])

        # KHÔNG chỉ tin vào returncode - xác minh lại bằng -ls (parse
        # output thật), vì đã từng gặp trường hợp returncode=0 nhưng
        # file không thực sự được ghi (do path tương đối không được
        # hadoop.cmd resolve đúng, hoặc do biến môi trường TZ).
        if result.returncode != 0 or not _hdfs_path_exists(target):
            logger.error(f"  Lỗi khi đẩy lên HDFS (đã xác minh lại bằng -ls): {result.stderr}")
            raise RuntimeError(f"hadoop dfs -put thất bại hoặc file không tồn tại sau khi put: {target}")
        logger.info(f"  -> Đã đẩy lên HDFS thành công (đã xác minh): {hdfs_path}")
    except FileNotFoundError:
        logger.warning(
            "  Lệnh 'hdfs' không tìm thấy trong PATH - bỏ qua bước đẩy lên "
            f"HDFS. File parquet đã có sẵn ở local: {local_file}"
        )
    except subprocess.TimeoutExpired:
        logger.warning(
            "  Lệnh 'hdfs dfs -put' bị timeout - bỏ qua bước đẩy lên HDFS. "
            f"File parquet đã có sẵn ở local: {local_file}"
        )


def upload_model_to_hdfs(local_model_path, hdfs_model_dir):
    """
    Đẩy một file model (.joblib) đã lưu local lên HDFS.

    Dùng "hadoop dfs -put" qua _run_hdfs_cmd (env sạch, không có TZ) -
    giống cách làm với save_pandas_as_parquet ở trên - để tránh hoàn
    toàn việc phải đi qua Spark (không cần thiết và từng gây lỗi
    socket). Yêu cầu: giảng viên cần thấy model nằm trên HDFS, nên các
    file .joblib (vốn không phải định dạng Spark hiểu được) vẫn được
    đẩy thẳng lên HDFS dưới dạng file nhị phân thông thường - HDFS lưu
    được bất kỳ loại file nào, không nhất thiết phải là định dạng Spark.
    """
    filename = os.path.basename(local_model_path)
    hdfs_target = f"{hdfs_model_dir}/{filename}"
    # Đường dẫn tuyệt đối, forward-slash - xem giải thích chi tiết ở
    # save_pandas_as_parquet (hadoop.cmd trên máy này không resolve
    # đúng đường dẫn tương đối khi gọi -put).
    local_model_path_abs = os.path.abspath(local_model_path).replace("\\", "/")

    logger.info(f"  Đang đẩy model lên HDFS: {hdfs_target}")
    try:
        _run_hdfs_cmd(HDFS_CLI_CMD + ["-mkdir", "-p", hdfs_model_dir])
        result = _run_hdfs_cmd(HDFS_CLI_CMD + ["-put", "-f", local_model_path_abs, hdfs_target])

        # Xác minh lại bằng -ls (parse output thật), không chỉ tin
        # returncode (xem lý do chi tiết trong docstring _hdfs_path_exists).
        if result.returncode != 0 or not _hdfs_path_exists(hdfs_target):
            logger.error(f"  Lỗi khi đẩy model lên HDFS (đã xác minh lại): {result.stderr}")
            return False
        logger.info(f"  -> Đã đẩy model lên HDFS thành công (đã xác minh): {hdfs_target}")
        return True
    except FileNotFoundError:
        logger.warning(
            "  Lệnh 'hdfs' không tìm thấy trong PATH - bỏ qua bước đẩy "
            f"model lên HDFS. File vẫn có sẵn ở local: {local_model_path}"
        )
        return False
    except subprocess.TimeoutExpired:
        logger.warning(
            "  Lệnh 'hdfs dfs -put' bị timeout khi đẩy model - bỏ qua. "
            f"File vẫn có sẵn ở local: {local_model_path}"
        )
        return False


# ============================================================================
# MÔ HÌNH 1: K-MEANS CUSTOMER SEGMENTATION
# ============================================================================

def train_kmeans_segmentation(rfm_df, spark):
    """
    Phân cụm khách hàng bằng K-Means dựa trên chỉ số RFM (scikit-learn).
    """
    logger.info("=" * 60)
    logger.info("MÔ HÌNH 1: K-MEANS CUSTOMER SEGMENTATION")
    logger.info("=" * 60)

    logger.info("  Chuẩn bị dữ liệu cho K-Means...")
    cols_needed = [
        "customer_unique_id", "recency", "frequency", "monetary",
        "avg_review_score", "avg_delivery_days", "customer_tenure_days",
        "r_score", "f_score", "m_score"
    ]
    kmeans_data = rfm_df[cols_needed].dropna().reset_index(drop=True)
    for col in ["recency", "frequency", "monetary"]:
        kmeans_data[col] = kmeans_data[col].astype(float)

    logger.info(f"  -> Dữ liệu K-Means: {len(kmeans_data):,} khách hàng")

    # --- Chuẩn hóa + train K-Means ---
    logger.info("  Xây dựng pipeline K-Means...")
    feature_cols = ["recency", "frequency", "monetary"]
    X = kmeans_data[feature_cols].values

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    logger.info("  Đang huấn luyện K-Means (k=4)...")
    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10, max_iter=300)
    clusters = kmeans.fit_predict(X_scaled)
    kmeans_data["cluster"] = clusters

    # --- Đánh giá bằng Silhouette Score ---
    # Lấy mẫu tối đa 10,000 dòng để tính silhouette cho nhanh nếu data lớn
    sample_size = min(10000, len(X_scaled))
    if sample_size < len(X_scaled):
        idx = np.random.RandomState(42).choice(len(X_scaled), sample_size, replace=False)
        sil_score = silhouette_score(X_scaled[idx], clusters[idx])
    else:
        sil_score = silhouette_score(X_scaled, clusters)
    logger.info(f"  -> Silhouette Score: {sil_score:.4f}")

    # --- Thống kê từng cụm ---
    cluster_stats = (
        kmeans_data
        .groupby("cluster")
        .agg(
            customer_count=("customer_unique_id", "count"),
            avg_recency=("recency", "mean"),
            avg_frequency=("frequency", "mean"),
            avg_monetary=("monetary", "mean"),
            avg_review=("avg_review_score", "mean"),
        )
        .reset_index()
        .sort_values("cluster")
    )
    logger.info("  Thống kê từng cụm:")
    print(cluster_stats.to_string(index=False))

    # --- Gán tên phân khúc dựa trên monetary giảm dần ---
    logger.info("  Gán tên phân khúc khách hàng...")
    sorted_clusters = cluster_stats.sort_values("avg_monetary", ascending=False)
    segment_names = ["Champions", "Loyal", "At Risk", "Lost"]
    cluster_to_segment = {}
    for i, (_, row) in enumerate(sorted_clusters.iterrows()):
        cluster_id = int(row["cluster"])
        cluster_to_segment[cluster_id] = segment_names[i]
        logger.info(
            f"    Cụm {cluster_id} -> {segment_names[i]}: "
            f"R={row['avg_recency']:.0f}, "
            f"F={row['avg_frequency']:.1f}, "
            f"M={row['avg_monetary']:.0f}"
        )

    kmeans_data["segment_name"] = kmeans_data["cluster"].map(cluster_to_segment)

    # --- Phân bố phân khúc ---
    segment_dist = (
        kmeans_data
        .groupby("segment_name")
        .agg(
            customer_count=("customer_unique_id", "count"),
            avg_monetary=("monetary", "mean"),
            avg_recency=("recency", "mean"),
            avg_frequency=("frequency", "mean"),
        )
        .reset_index()
        .sort_values("avg_monetary", ascending=False)
    )
    logger.info("  Phân bố phân khúc khách hàng:")
    print(segment_dist.to_string(index=False))

    # --- Lưu mô hình (local trước, không cần đẩy lên HDFS - tùy chọn) ---
    os.makedirs(LOCAL_TMP_DIR, exist_ok=True)
    model_path = os.path.join(LOCAL_TMP_DIR, "kmeans_segmentation.joblib")
    joblib.dump({"scaler": scaler, "kmeans": kmeans}, model_path)
    logger.info(f"  -> Đã lưu mô hình K-Means tại: {model_path}")
    upload_model_to_hdfs(model_path, HDFS_MODELS_PATH_CLI + "/kmeans_segmentation")

    # --- Lưu kết quả phân khúc lên HDFS ---
    segment_output_path = f"{HDFS_PROCESSED_PATH}/customer_segments"
    output_cols = [
        "customer_unique_id", "recency", "frequency", "monetary",
        "cluster", "segment_name",
        "avg_review_score", "avg_delivery_days", "customer_tenure_days",
        "r_score", "f_score", "m_score",
    ]
    save_pandas_as_parquet(spark, kmeans_data[output_cols], segment_output_path)
    logger.info(f"  -> Đã lưu phân khúc tại: {segment_output_path}")

    results = {
        "model_name": "KMeans_Segmentation",
        "k": 4,
        "silhouette_score": round(float(sil_score), 4),
        "cluster_mapping": cluster_to_segment,
        "segment_stats": segment_dist.to_dict(orient="records"),
    }

    return results, kmeans_data


# ============================================================================
# MÔ HÌNH 2: CHURN PREDICTION (PHÂN LOẠI)
# ============================================================================

def train_churn_prediction(merged_df):
    """
    Dự đoán khách hàng rời bỏ (churn) bằng RandomForest + LogisticRegression
    (scikit-learn).
    """
    logger.info("=" * 60)
    logger.info("MÔ HÌNH 2: CHURN PREDICTION")
    logger.info("=" * 60)

    logger.info("  Chuẩn bị dữ liệu cho Churn Prediction...")

    delivered = merged_df[merged_df["order_status"] == "delivered"].copy()

    churn_data = (
        delivered
        .groupby("customer_unique_id")
        .agg(
            recency=("recency", "first"),
            frequency=("frequency", "first"),
            monetary=("monetary", "first"),
            avg_items=("total_items", "mean"),
            avg_order_value=("order_value", "mean"),
            avg_freight_ratio=("freight_ratio", "mean"),
            avg_review_score=("review_score", "mean"),
            avg_delivery_days=("delivery_days", "mean"),
            avg_installments=("max_installments", "mean"),
            label=("churn", "first"),
        )
        .reset_index()
        .dropna()
    )

    # QUAN TRỌNG: "recency" KHÔNG được dùng làm feature ở đây.
    # Lý do: cột "churn" (label) được định nghĩa trực tiếp là
    # churn = 1 nếu recency > 90 ngày. Nếu đưa recency vào feature,
    # model chỉ cần học lại đúng ngưỡng 90 đó là đoán đúng 100%
    # (data leakage) - không phải model "dự đoán" được gì, mà đang
    # nhìn thẳng vào công thức tạo ra label. Vì vậy bỏ recency ra,
    # chỉ dùng các đặc trưng hành vi KHÁC để dự đoán churn thật sự.
    numeric_cols = [
        "frequency", "monetary", "avg_items", "avg_order_value",
        "avg_freight_ratio", "avg_review_score", "avg_delivery_days",
        "avg_installments"
    ]
    for col in numeric_cols + ["recency"]:
        churn_data[col] = churn_data[col].astype(float)
    churn_data["label"] = churn_data["label"].astype(float)

    logger.info(f"  -> Dữ liệu churn: {len(churn_data):,} khách hàng")
    logger.info("  Phân bố label churn:")
    print(churn_data["label"].value_counts().to_string())

    X = churn_data[numeric_cols].values
    y = churn_data["label"].values

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    logger.info(f"  -> Train: {len(X_train):,}, Test: {len(X_test):,}")

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # --- RandomForestClassifier ---
    logger.info("  Huấn luyện RandomForestClassifier...")
    rf_classifier = RandomForestClassifier(
        n_estimators=100, max_depth=8, random_state=42, n_jobs=-1
    )
    rf_classifier.fit(X_train_scaled, y_train)
    rf_pred = rf_classifier.predict(X_test_scaled)
    rf_proba = rf_classifier.predict_proba(X_test_scaled)[:, 1]

    rf_metrics = evaluate_classification(y_test, rf_pred, rf_proba, "RandomForest")

    importance_df = pd.DataFrame({
        "feature": numeric_cols,
        "importance": rf_classifier.feature_importances_,
    }).sort_values("importance", ascending=False)
    logger.info("  Feature Importance (RandomForest):")
    print(importance_df.to_string(index=False))

    # --- LogisticRegression ---
    logger.info("  Huấn luyện LogisticRegression...")
    lr_classifier = LogisticRegression(max_iter=1000, C=1.0, random_state=42)
    lr_classifier.fit(X_train_scaled, y_train)
    lr_pred = lr_classifier.predict(X_test_scaled)
    lr_proba = lr_classifier.predict_proba(X_test_scaled)[:, 1]

    lr_metrics = evaluate_classification(y_test, lr_pred, lr_proba, "LogisticRegression")

    # --- Confusion matrix ---
    logger.info("  Confusion Matrix (RandomForest):")
    rf_cm = create_confusion_matrix(y_test, rf_pred)
    logger.info("  Confusion Matrix (LogisticRegression):")
    lr_cm = create_confusion_matrix(y_test, lr_pred)

    # --- Chọn & lưu mô hình tốt nhất ---
    best_model_name = "RandomForest" if rf_metrics["auc_roc"] >= lr_metrics["auc_roc"] else "LogisticRegression"
    best_model = rf_classifier if best_model_name == "RandomForest" else lr_classifier

    os.makedirs(LOCAL_TMP_DIR, exist_ok=True)
    model_path = os.path.join(LOCAL_TMP_DIR, "churn_prediction.joblib")
    joblib.dump({"scaler": scaler, "model": best_model}, model_path)
    logger.info(f"  Đã lưu mô hình tốt nhất ({best_model_name}) tại: {model_path}")
    upload_model_to_hdfs(model_path, HDFS_MODELS_PATH_CLI + "/churn_prediction")

    results = {
        "model_name": "Churn_Prediction",
        "best_model": best_model_name,
        "random_forest": rf_metrics,
        "logistic_regression": lr_metrics,
        "feature_importance": importance_df.to_dict(orient="records"),
        "confusion_matrix_rf": rf_cm,
        "confusion_matrix_lr": lr_cm,
    }

    return results


def evaluate_classification(y_true, y_pred, y_proba, model_name):
    """Đánh giá mô hình phân loại với nhiều chỉ số (scikit-learn)."""
    metrics = {
        "auc_roc": round(float(roc_auc_score(y_true, y_proba)), 4),
        "auc_pr": round(float(average_precision_score(y_true, y_proba)), 4),
        "f1_score": round(float(f1_score(y_true, y_pred)), 4),
        "accuracy": round(float(accuracy_score(y_true, y_pred)), 4),
        "precision": round(float(precision_score(y_true, y_pred, zero_division=0)), 4),
        "recall": round(float(recall_score(y_true, y_pred, zero_division=0)), 4),
    }
    logger.info(f"  Kết quả {model_name}:")
    for k, v in metrics.items():
        logger.info(f"    {k}: {v}")
    return metrics


def create_confusion_matrix(y_true, y_pred):
    """Tạo confusion matrix dạng dict TP/TN/FP/FN."""
    cm = confusion_matrix(y_true, y_pred, labels=[0, 1])
    tn, fp, fn, tp = cm.ravel()
    print(f"           Pred=0   Pred=1")
    print(f"  True=0   {tn:<8} {fp:<8}")
    print(f"  True=1   {fn:<8} {tp:<8}")
    cm_data = {
        "true_positive": int(tp),
        "true_negative": int(tn),
        "false_positive": int(fp),
        "false_negative": int(fn),
    }
    logger.info(f"    TP={tp}, TN={tn}, FP={fp}, FN={fn}")
    return cm_data


# ============================================================================
# MÔ HÌNH 3: REVIEW SCORE PREDICTION (HỒI QUY)
# ============================================================================

def train_review_prediction(merged_df):
    """
    Dự đoán điểm đánh giá (review_score) bằng RandomForestRegressor +
    LinearRegression (scikit-learn).
    """
    logger.info("=" * 60)
    logger.info("MÔ HÌNH 3: REVIEW SCORE PREDICTION")
    logger.info("=" * 60)

    logger.info("  Chuẩn bị dữ liệu cho Review Prediction...")

    mask = (
        (merged_df["order_status"] == "delivered") &
        (merged_df["review_score"] > 0) &
        (merged_df["delivery_days"].notna()) &
        (merged_df["delivery_days"] >= 0)
    )
    review_data = merged_df.loc[mask, [
        "order_id", "delivery_days", "total_freight_value", "total_price",
        "total_items", "freight_ratio", "estimated_vs_actual",
        "max_installments", "main_category_english", "payment_type",
        "review_score",
    ]].copy()

    review_data = review_data.rename(columns={
        "total_freight_value": "freight_value",
        "total_price": "price",
        "total_items": "item_count",
        "max_installments": "installments",
        "main_category_english": "category",
        "review_score": "label",
    })
    review_data = review_data.dropna()

    numeric_features = [
        "delivery_days", "freight_value", "price", "item_count",
        "freight_ratio", "estimated_vs_actual", "installments"
    ]
    for col in numeric_features + ["label"]:
        review_data[col] = review_data[col].astype(float)

    logger.info(f"  -> Dữ liệu review: {len(review_data):,} dòng")

    # --- Chia train/test ---
    train_data, test_data = train_test_split(review_data, test_size=0.2, random_state=42)
    logger.info(f"  -> Train: {len(train_data):,}, Test: {len(test_data):,}")

    X_train_raw = train_data[numeric_features + ["category", "payment_type"]]
    X_test_raw = test_data[numeric_features + ["category", "payment_type"]]
    y_train = train_data["label"].values
    y_test = test_data["label"].values

    # --- Pipeline tiền xử lý: numeric scale + categorical one-hot ---
    logger.info("  Xây dựng pipeline đặc trưng...")
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), numeric_features),
            ("cat", OneHotEncoder(handle_unknown="ignore"), ["category", "payment_type"]),
        ]
    )

    # =============================================
    # Mô hình 3A: RandomForestRegressor
    # =============================================
    logger.info("  Huấn luyện RandomForestRegressor...")
    rf_pipeline = Pipeline(steps=[
        ("preprocess", preprocessor),
        ("model", RandomForestRegressor(
            n_estimators=100, max_depth=8, random_state=42, n_jobs=-1
        )),
    ])
    rf_pipeline.fit(X_train_raw, y_train)
    rf_pred = rf_pipeline.predict(X_test_raw)
    rf_metrics = evaluate_regression(y_test, rf_pred, "RandomForestRegressor")

    # Feature importance chỉ cho các cột số (đơn giản hóa, giống bản gốc)
    rf_model = rf_pipeline.named_steps["model"]
    rf_importance_df = pd.DataFrame({
        "feature": numeric_features,
        "importance": rf_model.feature_importances_[:len(numeric_features)],
    }).sort_values("importance", ascending=False)
    logger.info("  Feature Importance (RF Regressor):")
    print(rf_importance_df.to_string(index=False))

    # =============================================
    # Mô hình 3B: LinearRegression
    # =============================================
    logger.info("  Huấn luyện LinearRegression...")
    lr_pipeline = Pipeline(steps=[
        ("preprocess", preprocessor),
        ("model", LinearRegression()),
    ])
    lr_pipeline.fit(X_train_raw, y_train)
    lr_pred = lr_pipeline.predict(X_test_raw)
    lr_metrics = evaluate_regression(y_test, lr_pred, "LinearRegression")

    # --- Chọn & lưu mô hình tốt nhất ---
    best_model_name = "RandomForestRegressor" if rf_metrics["rmse"] <= lr_metrics["rmse"] else "LinearRegression"
    best_pipeline = rf_pipeline if best_model_name == "RandomForestRegressor" else lr_pipeline

    os.makedirs(LOCAL_TMP_DIR, exist_ok=True)
    model_path = os.path.join(LOCAL_TMP_DIR, "review_prediction.joblib")
    joblib.dump(best_pipeline, model_path)
    logger.info(f"  Đã lưu mô hình tốt nhất ({best_model_name}) tại: {model_path}")
    upload_model_to_hdfs(model_path, HDFS_MODELS_PATH_CLI + "/review_predictor")

    # --- Phân tích dự đoán vs thực tế ---
    compare_df = pd.DataFrame({"label": y_test, "prediction": rf_pred})
    logger.info("  Phân tích dự đoán vs thực tế (RF):")
    print(compare_df.describe().to_string())

    results = {
        "model_name": "Review_Score_Prediction",
        "best_model": best_model_name,
        "random_forest_regressor": rf_metrics,
        "linear_regression": lr_metrics,
        "feature_importance": rf_importance_df.to_dict(orient="records"),
    }

    return results


def evaluate_regression(y_true, y_pred, model_name):
    """Đánh giá mô hình hồi quy với: RMSE, R², MAE (scikit-learn)."""
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    r2 = float(r2_score(y_true, y_pred))
    mae = float(mean_absolute_error(y_true, y_pred))

    metrics = {
        "rmse": round(rmse, 4),
        "r2": round(r2, 4),
        "mae": round(mae, 4),
    }
    logger.info(f"  Kết quả {model_name}:")
    for k, v in metrics.items():
        logger.info(f"    {k}: {v}")
    return metrics


# ============================================================================
# MAIN: Chạy tất cả mô hình
# ============================================================================

def run_all_models():
    """Chạy tất cả 3 mô hình ML và trả về kết quả tổng hợp."""
    start_time = datetime.now()
    logger.info("*" * 60)
    logger.info("BẮT ĐẦU HUẤN LUYỆN MÔ HÌNH ML (scikit-learn)")
    logger.info(f"Thời gian: {start_time}")
    logger.info("*" * 60)

    spark = None
    try:
        # Spark chỉ dùng để đọc dữ liệu từ HDFS rồi chuyển sang pandas
        spark = create_spark_session()
        merged_df, rfm_df = load_data(spark)

        all_results = {}

        # Mô hình 1: K-Means Segmentation
        logger.info("\n" + "=" * 60)
        kmeans_results, segmented_df = train_kmeans_segmentation(rfm_df, spark)
        all_results["kmeans_segmentation"] = kmeans_results

        # Mô hình 2: Churn Prediction (thuần pandas/sklearn, không cần Spark)
        logger.info("\n" + "=" * 60)
        churn_results = train_churn_prediction(merged_df)
        all_results["churn_prediction"] = churn_results

        # Mô hình 3: Review Score Prediction (thuần pandas/sklearn)
        logger.info("\n" + "=" * 60)
        review_results = train_review_prediction(merged_df)
        all_results["review_prediction"] = review_results

        # Tóm tắt kết quả
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("\n" + "*" * 60)
        logger.info("TẤT CẢ MÔ HÌNH ĐÃ HUẤN LUYỆN XONG!")
        logger.info(f"  Thời gian chạy: {duration:.1f} giây")
        logger.info("  Tóm tắt kết quả:")
        logger.info(f"    K-Means Silhouette: {kmeans_results['silhouette_score']}")
        logger.info(f"    Churn Best Model: {churn_results['best_model']}")
        logger.info(f"    Churn AUC-ROC (RF): {churn_results['random_forest']['auc_roc']}")
        logger.info(f"    Churn AUC-ROC (LR): {churn_results['logistic_regression']['auc_roc']}")
        logger.info(f"    Review Best Model: {review_results['best_model']}")
        logger.info(f"    Review RMSE (RF): {review_results['random_forest_regressor']['rmse']}")
        logger.info(f"    Review RMSE (LR): {review_results['linear_regression']['rmse']}")
        logger.info(f"  Model files lưu tại thư mục local: {LOCAL_TMP_DIR}/")
        logger.info("*" * 60)

        # Lưu toàn bộ kết quả ML ra file JSON local. File này cho phép
        # export_to_mongo.py đọc lại kết quả thật (AUC, RMSE...) khi
        # chạy ĐỘC LẬP (không import trực tiếp ml_models_simple.py),
        # thay vì phải chạy với ml_results=None (placeholder rỗng).
        os.makedirs(LOCAL_TMP_DIR, exist_ok=True)
        results_json_path = os.path.join(LOCAL_TMP_DIR, "ml_results.json")
        with open(results_json_path, "w", encoding="utf-8") as f:
            json.dump(all_results, f, default=str, ensure_ascii=False, indent=2)
        logger.info(f"  Đã lưu kết quả ML ra: {results_json_path}")

        return all_results

    except Exception as e:
        logger.error(f"LỖI ML: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    results = run_all_models()
    logger.info("Hoàn tất huấn luyện tất cả mô hình ML.")
