"""
=============================================================================
Machine Learning Models - Olist E-Commerce Big Data Project
=============================================================================
Su dung 100% Spark MLlib (JVM-based).
Thiet ke tranh loi Python worker crash tren Windows:
  - KHONG dung toPandas(), collect(), Python UDF, RDD lambda
  - Chi dung DataFrame API + MLlib (chay tren JVM)
  - Lay metrics qua Evaluator (tra so qua Py4J)
  - Confusion matrix bang filter().count()
  - Feature importance qua model.featureImportances (Py4J)

Mo hinh:
  1. K-Means Customer Segmentation (phan cum RFM)
  2. Churn Prediction (Random Forest + Logistic Regression)
  3. Review Score Prediction (Random Forest + Linear Regression)
=============================================================================
"""

import os
import sys
import json
import logging
from datetime import datetime

# ==========================================================================
# Buoc PySpark dung Python 3.12 (Python 3.13 khong tuong thich)
# ==========================================================================
PYTHON_PATH = "C:/Users/Admin/AppData/Local/Programs/Python/Python312/python.exe"
os.environ["PYSPARK_PYTHON"] = PYTHON_PATH
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_PATH

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    VectorAssembler, StandardScaler, StringIndexer
)
from pyspark.ml.clustering import KMeans
from pyspark.ml.classification import (
    RandomForestClassifier, LogisticRegression
)
from pyspark.ml.regression import (
    RandomForestRegressor,
    LinearRegression as SparkLinearRegression
)
from pyspark.ml.evaluation import (
    ClusteringEvaluator,
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
    RegressionEvaluator
)
from pyspark.ml.functions import vector_to_array
from pyspark.ml.classification import GBTClassifier
# ==========================================================================
# Cau hinh
# ==========================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("OlistML")

HDFS_SILVER = "hdfs://localhost:9000/user/bigdata/olist/silver"
HDFS_GOLD = "hdfs://localhost:9000/user/bigdata/olist/gold"
HDFS_MODELS = "hdfs://localhost:9000/user/bigdata/olist/models"


# ==========================================================================
# Tao SparkSession - toi uu cho may yeu + tranh loi socket Windows
# ==========================================================================
def create_spark_session():
    """
    Tao SparkSession toi uu cho may yeu.
    - local[2]: chi dung 2 cores, giam tai CPU
    - 2g driver memory: du cho ~100K rows
    - shuffle.partitions=4: giam overhead shuffle
    - driver.host=127.0.0.1: tranh loi Hyper-V hostname
    """
    logger.info("Dang khoi tao SparkSession cho ML...")
    spark = (
        SparkSession.builder
        .appName("Olist_ML_Models")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        # Tranh loi socket tren Windows (Hyper-V)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        # Buoc worker dung Python 3.12
        .config("spark.pyspark.python", PYTHON_PATH)
        .config("spark.pyspark.driver.python", PYTHON_PATH)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession da khoi tao thanh cong.")
    return spark


# ==========================================================================
# Doc du lieu tu HDFS
# ==========================================================================
def load_data(spark):
    """Doc du lieu da xu ly tu HDFS (output cua etl.py)."""
    logger.info("Dang doc du lieu tu HDFS...")

    merged_df = spark.read.parquet(f"{HDFS_SILVER}/merged_orders")
    rfm_df = spark.read.parquet(f"{HDFS_GOLD}/rfm_customers")

    # count() tra ve long qua Py4J — khong can Python workers
    merged_count = merged_df.count()
    rfm_count = rfm_df.count()
    logger.info(f"  merged_orders: {merged_count:,} dong")
    logger.info(f"  rfm_customers: {rfm_count:,} dong")

    return merged_df, rfm_df


# ==========================================================================
# MO HINH 1: K-MEANS CUSTOMER SEGMENTATION
# ==========================================================================
def train_kmeans_segmentation(rfm_df, spark):
    """
    Phan cum khach hang bang K-Means tren features RFM.
    100% Spark MLlib (JVM) — khong can Python workers.

    Pipeline: VectorAssembler -> StandardScaler -> KMeans(k=4)
    Danh gia: Silhouette Score
    Dat ten segment: dua vao cluster centers (qua Py4J)
    """
    logger.info("=" * 60)
    logger.info("MO HINH 1: K-MEANS CUSTOMER SEGMENTATION")
    logger.info("=" * 60)

    # Chuan bi features RFM
    feature_cols = ["recency", "frequency", "monetary"]
    clean_df = (
        rfm_df
        .select("customer_unique_id", *feature_cols)
        .na.drop(subset=feature_cols)
    )
    data_count = clean_df.count()
    logger.info(f"  Du lieu K-Means: {data_count:,} khach hang")

    # --- Pipeline: Assembler -> Scaler -> KMeans ---
    assembler = VectorAssembler(
        inputCols=feature_cols, outputCol="features_raw"
    )
    scaler = StandardScaler(
        inputCol="features_raw", outputCol="features",
        withStd=True, withMean=True
    )
    kmeans = KMeans(
        featuresCol="features", predictionCol="cluster",
        k=4, seed=42, maxIter=30
    )
    pipeline = Pipeline(stages=[assembler, scaler, kmeans])

    logger.info("  Dang huan luyen K-Means (k=4)...")
    model = pipeline.fit(clean_df)
    predictions = model.transform(clean_df)

    # --- Danh gia Silhouette Score ---
    evaluator = ClusteringEvaluator(
        featuresCol="features", predictionCol="cluster",
        metricName="silhouette"
    )
    silhouette = evaluator.evaluate(predictions)
    logger.info(f"  -> Silhouette Score: {silhouette:.4f}")

    # --- Dat ten segment dua vao cluster centers ---
    # clusterCenters() tra ve list numpy arrays qua Py4J (khong can workers)
    kmeans_model = model.stages[-1]  # KMeansModel
    centers = kmeans_model.clusterCenters()

    # Sap xep theo monetary (index 2) giam dan
    # StandardScaler la linear transform nen thu tu duoc bao toan
    indexed_centers = [
        (i, float(centers[i][2]))  # (cluster_id, scaled_monetary)
        for i in range(len(centers))
    ]
    indexed_centers.sort(key=lambda x: x[1], reverse=True)

    segment_names = ["Champions", "Loyal", "At Risk", "Lost"]
    cluster_to_segment = {}
    for rank, (cluster_id, monetary_val) in enumerate(indexed_centers):
        cluster_to_segment[cluster_id] = segment_names[rank]
        logger.info(
            f"    Cluster {cluster_id} -> {segment_names[rank]}"
        )

    # --- Gan ten segment bang F.when() (JVM, khong can workers) ---
    condition = None
    for cid, name in cluster_to_segment.items():
        if condition is None:
            condition = F.when(F.col("cluster") == cid, F.lit(name))
        else:
            condition = condition.when(F.col("cluster") == cid, F.lit(name))
    condition = condition.otherwise(F.lit("Unknown"))

    segmented_df = predictions.withColumn("segment_name", condition)

    # --- Thong ke phan bo (dung filter + count, tranh collect) ---
    logger.info("  Phan bo phan khuc khach hang:")
    segment_stats = {}
    for cid, name in cluster_to_segment.items():
        count = segmented_df.filter(F.col("cluster") == cid).count()
        segment_stats[name] = int(count)
        logger.info(f"    {name}: {count:,} khach hang")

    # --- Luu model len HDFS ---
    model_path = f"{HDFS_MODELS}/kmeans_segmentation"
    model.write().overwrite().save(model_path)
    logger.info(f"  Da luu model tai: {model_path}")

    # --- Luu ket qua phan cum len HDFS (Parquet) ---
    output_path = f"{HDFS_GOLD}/customer_segments"
    (
        segmented_df
        .select(
            "customer_unique_id", "recency", "frequency", "monetary",
            "cluster", "segment_name"
        )
        .coalesce(2)
        .write.mode("overwrite")
        .parquet(output_path)
    )
    logger.info(f"  Da luu segments tai: {output_path}")

    results = {
        "model_name": "KMeans_Segmentation",
        "k": 4,
        "silhouette_score": round(silhouette, 4),
        "cluster_mapping": {str(k): v for k, v in cluster_to_segment.items()},
        "segment_stats": segment_stats,
    }

    return results, segmented_df


# ==========================================================================
# MO HINH 2: CHURN PREDICTION (CLASSIFICATION)
# ==========================================================================
def train_churn_prediction(merged_df, rfm_df, spark):
    """
    Du doan khach hang roi bo bang GBTClassifier.
    Su dung Composite Churn Score de dong bo voi Sklearn model.
    """
    logger.info("=" * 60)
    logger.info("MO HINH 2: CHURN PREDICTION (CLASSIFICATION)")
    logger.info("=" * 60)

    # --- Tinh them features per customer tu merged_orders ---
    customer_features = (
        merged_df
        .groupBy("customer_unique_id")
        .agg(
            F.avg("avg_review_score").alias("avg_review"),
            F.avg("delivery_days").alias("avg_delivery_days"),
        )
    )

    # --- Join voi RFM ---
    ml_df = (
        rfm_df
        .select("customer_unique_id", "recency", "frequency", "monetary")
        .join(customer_features, on="customer_unique_id", how="inner")
    )

    # Xu ly null
    fill_values = {"recency": 0.0, "frequency": 1.0, "monetary": 0.0, "avg_review": 4.0, "avg_delivery_days": 10.0}
    ml_df = ml_df.fillna(fill_values)

    # --- Tinh Composite Risk Score ---
    ml_df = ml_df.withColumn("rec_n", F.least(F.col("recency") / 730.0, F.lit(1.0)))
    ml_df = ml_df.withColumn("freq_n", F.least((F.col("frequency") - 1) / 19.0, F.lit(1.0)))
    ml_df = ml_df.withColumn("mon_n", F.least(F.col("monetary") / 5000.0, F.lit(1.0)))
    ml_df = ml_df.withColumn("rev_n", (F.col("avg_review") - 1) / 4.0)
    ml_df = ml_df.withColumn("del_n", F.least(F.col("avg_delivery_days") / 60.0, F.lit(1.0)))

    ml_df = ml_df.withColumn(
        "risk",
        0.30 * F.col("rec_n") +
        0.15 * (1.0 - F.col("freq_n")) +
        0.15 * (1.0 - F.col("mon_n")) +
        0.20 * (1.0 - F.col("rev_n")) +
        0.20 * F.col("del_n")
    )
    ml_df = ml_df.withColumn("label", F.when(F.col("risk") > 0.50, 1.0).otherwise(0.0))

    feature_cols = ["recency", "frequency", "monetary", "avg_review", "avg_delivery_days"]

    data_count = ml_df.count()
    churn_1 = ml_df.filter(F.col("label") == 1.0).count()
    churn_0 = ml_df.filter(F.col("label") == 0.0).count()
    logger.info(f"  Du lieu: {data_count:,} khach hang")
    logger.info(f"  Churn=1 (roi bo): {churn_1:,}, Churn=0 (giu lai): {churn_0:,}")
    
    # Can bang du lieu bang Class Weights
    w_1 = data_count / (2 * churn_1) if churn_1 > 0 else 1.0
    w_0 = data_count / (2 * churn_0) if churn_0 > 0 else 1.0
    ml_df = ml_df.withColumn("weight", F.when(F.col("label") == 1.0, w_1).otherwise(w_0))

    # --- Train/Test split ---
    train_df, test_df = ml_df.randomSplit([0.8, 0.2], seed=42)

    # --- VectorAssembler ---
    assembler = VectorAssembler(
        inputCols=feature_cols, outputCol="features",
        handleInvalid="skip"
    )

    # --- Gradient Boosting Classifier ---
    gbt = GBTClassifier(
        featuresCol="features", labelCol="label", weightCol="weight",
        maxIter=50, maxDepth=5, seed=42
    )
    gbt_pipeline = Pipeline(stages=[assembler, gbt])

    logger.info("  Dang huan luyen GBTClassifier...")
    best_model = gbt_pipeline.fit(train_df)
    best_preds = best_model.transform(test_df)
    best_name = "GradientBoosting"

    # --- Danh gia ---
    auc_eval = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    acc_eval = MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy")
    f1_eval = MulticlassClassificationEvaluator(labelCol="label", metricName="f1")
    prec_eval = MulticlassClassificationEvaluator(labelCol="label", metricName="weightedPrecision")
    rec_eval = MulticlassClassificationEvaluator(labelCol="label", metricName="weightedRecall")

    gbt_metrics = {
        "auc_roc": round(auc_eval.evaluate(best_preds), 4),
        "accuracy": round(acc_eval.evaluate(best_preds), 4),
        "f1_score": round(f1_eval.evaluate(best_preds), 4),
        "precision": round(prec_eval.evaluate(best_preds), 4),
        "recall": round(rec_eval.evaluate(best_preds), 4),
    }
    logger.info(f"  GBT -> AUC: {gbt_metrics['auc_roc']}, Acc: {gbt_metrics['accuracy']}, F1: {gbt_metrics['f1_score']}")

    # --- Confusion Matrix ---
    tp = best_preds.filter((F.col("label") == 1.0) & (F.col("prediction") == 1.0)).count()
    tn = best_preds.filter((F.col("label") == 0.0) & (F.col("prediction") == 0.0)).count()
    fp = best_preds.filter((F.col("label") == 0.0) & (F.col("prediction") == 1.0)).count()
    fn = best_preds.filter((F.col("label") == 1.0) & (F.col("prediction") == 0.0)).count()
    logger.info(f"  Confusion Matrix: TP={tp}, TN={tn}, FP={fp}, FN={fn}")

    # --- Feature Importance ---
    gbt_classifier = best_model.stages[-1]
    importances = gbt_classifier.featureImportances.toArray().tolist()
    fi_dict = {}
    for i, name in enumerate(feature_cols):
        fi_dict[name] = round(importances[i], 4) if i < len(importances) else 0.0
    fi_sorted = dict(sorted(fi_dict.items(), key=lambda x: x[1], reverse=True))

    logger.info("  Feature Importance (GBT):")
    for feat, imp in fi_sorted.items():
        logger.info(f"    {feat}: {imp:.4f}")

    # --- Luu model ---
    model_path = f"{HDFS_MODELS}/churn_classifier"
    best_model.write().overwrite().save(model_path)

    # --- Luu churn predictions (Parquet) ---
    churn_output = (
        best_preds
        .withColumn("prob_array", vector_to_array("probability"))
        .withColumn("churn_probability", F.round(F.element_at("prob_array", 2), 4))
        .select("customer_unique_id", "label", "prediction", "churn_probability")
    )
    output_path = f"{HDFS_GOLD}/churn_predictions"
    churn_output.coalesce(2).write.mode("overwrite").parquet(output_path)

    results = {
        "model_name": "Churn_Prediction",
        "best_model": best_name,
        "models": {
            "GradientBoosting": gbt_metrics,
        },
        "confusion_matrix": {
            "true_positive": int(tp),
            "true_negative": int(tn),
            "false_positive": int(fp),
            "false_negative": int(fn)
        },
        "feature_importance": fi_sorted,
        "features_used": feature_cols
    }
    return results


# ==========================================================================
# MO HINH 3: REVIEW SCORE PREDICTION (REGRESSION)
# ==========================================================================
def train_review_prediction(merged_df, spark):
    """
    Du doan diem review bang RandomForestRegressor + LinearRegression.
    100% Spark MLlib (JVM).

    Features: gia, phi ship, thoi gian giao hang, thanh toan, danh muc san pham
    Label: avg_review_score
    """
    logger.info("=" * 60)
    logger.info("MO HINH 3: REVIEW SCORE PREDICTION (REGRESSION)")
    logger.info("=" * 60)

    # --- Chon features ---
    numeric_features = [
        "total_price", "total_freight_value", "delivery_days",
        "total_payment_value", "total_items", "freight_ratio",
    ]
    cat_feature = "main_category_english"
    label_col = "avg_review_score"

    # Lay du lieu, loai null
    review_df = (
        merged_df
        .select(*numeric_features, cat_feature, label_col)
        .na.drop(subset=[label_col])
    )

    # Fill null
    fill_values = {col: 0.0 for col in numeric_features}
    fill_values[cat_feature] = "unknown"
    review_df = review_df.fillna(fill_values)

    review_df = review_df.withColumn(
        "label", F.col(label_col).cast("double")
    ).filter(F.col("label").isNotNull())

    data_count = review_df.count()
    logger.info(f"  Du lieu: {data_count:,} dong")
    logger.info(f"  Numeric features: {numeric_features}")
    logger.info(f"  Categorical: {cat_feature}")

    # --- Pipeline: StringIndexer -> VectorAssembler -> Model ---
    indexer = StringIndexer(
        inputCol=cat_feature, outputCol="category_index",
        handleInvalid="keep"
    )
    all_feature_cols = numeric_features + ["category_index"]

    assembler = VectorAssembler(
        inputCols=all_feature_cols, outputCol="features",
        handleInvalid="skip"
    )

    # --- Train/Test split ---
    train_df, test_df = review_df.randomSplit([0.8, 0.2], seed=42)
    logger.info(f"  Train: {train_df.count():,}, Test: {test_df.count():,}")

    # --- Random Forest Regressor ---
    rf = RandomForestRegressor(
        featuresCol="features", labelCol="label",
        numTrees=50, maxDepth=8, maxBins=80, seed=42
    )
    rf_pipeline = Pipeline(stages=[indexer, assembler, rf])

    logger.info("  Dang huan luyen Random Forest Regressor...")
    rf_model = rf_pipeline.fit(train_df)
    rf_preds = rf_model.transform(test_df)

    # --- Linear Regression ---
    lr = SparkLinearRegression(
        featuresCol="features", labelCol="label",
        maxIter=100, regParam=0.01
    )
    lr_pipeline = Pipeline(stages=[indexer, assembler, lr])

    logger.info("  Dang huan luyen Linear Regression...")
    lr_model = lr_pipeline.fit(train_df)
    lr_preds = lr_model.transform(test_df)

    # --- Danh gia (Evaluator tra float qua Py4J) ---
    rmse_eval = RegressionEvaluator(labelCol="label", metricName="rmse")
    r2_eval = RegressionEvaluator(labelCol="label", metricName="r2")
    mae_eval = RegressionEvaluator(labelCol="label", metricName="mae")

    rf_metrics = {
        "rmse": round(rmse_eval.evaluate(rf_preds), 4),
        "r2": round(r2_eval.evaluate(rf_preds), 4),
        "mae": round(mae_eval.evaluate(rf_preds), 4),
    }
    logger.info(
        f"  RF  -> RMSE: {rf_metrics['rmse']}, "
        f"R2: {rf_metrics['r2']}, MAE: {rf_metrics['mae']}"
    )

    lr_metrics = {
        "rmse": round(rmse_eval.evaluate(lr_preds), 4),
        "r2": round(r2_eval.evaluate(lr_preds), 4),
        "mae": round(mae_eval.evaluate(lr_preds), 4),
    }
    logger.info(
        f"  LR  -> RMSE: {lr_metrics['rmse']}, "
        f"R2: {lr_metrics['r2']}, MAE: {lr_metrics['mae']}"
    )

    # --- Chon model tot nhat theo RMSE (thap hon = tot hon) ---
    if rf_metrics["rmse"] <= lr_metrics["rmse"]:
        best_name = "RandomForest"
        best_model = rf_model
    else:
        best_name = "LinearRegression"
        best_model = lr_model
    logger.info(f"  -> Best model: {best_name}")

    # --- Feature Importance (RF, qua Py4J) ---
    rf_regressor = rf_model.stages[-1]  # RandomForestRegressionModel
    importances = rf_regressor.featureImportances.toArray().tolist()
    fi_dict = {}
    for i, name in enumerate(all_feature_cols):
        fi_dict[name] = round(importances[i], 4) if i < len(importances) else 0.0
    fi_sorted = dict(sorted(fi_dict.items(), key=lambda x: x[1], reverse=True))

    logger.info("  Feature Importance (RF):")
    for feat, imp in fi_sorted.items():
        logger.info(f"    {feat}: {imp:.4f}")

    # --- Luu model ---
    model_path = f"{HDFS_MODELS}/review_predictor"
    best_model.write().overwrite().save(model_path)
    logger.info(f"  Da luu model tai: {model_path}")

    results = {
        "model_name": "Review_Score_Prediction",
        "best_model": best_name,
        "models": {
            "RandomForest": rf_metrics,
            "LinearRegression": lr_metrics,
        },
        "feature_importance": fi_sorted,
        "features_used": all_feature_cols,
    }

    return results


# ==========================================================================
# CHAY TOAN BO PIPELINE ML
# ==========================================================================
def run_all_models():
    """Chay toan bo 3 mo hinh ML."""
    start_time = datetime.now()
    logger.info("*" * 60)
    logger.info("BAT DAU HUAN LUYEN MO HINH ML (Spark MLlib)")
    logger.info(f"Thoi gian: {start_time}")
    logger.info("*" * 60)

    spark = create_spark_session()

    try:
        # Doc du lieu
        merged_df, rfm_df = load_data(spark)

        # Cache de tang toc (du lieu duoc dung nhieu lan)
        merged_df.cache()
        rfm_df.cache()

        # ----- Model 1: K-Means -----
        logger.info("")
        kmeans_results, segmented_df = train_kmeans_segmentation(
            rfm_df, spark
        )

        # ----- Model 2: Churn Prediction -----
        logger.info("")
        churn_results = train_churn_prediction(merged_df, rfm_df, spark)

        # ----- Model 3: Review Prediction -----
        logger.info("")
        review_results = train_review_prediction(merged_df, spark)

        # Giai phong cache
        merged_df.unpersist()
        rfm_df.unpersist()

        # ----- Luu tong hop ket qua ML ra JSON -----
        elapsed = (datetime.now() - start_time).total_seconds()
        all_results = {
            "kmeans_segmentation": kmeans_results,
            "churn_prediction": churn_results,
            "review_prediction": review_results,
            "timestamp": start_time.isoformat(),
            "duration_seconds": round(elapsed, 1),
        }

        os.makedirs("tmp_models", exist_ok=True)
        json_path = os.path.join("tmp_models", "ml_results.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        logger.info(f"\nDa luu ket qua ML tai: {json_path}")

        # ----- Tong ket -----
        logger.info("")
        logger.info("=" * 60)
        logger.info("HOAN THANH HUAN LUYEN MO HINH")
        logger.info(f"Tong thoi gian: {elapsed:.1f} giay")
        logger.info("=" * 60)
        logger.info("  Mo hinh 1: K-Means Segmentation")
        logger.info(
            f"    Silhouette: {kmeans_results['silhouette_score']}"
        )
        logger.info(f"  Mo hinh 2: Churn ({churn_results['best_model']})")
        best_churn = churn_results["models"][churn_results["best_model"]]
        logger.info(
            f"    AUC: {best_churn['auc_roc']}, "
            f"F1: {best_churn['f1_score']}"
        )
        logger.info(
            f"  Mo hinh 3: Review ({review_results['best_model']})"
        )
        best_review = review_results["models"][review_results["best_model"]]
        logger.info(
            f"    RMSE: {best_review['rmse']}, "
            f"R2: {best_review['r2']}"
        )

    except Exception as e:
        logger.error(f"LOI: {e}")
        import traceback
        traceback.print_exc()

    finally:
        spark.stop()
        logger.info("SparkSession da dong.")


# ==========================================================================
# Giao tiep Flask API (Scikit-Learn lightweight)
# ==========================================================================
def train_sklearn_model_for_api():
    """
    Train a Scikit-Learn model using a COMPOSITE churn label that weighs
    multiple behavioural signals instead of relying solely on recency > 90.

    Composite Churn Risk Score:
        30% recency (high = bad)
        20% avg_delivery_days (high = bad)
        20% avg_review_score (low = bad, inverted)
        15% frequency (low = bad, inverted)
        15% monetary (low = bad, inverted)

    A customer is labelled as "churned" when risk_score > 0.50.
    """
    logger.info("Training Composite Churn Model for Flask API...")
    import pymongo
    import pandas as pd
    import numpy as np
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
    import joblib
    import os

    try:
        client = pymongo.MongoClient('mongodb://localhost:27017/')
        db = client['olist_dw']
        df = pd.DataFrame(list(db['customers'].find({}, {
            '_id': 0, 'recency': 1, 'frequency': 1, 'monetary': 1,
            'avg_review_score': 1, 'avg_delivery_days': 1
        })))

        if df.empty:
            logger.warning("No data found for sklearn training.")
            return

        df.fillna(0, inplace=True)

        # ── Normalize each column to 0-1 ──
        def norm(col):
            mn, mx = col.min(), col.max()
            return (col - mn) / (mx - mn) if mx > mn else col * 0

        rec_n   = norm(df['recency'])
        freq_n  = norm(df['frequency'])
        mon_n   = norm(df['monetary'])
        rev_n   = norm(df['avg_review_score'])
        del_n   = norm(df['avg_delivery_days'])

        # ── Composite churn risk score ──
        risk = (
            0.30 * rec_n +            # high recency = bad
            0.15 * (1 - freq_n) +     # low frequency = bad
            0.15 * (1 - mon_n) +      # low monetary = bad
            0.20 * (1 - rev_n) +      # low review = bad
            0.20 * del_n              # high delivery days = bad
        )

        y = (risk > 0.50).astype(int)

        churn_1 = int(y.sum())
        churn_0 = int(len(y) - churn_1)
        logger.info(f"  Composite label => Churn=1: {churn_1:,}, Churn=0: {churn_0:,}")

        # ── Features ──
        feature_names = ['recency', 'frequency', 'monetary', 'avg_review_score', 'avg_delivery_days']
        X = df[feature_names]

        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        X_train, X_test, y_train, y_test = train_test_split(
            X_scaled, y, test_size=0.2, random_state=42, stratify=y
        )

        # ── Train GradientBoosting ──
        model = GradientBoostingClassifier(
            n_estimators=150,
            max_depth=5,
            learning_rate=0.1,
            subsample=0.8,
            random_state=42
        )
        model.fit(X_train, y_train)

        # ── Evaluate ──
        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1]
        acc = accuracy_score(y_test, y_pred)
        f1  = f1_score(y_test, y_pred)
        auc = roc_auc_score(y_test, y_prob)
        logger.info(f"  Accuracy: {acc:.4f}, F1: {f1:.4f}, AUC: {auc:.4f}")

        fi = dict(zip(feature_names, [round(v, 4) for v in model.feature_importances_]))
        logger.info(f"  Feature importances: {fi}")

        # ── Save ──
        os.makedirs("tmp_models", exist_ok=True)
        joblib.dump({
            'scaler': scaler,
            'model': model,
            'feature_names': feature_names,
        }, 'tmp_models/churn_prediction.joblib')
        logger.info("Composite churn model saved to tmp_models/churn_prediction.joblib.")

    except Exception as e:
        logger.error(f"Error training sklearn model: {e}")

# ==========================================================================
# MAIN
# ==========================================================================
if __name__ == "__main__":
    run_all_models()
    train_sklearn_model_for_api()
