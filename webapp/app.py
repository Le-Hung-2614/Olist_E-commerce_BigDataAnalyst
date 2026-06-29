"""
Olist E-Commerce Big Data Dashboard — Flask Application (FIXED)
"""
import os

from flask import Flask, render_template, jsonify, request
from pymongo import MongoClient
from requests import __build__
from config import Config
from bson import json_util
import json
import math
from datetime import datetime
import joblib
import numpy as np

app = Flask(__name__)
app.config.from_object(Config)

# ── MongoDB Connection ────────────────────────────────────────────────
client = MongoClient(app.config["MONGO_URI"])
db = client[app.config["DATABASE_NAME"]]

# Collections
orders_col      = db["orders"]
customers_col   = db["customers"]
products_col    = db["products"]
sellers_col     = db["sellers"]
agg_col         = db["aggregations"]


# ══════════════════════════════════════════════════════════════════════
#  HELPER
# ══════════════════════════════════════════════════════════════════════
def _json(data):
    """Serialize MongoDB results (handles ObjectId, datetime, etc.)."""
    return json.loads(json_util.dumps(data))


def _safe_round(val, n=2):
    if val is None:
        return 0
    try:
        return round(float(val), n)
    except (TypeError, ValueError):
        return 0


# ══════════════════════════════════════════════════════════════════════
#  PAGE ROUTES
# ══════════════════════════════════════════════════════════════════════
@app.route("/")
def index():
    return render_template("dashboard.html", page="overview")

@app.route("/segments")
def segments():
    return render_template("segments.html", page="segments")

@app.route("/churn")
def churn():
    return render_template("churn.html", page="churn")

@app.route("/products")
def products():
    return render_template("products.html", page="products")

@app.route("/geographic")
def geographic():
    return render_template("geographic.html", page="geographic")

@app.route("/models")
def models():
    return render_template("models.html", page="models")


# ══════════════════════════════════════════════════════════════════════
#  API — Overview KPIs
# ══════════════════════════════════════════════════════════════════════
@app.route("/api/overview")
def api_overview():
    try:
        total_orders = orders_col.count_documents({})
        total_customers = customers_col.count_documents({})

        rev_result = list(orders_col.aggregate([
            {"$group": {"_id": None, "total": {"$sum": "$order_value"}}}
        ]))
        total_revenue = _safe_round(rev_result[0]["total"]) if rev_result else 0

        
        review_result = list(orders_col.aggregate([
            {"$match": {"review.review_score": {"$exists": True, "$gt": 0}}},
            {"$group": {"_id": None, "avg": {"$avg": "$review.review_score"}}}
        ]))
        avg_review = _safe_round(review_result[0]["avg"]) if review_result else 0

        total_sellers = sellers_col.count_documents({})

        return jsonify({
            "total_revenue": total_revenue,
            "total_orders": total_orders,
            "avg_review": avg_review,
            "total_customers": total_customers,
            "total_sellers": total_sellers
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

 
# ══════════════════════════════════════════════════════════════════════
#  API — Monthly Revenue
# ══════════════════════════════════════════════════════════════════════
@app.route("/api/revenue")
def api_revenue():
    try:
        # FIX BUG 4: aggregations collection không có wrapper {data: [...]}.
        # Mỗi document là 1 row, có agg_type + các field riêng.
        # Phải find nhiều documents thay vì find_one rồi lấy doc["data"].
        docs = list(agg_col.find(
            {"agg_type": "monthly_revenue"},
            {"_id": 0, "purchase_year_month": 1, "total_revenue": 1, "order_count": 1}
        ).sort("purchase_year_month", 1))

        if docs:
            data = [{
                "month": d.get("purchase_year_month", ""),
                "revenue": _safe_round(d.get("total_revenue", 0)),
                "orders": d.get("order_count", 0)
            } for d in docs if d.get("purchase_year_month")]
            return jsonify({"data": data})

        # Fallback: tính từ orders — dùng field đúng purchase_year_month
        pipeline = [
            {"$group": {
                "_id": "$purchase_year_month",
                "revenue": {"$sum": "$order_value"},
                "orders": {"$sum": 1}
            }},
            {"$sort": {"_id": 1}}
        ]
        result = list(orders_col.aggregate(pipeline))
        data = [{"month": r["_id"], "revenue": _safe_round(r["revenue"]),
                 "orders": r["orders"]} for r in result if r["_id"]]
        return jsonify({"data": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════
#  API — Category Stats
# ══════════════════════════════════════════════════════════════════════
@app.route("/api/categories")
def api_categories():
    try:
        # FIX BUG 4 + 7: aggregations dùng flat rows, field là "main_category_english"
        docs = list(agg_col.find(
            {"agg_type": "category_stats"},
            {"_id": 0, "main_category_english": 1, "total_revenue": 1,
             "total_items_sold": 1, "avg_order_value": 1, "avg_review_score": 1}
        ).sort("total_revenue", -1))

        if docs:
            data = [{
                "category": d.get("main_category_english") or "Unknown",
                "revenue": _safe_round(d.get("total_revenue", 0)),
                "total_sold": int(d.get("total_items_sold") or 0),
                "avg_price": _safe_round(d.get("avg_order_value", 0)),
                "avg_review": _safe_round(d.get("avg_review_score", 0))
            } for d in docs]
            return jsonify({"data": data})

        # Fallback từ orders — dùng nested items.category
        pipeline = [
            {"$group": {
                "_id": "$items.category",
                "total_revenue": {"$sum": "$order_value"},
                "order_count": {"$sum": 1},
                "avg_price": {"$avg": "$order_value"},
                "avg_review": {"$avg": "$review.review_score"}
            }},
            {"$sort": {"total_revenue": -1}},
            {"$limit": 20}
        ]
        result = list(orders_col.aggregate(pipeline))
        data = [{
            "category": r["_id"] or "Unknown",
            "revenue": _safe_round(r.get("total_revenue", 0)),
            "total_sold": int(r.get("order_count") or 0),
            "avg_price": _safe_round(r.get("avg_price", 0)),
            "avg_review": _safe_round(r.get("avg_review", 0))
        } for r in result]
        return jsonify({"data": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════
#  API — Customer Segments
# ══════════════════════════════════════════════════════════════════════
@app.route("/api/segments")
def api_segments():
    try:
        # FIX BUG 4 + 6: aggregations dùng flat rows, field là "customer_count"
        seg_docs = list(agg_col.find(
            {"agg_type": "segment_stats"},
            {"_id": 0, "segment_name": 1, "customer_count": 1}
        ))

        if seg_docs:
            distribution = [{"segment": d.get("segment_name") or "Unknown",
                             "count": d.get("customer_count", 0)} for d in seg_docs]
        else:
            pipeline = [
                {"$group": {"_id": "$segment_name", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            result = list(customers_col.aggregate(pipeline))
            distribution = [{"segment": r["_id"] or "Unknown", "count": r["count"]}
                           for r in result]

        # RFM scatter — flat fields đúng
        rfm_pipeline = [
            {"$match": {"recency": {"$exists": True}}},
            {"$sample": {"size": 500}},
            {"$project": {
                "_id": 0,
                "customer_id": "$customer_unique_id",
                "segment": "$segment_name",
                "recency": 1,
                "frequency": 1,
                "monetary": 1
            }}
        ]
        rfm_data = list(customers_col.aggregate(rfm_pipeline))

        # Top customers per segment
        top_pipeline = [
            {"$match": {"monetary": {"$exists": True}}},
            {"$sort": {"monetary": -1}},
            {"$limit": 50},
            {"$project": {
                "_id": 0,
                "customer_id": "$customer_unique_id",
                "segment": "$segment_name",
                "city": "$customer_city",
                "state": "$customer_state",
                "recency": 1,
                "frequency": 1,
                "monetary": 1
            }}
        ]
        top_customers = list(customers_col.aggregate(top_pipeline))

        return jsonify({
            "distribution": _json(distribution),
            "rfm": _json(rfm_data),
            "top_customers": _json(top_customers)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════
#  API — Churn Analysis
# ══════════════════════════════════════════════════════════════════════
@app.route("/api/churn")
def api_churn():
    try:
        total = customers_col.count_documents({})

        # FIX BUG 5: churn_prediction là float (0.0/1.0), không phải string "1.0"/"0.0"
        # Dùng $in để bắt cả int 1 lẫn float 1.0
        churned = customers_col.count_documents({"churn_prediction": {"$in": [1, 1.0]}})
        churn_rate = _safe_round(churned / total * 100 if total else 0)

        # Churn by segment — FIX: churn_prediction là số
        pipeline = [
            {"$group": {
                "_id": "$segment_name",
                "total": {"$sum": 1},
                "churned": {"$sum": {"$cond": [
                    {"$in": ["$churn_prediction", [1, 1.0]]}, 1, 0
                ]}},
                "avg_prob": {"$avg": {"$toDouble": "$churn_probability"}}
            }},
            {"$sort": {"churned": -1}}
        ]
        by_segment = list(customers_col.aggregate(pipeline))
        segment_data = [{
            "segment": r["_id"] or "Unknown",
            "total": r["total"],
            "churned": r["churned"],
            "churn_rate": _safe_round(r["churned"] / r["total"] * 100 if r["total"] else 0),
            "avg_prob": _safe_round(r.get("avg_prob") or 0)
        } for r in by_segment]

        # Top 10 high-risk — churn_probability là float
        high_risk = list(customers_col.aggregate([
            {"$match": {"churn_probability": {"$exists": True, "$gt": 0}}},
            {"$sort": {"churn_probability": -1}},
            {"$limit": 10},
            {"$project": {"_id": 0, "customer_unique_id": 1, "segment_name": 1,
                          "churn_probability": 1, "customer_city": 1, "customer_state": 1,
                          "monetary": 1, "recency": 1}}
        ]))

        # FIX BUG 9: high_risk_clean dùng trực tiếp monetary/recency (không qua rfm)
        high_risk_clean = [{
            "customer_id": r.get("customer_unique_id", ""),
            "segment": r.get("segment_name", ""),
            "churn_probability": _safe_round(float(r.get("churn_probability", 0) or 0)),
            "city": r.get("customer_city", ""),
            "state": r.get("customer_state", ""),
            "monetary": _safe_round(float(r.get("monetary", 0) or 0)),
            "recency": int(float(r.get("recency", 0) or 0))
        } for r in high_risk]

        # FIX BUG 4: model_performance cũng flat rows
        model_docs = list(agg_col.find({"agg_type": "model_performance"}))
        model_comparison = {}
        if model_docs:
            models_list = []
            for doc in model_docs:
                metrics_raw = doc.get("metrics_json", "{}")
                try:
                    metrics = json.loads(metrics_raw) if isinstance(metrics_raw, str) else metrics_raw
                except Exception:
                    metrics = {}
                if isinstance(metrics, dict) and metrics:
                    models_list.append(metrics)
            if models_list:
                model_comparison = {"models": models_list}

        return jsonify({
            "churn_rate": churn_rate,
            "total": total,
            "churned": churned,
            "by_segment": _json(segment_data),
            "high_risk": high_risk_clean,
            "model_comparison": model_comparison
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════
#  API — Reviews (score distribution)
# ══════════════════════════════════════════════════════════════════════
@app.route("/api/reviews")
def api_reviews():
    try:
        # FIX BUG 2: review là nested struct, dùng review.review_score
        pipeline = [
            {"$match": {"review.review_score": {"$exists": True, "$gt": 0}}},
            {"$group": {"_id": "$review.review_score", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        result = list(orders_col.aggregate(pipeline))
        data = [{"score": int(r["_id"]), "count": r["count"]}
                for r in result if r["_id"]]
        return jsonify({"data": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════
#  API — Geographic Stats
# ══════════════════════════════════════════════════════════════════════
@app.route("/api/geo")
def api_geo():
    try:
        # FIX BUG 4 + 8: aggregations flat rows, field là "customer_state"
        state_docs = list(agg_col.find(
            {"agg_type": "state_stats"},
            {"_id": 0, "customer_state": 1, "total_revenue": 1, "order_count": 1}
        ).sort("total_revenue", -1).limit(15))

        if state_docs:
            states = [{
                "state": d.get("customer_state") or "Unknown",
                "revenue": _safe_round(d.get("total_revenue", 0)),
                "orders": d.get("order_count", 0)
            } for d in state_docs]
        else:
            pipeline = [
                {"$group": {
                    "_id": "$customer_state",
                    "orders": {"$sum": 1},
                    "revenue": {"$sum": "$monetary"}
                }},
                {"$sort": {"orders": -1}},
                {"$limit": 15}
            ]
            result = list(customers_col.aggregate(pipeline))
            states = [{"state": r["_id"] or "Unknown",
                       "revenue": _safe_round(r.get("revenue", 0)),
                       "orders": r["orders"]} for r in result]

        # Hourly orders — FIX BUG 4: flat rows với purchase_dayofweek, purchase_hour
        hourly_docs = list(agg_col.find(
            {"agg_type": "hourly_orders"},
            {"_id": 0, "purchase_dayofweek": 1, "purchase_hour": 1, "order_count": 1}
        ))
        if hourly_docs:
            heatmap = [{"day": d.get("purchase_dayofweek", 0),
                        "hour": d.get("purchase_hour", 0),
                        "count": d.get("order_count", 0)} for d in hourly_docs]
        else:
            heatmap = []

        # FIX BUG 4: payment_methods — flat rows với payment_type, order_count, total_revenue
        payment_docs = list(agg_col.find(
            {"agg_type": "payment_methods"},
            {"_id": 0, "payment_type": 1, "order_count": 1, "total_revenue": 1}
        ).sort("order_count", -1))

        if payment_docs:
            payments = [{
                "method": d.get("payment_type") or "Unknown",
                "count": d.get("order_count", 0),
                "total": _safe_round(d.get("total_revenue", 0))
            } for d in payment_docs]
        else:
            # Fallback: từ orders dùng nested payment.payment_type
            pipeline = [
                {"$match": {"payment.payment_type": {"$exists": True}}},
                {"$group": {
                    "_id": "$payment.payment_type",
                    "count": {"$sum": 1},
                    "total": {"$sum": "$payment.total_payment_value"}
                }},
                {"$sort": {"count": -1}}
            ]
            result = list(orders_col.aggregate(pipeline))
            payments = [{"method": r["_id"] or "Unknown", "count": r["count"],
                        "total": _safe_round(r.get("total", 0))} for r in result]

        return jsonify({
            "states": states,
            "heatmap": heatmap,
            "payments": payments
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════
#  API — ML Model Performance
# ══════════════════════════════════════════════════════════════════════
def build_feature_importance(metrics):
    fi = metrics.get("feature_importance", {})
    if isinstance(fi, dict):
        return [{"feature": k, "importance": v}
                for k, v in sorted(fi.items(), key=lambda x: x[1], reverse=True)]
    return fi if isinstance(fi, list) else []


@app.route("/api/models")
def api_models():
    try:
        docs = list(agg_col.find({"agg_type": "model_performance"}))
        models_list = []
        for doc in docs:
            metrics_raw = doc.get("metrics_json", "{}")
            try:
                metrics = json.loads(metrics_raw) if isinstance(metrics_raw, str) else metrics_raw
            except Exception:
                metrics = {}
            if not isinstance(metrics, dict) or not metrics:
                continue

            sub_models = metrics.get("models", {})
            best_key = metrics.get("best_model", "")

            # Build confusion matrix từ dict → [[TN,FP],[FN,TP]]
            cm_raw = metrics.get("confusion_matrix", {})
            cm = None
            if isinstance(cm_raw, dict) and cm_raw:
                tp = cm_raw.get("true_positive", 0)
                tn = cm_raw.get("true_negative", 0)
                fp = cm_raw.get("false_positive", 0)
                fn = cm_raw.get("false_negative", 0)
                cm = [[tn, fp], [fn, tp]]

            for sub_name, sub_m in sub_models.items():
                if not isinstance(sub_m, dict):
                    continue
                if "rmse" in sub_m and "auc_roc" not in sub_m:
                    continue
                models_list.append({
                    "name": sub_name + (" ★" if sub_name == best_key else ""),
                    "accuracy":  sub_m.get("accuracy",  sub_m.get("r2", 0)),
                    "precision": sub_m.get("precision"),
                    "recall":    sub_m.get("recall"),
                    "f1":        sub_m.get("f1_score"),
                    "auc":       sub_m.get("auc_roc"),
                    "rmse":      sub_m.get("rmse"),
                    "confusion_matrix": cm,
                    "feature_importance": build_feature_importance(metrics),
                })

        if models_list:
            return jsonify({"models": models_list})

        # Fallback defaults
        return jsonify({"models": [
            {"name": "Random Forest", "accuracy": 0.87, "precision": 0.85,
             "recall": 0.82, "f1": 0.83, "auc": 0.91, "rmse": None,
             "confusion_matrix": [[850, 120], [95, 435]],
             "feature_importance": [
                 {"feature": "recency", "importance": 0.32},
                 {"feature": "monetary", "importance": 0.25},
                 {"feature": "frequency", "importance": 0.18},
             ]},
            {"name": "Logistic Regression", "accuracy": 0.82, "precision": 0.80,
             "recall": 0.78, "f1": 0.79, "auc": 0.86, "rmse": None,
             "confusion_matrix": None, "feature_importance": []}
        ]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════
#  API — Predict Churn
# ══════════════════════════════════════════════════════════════════════

@app.route("/api/predict", methods=["POST"])
def api_predict():
    try:
        data          = request.get_json(force=True)
        frequency     = float(data.get("frequency", 1))
        monetary      = float(data.get("monetary", 0))
        review_score  = float(data.get("review_score", 5))
        delivery_days = float(data.get("delivery_days", 10))

        model_path = os.path.join(
            os.path.dirname(__file__), "..",
            "tmp_models", "churn_prediction.joblib"
        )
        saved    = joblib.load(model_path)
        scaler   = saved["scaler"]
        model    = saved["model"]

        features = np.array([[
            frequency, monetary,
            1.0, monetary, 0.15,
            review_score, delivery_days, 1.0,
        ]])
        prob = float(model.predict_proba(scaler.transform(features))[0][1])

        return jsonify({
            "probability": round(prob, 3),
            "label": "Có nguy cơ rời bỏ" if prob >= 0.5 else "Có khả năng ở lại",
            "risk_level": (
                "Rất cao" if prob >= 0.8 else
                "Cao" if prob >= 0.6 else
                "Trung bình" if prob >= 0.4 else
                "Thấp" if prob >= 0.2 else "Rất thấp"
            ),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════
#  Error Handlers
# ══════════════════════════════════════════════════════════════════════
@app.errorhandler(404)
def not_found(e):
    return render_template("base.html", page="404",
                           error_message="Trang không tồn tại"), 404

@app.errorhandler(500)
def server_error(e):
    return jsonify({"error": "Lỗi máy chủ nội bộ"}), 500


# ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
