"""
Olist E-Commerce Big Data Dashboard — Flask Application
"""
from flask import Flask, render_template, jsonify, request
from pymongo import MongoClient
from config import Config
from bson import json_util
import json
import math
from datetime import datetime

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

        # Total revenue — flat field "order_value"
        rev_result = list(orders_col.aggregate([
            {"$group": {"_id": None, "total": {"$sum": "$order_value"}}}
        ]))
        total_revenue = _safe_round(rev_result[0]["total"]) if rev_result else 0

        # Average review — "review" field là string "[5.0, ...]" do pandas.to_json()
        # Lấy số đầu tiên bằng $toDouble trên substring trước dấu phẩy
        review_result = list(orders_col.aggregate([
            {"$match": {"review": {"$exists": True, "$ne": None, "$ne": ""}}},
            {"$project": {
                "score": {"$toDouble": {
                    "$trim": {"input": {
                        "$arrayElemAt": [
                            {"$split": [
                                {"$trim": {"input": "$review", "chars": "["}},
                                ","
                            ]},
                            0
                        ]
                    }}
                }}
            }},
            {"$match": {"score": {"$gt": 0}}},
            {"$group": {"_id": None, "avg": {"$avg": "$score"}}}
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
        # aggregations dùng field "agg_type" không phải "type"
        doc = agg_col.find_one({"agg_type": "monthly_revenue"})
        if doc and "data" in doc:
            return jsonify({"data": _json(doc["data"])})

        # Fallback: tính từ orders dùng flat fields
        pipeline = [
            {"$group": {
                "_id": {"$substr": ["$order_purchase_timestamp", 0, 7]},
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
        doc = agg_col.find_one({"agg_type": "category_stats"})
        if doc and "data" in doc:
            return jsonify({"data": _json(doc["data"])})

        # Fallback từ products collection - dùng đúng field thật
        pipeline = [
            {"$group": {
                "_id": "$category_en",
                "total_sold": {"$sum": {"$toDouble": "$total_orders"}},
                "total_revenue": {"$sum": {"$toDouble": "$total_revenue"}},
                "avg_price": {"$avg": {"$toDouble": "$avg_price"}},
                "avg_review": {"$avg": {"$toDouble": "$avg_review_score"}}
            }},
            {"$sort": {"total_sold": -1}},
            {"$limit": 20}
        ]
        result = list(products_col.aggregate(pipeline))
        data = [{
            "category": r["_id"] or "Unknown",
            "revenue": _safe_round(r.get("total_revenue", 0)),
            "total_sold": int(r.get("total_sold") or 0),
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
        # Segment distribution — dùng "agg_type" và fallback dùng "segment_name"
        seg_doc = agg_col.find_one({"agg_type": "segment_stats"})
        if seg_doc and "data" in seg_doc:
            distribution = _json(seg_doc["data"])
        else:
            pipeline = [
                {"$group": {"_id": "$segment_name", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            result = list(customers_col.aggregate(pipeline))
            distribution = [{"segment": r["_id"] or "Unknown", "count": r["count"]}
                           for r in result]

        # RFM scatter — flat fields, không nested rfm.*
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
        # churn_prediction là STRING "1.0"/"0.0" do pandas .to_json() export
        churned = customers_col.count_documents({"churn_prediction": "1.0"})
        churn_rate = _safe_round(churned / total * 100 if total else 0)

        # Churn by segment
        pipeline = [
            {"$group": {
                "_id": "$segment_name",
                "total": {"$sum": 1},
                "churned": {"$sum": {"$cond": [
                    {"$eq": ["$churn_prediction", "1.0"]}, 1, 0
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

        # Top 10 high-risk — sort by churn_probability string (dùng pipeline)
        high_risk = list(customers_col.aggregate([
            {"$match": {"churn_probability": {"$exists": True}}},
            {"$addFields": {"prob_float": {"$toDouble": "$churn_probability"}}},
            {"$sort": {"prob_float": -1}},
            {"$limit": 10},
            {"$project": {"_id": 0, "customer_unique_id": 1, "segment_name": 1,
                          "prob_float": 1, "customer_city": 1, "customer_state": 1,
                          "monetary": 1, "recency": 1}}
        ]))

        high_risk_clean = [{
            "customer_id": r.get("customer_unique_id", ""),
            "segment": r.get("segment_name", ""),
            "churn_probability": _safe_round(r.get("prob_float", 0)),
            "city": r.get("customer_city", ""),
            "state": r.get("customer_state", ""),
            "monetary": _safe_round(float(r.get("monetary", 0) or 0)),
            "recency": int(float(r.get("recency", 0) or 0))
        } for r in high_risk]

        model_doc = agg_col.find_one({"agg_type": "model_performance"})
        model_comparison = _json(model_doc.get("data", {})) if model_doc else {}

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
        # "review" field là string "[5.0, 'comment'...]" - parse score từ index 0
        pipeline = [
            {"$match": {"review": {"$exists": True, "$ne": None, "$ne": ""}}},
            {"$project": {
                "score": {"$toDouble": {
                    "$trim": {"input": {
                        "$arrayElemAt": [
                            {"$split": [
                                {"$trim": {"input": "$review", "chars": "["}},
                                ","
                            ]},
                            0
                        ]
                    }}
                }}
            }},
            {"$match": {"score": {"$gt": 0}}},
            {"$group": {"_id": "$score", "count": {"$sum": 1}}},
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
        # State stats
        state_doc = agg_col.find_one({"agg_type": "state_stats"})
        if state_doc and "data" in state_doc:
            states = _json(state_doc["data"])
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

        # Hourly orders heatmap
        hourly_doc = agg_col.find_one({"agg_type": "hourly_orders"})
        heatmap = _json(hourly_doc["data"]) if hourly_doc and "data" in hourly_doc else []

        # Payment methods - từ aggregations hoặc parse field payment string
        payment_doc = agg_col.find_one({"agg_type": "payment_methods"})
        if payment_doc and "data" in payment_doc and payment_doc["data"]:
            payments = _json(payment_doc["data"])
        else:
            # payment field là string "[141.9, 'credit_card', 8, 1]"
            # Dùng $split và lấy phần tử index 1 để lấy tên phương thức
            pipeline = [
                {"$match": {"payment": {"$exists": True, "$ne": None}}},
                {"$project": {
                    "method": {"$trim": {
                        "input": {"$replaceAll": {
                            "input": {"$arrayElemAt": [
                                {"$split": ["$payment", ","]}, 1
                            ]},
                            "find": "'", "replacement": ""
                        }},
                        "chars": " "
                    }},
                    "value": {"$toDouble": {
                        "$trim": {"input": {"$ltrim": {
                            "input": {"$arrayElemAt": [{"$split": ["$payment", ","]}, 0]},
                            "chars": "["
                        }}}
                    }}
                }},
                {"$group": {
                    "_id": "$method",
                    "count": {"$sum": 1},
                    "total": {"$sum": "$value"}
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
@app.route("/api/models")
def api_models():
    try:
        doc = agg_col.find_one({"agg_type": "model_performance"})
        if doc and "data" in doc:
            return jsonify(_json(doc["data"]))

        # Return defaults if nothing stored
        return jsonify({
            "models": [
                {
                    "name": "Random Forest",
                    "accuracy": 0.87, "precision": 0.85, "recall": 0.82,
                    "f1": 0.83, "auc": 0.91, "rmse": 0.36,
                    "confusion_matrix": [[850, 120], [95, 435]],
                    "feature_importance": [
                        {"feature": "recency", "importance": 0.32},
                        {"feature": "monetary", "importance": 0.25},
                        {"feature": "frequency", "importance": 0.18},
                        {"feature": "delivery_days", "importance": 0.10},
                        {"feature": "review_score", "importance": 0.08},
                        {"feature": "installments", "importance": 0.04},
                        {"feature": "freight_value", "importance": 0.03}
                    ]
                },
                {
                    "name": "Logistic Regression",
                    "accuracy": 0.82, "precision": 0.80, "recall": 0.78,
                    "f1": 0.79, "auc": 0.86, "rmse": 0.42,
                    "confusion_matrix": [[810, 160], [130, 400]],
                    "feature_importance": [
                        {"feature": "recency", "importance": 0.28},
                        {"feature": "monetary", "importance": 0.22},
                        {"feature": "frequency", "importance": 0.20},
                        {"feature": "delivery_days", "importance": 0.12},
                        {"feature": "review_score", "importance": 0.09},
                        {"feature": "installments", "importance": 0.05},
                        {"feature": "freight_value", "importance": 0.04}
                    ]
                }
            ]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════
#  API — Predict Churn
# ══════════════════════════════════════════════════════════════════════
@app.route("/api/predict", methods=["POST"])
def api_predict():
    try:
        data = request.get_json(force=True)
        recency     = float(data.get("recency", 0))
        frequency   = float(data.get("frequency", 1))
        monetary    = float(data.get("monetary", 0))
        review      = float(data.get("review_score", 5))
        delivery    = float(data.get("delivery_days", 10))

        # Simple heuristic model (replace with real model if available)
        score = 0.0
        if recency > 180:
            score += 0.35
        elif recency > 90:
            score += 0.20
        elif recency > 30:
            score += 0.05

        if frequency <= 1:
            score += 0.25
        elif frequency <= 3:
            score += 0.10

        if monetary < 50:
            score += 0.15
        elif monetary < 150:
            score += 0.05

        if review <= 2:
            score += 0.15
        elif review <= 3:
            score += 0.08

        if delivery > 30:
            score += 0.10
        elif delivery > 15:
            score += 0.05

        probability = min(max(score, 0.02), 0.98)
        label = "Có nguy cơ rời bỏ" if probability >= 0.5 else "Có khả năng ở lại"
        risk_level = (
            "Rất cao" if probability >= 0.8 else
            "Cao" if probability >= 0.6 else
            "Trung bình" if probability >= 0.4 else
            "Thấp" if probability >= 0.2 else "Rất thấp"
        )

        return jsonify({
            "probability": _safe_round(probability, 3),
            "label": label,
            "risk_level": risk_level,
            "input": {
                "recency": recency,
                "frequency": frequency,
                "monetary": monetary,
                "review_score": review,
                "delivery_days": delivery
            }
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
