import pymongo
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
import joblib

client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client['olist_dw']
df = pd.DataFrame(list(db['customers'].find({}, {
    '_id': 0, 'recency': 1, 'frequency': 1, 'monetary': 1, 
    'avg_review_score': 1, 'avg_delivery_days': 1, 'churn_probability': 1
})))

if not df.empty:
    df.fillna(0, inplace=True)
    X = df[['recency', 'frequency', 'monetary', 'avg_review_score', 'avg_delivery_days']]
    y = (df['churn_probability'] > 0.5).astype(int)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = RandomForestClassifier(max_depth=5, n_estimators=50, random_state=42)
    model.fit(X_scaled, y)

    joblib.dump({'scaler': scaler, 'model': model}, 'tmp_models/churn_prediction.joblib')
    print('Model trained and saved with 5 features.')
else:
    print('No data found.')
