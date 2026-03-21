# backend/ml_engine.py
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
import joblib
import os

class MLEngine:
    def __init__(self):
        self.encoder = LabelEncoder()
        self.model = None
        self.is_trained = False

    def train_churn_model(self, df):
        """
        Trains a simple Churn Prediction model using RandomForest.
        For demo purposes, we'll create a synthetic 'churn' label based on engagement.
        """
        if df.empty or len(df) < 10:
            return False
            
        try:
            # 1. Feature Engineering
            # Use numeric features and encode categorical ones
            ml_df = df.copy()
            
            # Target variable (synthetic): Churn if satisfaction < 4 or return rate > 0.5
            # Or if frequency is 'Rarely'
            if 'customer_satisfaction' in ml_df.columns:
                ml_df['churn'] = ((ml_df['customer_satisfaction'] < 4) | (ml_df['return_rate'] > 0.4)).astype(int)
            else:
                # Fallback synthetic target
                ml_df['churn'] = (ml_df['purchase_amount'] < ml_df['purchase_amount'].mean() * 0.5).astype(int)

            features = ['age', 'purchase_amount', 'product_rating', 'return_rate', 'customer_satisfaction']
            # Filter only existing columns
            features = [f for f in features if f in ml_df.columns]
            
            if not features:
                return False

            X = ml_df[features].fillna(0)
            y = ml_df['churn']

            self.model = RandomForestClassifier(n_estimators=50, random_state=42)
            self.model.fit(X, y)
            self.is_trained = True
            self.features = features
            return True
        except Exception as e:
            print(f"ML Training Error: {e}")
            return False

    def predict_churn(self, df):
        """Returns churn probability and risk level for the dataset."""
        if not self.is_trained or df.empty or len(df) < 5:
            return {
                "status": "fallback",
                "risk": "Unknown",
                "message": "Churn prediction unavailable due to limited or insufficient data points.",
                "reason": "Model requires at least 10 historical records for accurate classification.",
                "suggestion": "Try broadening your query range or filters to include more user data."
            }
        
        try:
            X = df[self.features].fillna(0)
            preds = self.model.predict(X)
            churn_count = sum(preds)
            churn_rate = (churn_count / len(df)) * 100
            
            risk = "High" if churn_rate > 30 else ("Medium" if churn_rate > 15 else "Low")
            return {
                "status": "success",
                "risk": risk,
                "message": f"{risk} Risk level: {churn_rate:.1f}% users likely to churn."
            }
        except Exception as e:
            return {
                "status": "fallback",
                "risk": "Error",
                "message": "Prediction module encountered a processing issue.",
                "reason": str(e),
                "suggestion": "Simplify categorical filters to improve model stability."
            }

    def get_recommendations(self, df):
        """Simple Recommendation logic based on spending behavior."""
        if df.empty:
            return []
            
        recs = []
        avg_spend = df['purchase_amount'].mean() if 'purchase_amount' in df.columns else 0
        
        # User Logic: spending > 300 -> Luxury Goods
        if avg_spend > 300:
            recs.append("💎 **Recommendation**: These customers have high spending potential. Recommend **Luxury Goods** and premium tiers.")
        elif avg_spend > 150:
            recs.append("📦 **Recommendation**: Moderate spenders. Recommend **Bundle Deals** and seasonal collections.")
        else:
            recs.append("🏷️ **Recommendation**: Budget-conscious segment. Recommend **Value Essentials** and discounted items.")
            
        return recs

    def detect_anomalies(self, df):
        """Detects unusual purchase behavior (extreme values)."""
        if 'purchase_amount' not in df.columns or df.empty:
            return []
            
        anomalies = []
        amounts = df['purchase_amount'].dropna()
        if len(amounts) < 1: return []
        
        avg = amounts.mean()
        
        # User Rule: if purchase > avg * 3 -> Unusual behavior
        limit = avg * 3
        outliers = df[df['purchase_amount'] > limit]
        
        if not outliers.empty:
            count = len(outliers)
            anomalies.append(f"🚩 **Anomaly Detection**: {count} cases of **Unusual behavior** detected. Purchases exceeded 3x the average threshold (Limit: ₹{limit:,.2f}).")
            
        return anomalies

def get_ml_insights(df):
    """Entry point for generating ML-driven insights."""
    engine = MLEngine()
    
    # Use a bigger slice of global data for training if possible? 
    # For now, we'll try to train on the provided df if it's large enough.
    # In a real app, you'd have a pre-trained model loaded.
    engine.train_churn_model(df)
    
    return {
        "churn_prediction": engine.predict_churn(df),
        "recommendations": engine.get_recommendations(df),
        "anomalies": engine.detect_anomalies(df)
    }
