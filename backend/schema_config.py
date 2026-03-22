# backend/schema_config.py

# Centralized Schema Abstraction Layer
SCHEMA = {
    "table_name": "ecommerce_behavior",
    "metric_column": "purchase_amount",
    "category_column": "purchase_category",
    "location_column": "location",
    "date_column": "time_of_purchase",
    "discount_column": "discount_used",
    "numeric_columns": [
        "purchase_amount", "age", "product_rating", "time_spent_on_product_researchhours",
        "return_rate", "customer_satisfaction", "time_to_decision"
    ],
    "categorical_columns": [
        "income_level", "frequency_of_purchase", "discount_sensitivity", "brand_loyalty",
        "social_media_influence", "engagement_with_ads", "purchase_intent", "discount_used",
        "customer_loyalty_program_member", "gender", "marital_status", "education_level",
        "occupation", "location", "purchase_category", "purchase_channel",
        "device_used_for_shopping", "payment_method", "shipping_preference"
    ],
    "all_columns": [
        "customer_id", "age", "gender", "income_level", "marital_status", "education_level",
        "occupation", "location", "purchase_category", "purchase_amount",
        "frequency_of_purchase", "purchase_channel", "brand_loyalty", "product_rating",
        "time_spent_on_product_researchhours", "social_media_influence", "discount_sensitivity",
        "return_rate", "customer_satisfaction", "engagement_with_ads", "device_used_for_shopping",
        "payment_method", "time_of_purchase", "discount_used", "customer_loyalty_program_member",
        "purchase_intent", "shipping_preference", "time_to_decision"
    ]
}

def get_metric_col(schema_dict=None):
    return (schema_dict or SCHEMA).get("metric_column", "purchase_amount")

def get_category_col(schema_dict=None):
    return (schema_dict or SCHEMA).get("category_column", "purchase_category")

def get_location_col(schema_dict=None):
    return (schema_dict or SCHEMA).get("location_column", "location")

def get_date_col(schema_dict=None):
    return (schema_dict or SCHEMA).get("date_column", "time_of_purchase")

def get_table_name(schema_dict=None):
    """
    Returns RUNTIME_TABLE if present, otherwise falls back to hardcoded SCHEMA.
    """
    if RUNTIME_TABLE:
        return RUNTIME_TABLE
    return (schema_dict or SCHEMA).get("table_name", "ecommerce_behavior")

# --- Dynamic Schema Detection Layer ---
import logging
logger = logging.getLogger(__name__)

RUNTIME_SCHEMA = None
RUNTIME_TABLE = None # Current active table name for SQL generation

def detect_schema(df):
    """
    Dynamically extracts dataset structure at runtime.
    """
    if df is None or df.empty:
        return None
        
    try:
        import pandas as pd
        # Final Defensive Normalization: strip and lowercase columns
        df.columns = [col.strip().lower() for col in df.columns]
        
        schema = {
            "columns": list(df.columns),
            "numeric": list(df.select_dtypes(include=['number']).columns),
            "categorical": list(df.select_dtypes(include=['object', 'category']).columns),
            "datetime": list(df.select_dtypes(include=['datetime', 'datetime64[ns]']).columns)
        }
        logger.info(f"Dynamically Detected Schema: {schema}")
        return schema
    except Exception as e:
        logger.error(f"Schema Detection Error: {str(e)}")
        return None

def set_runtime_schema(df, table_name=None):
    global RUNTIME_SCHEMA, RUNTIME_MAPPING, RUNTIME_TABLE
    RUNTIME_SCHEMA = detect_schema(df)
    if table_name:
        # Sanitize and GENERATE A UNIQUE table name (NEVER assume/guess)
        import os, time, re
        clean_name = os.path.splitext(table_name)[0].lower().replace(" ", "_").strip()
        # Strictly alphanumeric for SQL naming safety
        clean_name = re.sub(r'[^a-z0-9_]', '', clean_name)
        timestamp = int(time.time() % 1000000)
        RUNTIME_TABLE = f"{clean_name}_{timestamp}"
        logger.info(f"Target UNIQUE table generated: {RUNTIME_TABLE}")
        
    if RUNTIME_SCHEMA:
        RUNTIME_MAPPING = map_columns(RUNTIME_SCHEMA)

def get_active_schema():
    """
    Returns RUNTIME_SCHEMA if present, otherwise falls back to hardcoded SCHEMA.
    """
    return RUNTIME_SCHEMA if RUNTIME_SCHEMA else SCHEMA

# --- Column Mapping Engine ---
RUNTIME_MAPPING = None
RUNTIME_OVERRIDE = None

def set_manual_override(mapping):
    """
    Sets a manual override for the column mapping.
    Ensures that the requested columns exist in the active schema.
    Returns True if successful.
    """
    global RUNTIME_OVERRIDE
    active_schema = get_active_schema()
    cols = active_schema.get("columns", []) if active_schema else []
    
    if not mapping:
        RUNTIME_OVERRIDE = None
        return True
        
    valid_mapping = {}
    for role in ["measure", "group", "discount"]:
        col = mapping.get(role)
        # Allow None for discount if requested, otherwise check column presence
        if col in cols or (role == "discount" and col is None):
            valid_mapping[role] = col
            
    if valid_mapping:
        RUNTIME_OVERRIDE = valid_mapping
        logger.info(f"User manual override applied: {RUNTIME_OVERRIDE}")
        return True
    return False

# --- ML-Based Column Classification Layer (Optional) ---
try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.linear_model import LogisticRegression
    import numpy as np

    class MLColumnClassifier:
        """
        Uses TF-IDF + Logistic Regression to predict column roles (measure, group, discount).
        Optional intelligence layer for ambiguous datasets.
        """
        def __init__(self):
            self.vectorizer = TfidfVectorizer()
            self.clf = LogisticRegression()
            self._train()

        def _train(self):
            # Training Data: Common business, education, and marketing terms mapped to roles
            data = {
                "revenue": "measure", "sales": "measure", "amount": "measure", "price": "measure",
                "spending": "measure", "cost": "measure", "profit": "measure", "income": "measure",
                "score": "measure", "valuation": "measure", "total": "measure", "value": "measure",
                "count": "measure", "orders": "measure", "transactions": "measure", "earnings": "measure",
                "category": "group", "type": "group", "product": "group", "product_name": "group",
                "segment": "group", "industry": "group", "status": "group", "brand": "group",
                "department": "group", "role": "group", "class": "group", "subject": "group",
                "discount": "discount", "coupon": "discount", "promo": "discount", "is_member": "discount",
                "is_active": "discount", "has_coupon": "discount", "passed": "discount", "flag": "discount"
            }
            X_train = list(data.keys())
            y_train = list(data.values())
            X_vec = self.vectorizer.fit_transform(X_train)
            self.clf.fit(X_vec, y_train)
            logger.info("ML Column Classifier trained successfully.")

        def predict(self, col_list, current_mapping):
            predictions = {}
            unmapped_roles = [r for r in ["measure", "group", "discount"] if r not in current_mapping]
            if not unmapped_roles: return {}

            for col in col_list:
                clean_col = "".join(c for c in col.lower() if c.isalnum() or c == '_')
                vec = self.vectorizer.transform([clean_col])
                probs = self.clf.predict_proba(vec)[0]
                classes = self.clf.classes_
                best_idx = np.argmax(probs)
                if probs[best_idx] > 0.4:
                    role = classes[best_idx]
                    if role in unmapped_roles:
                        predictions[role] = col
                        unmapped_roles.remove(role)
                        logger.info(f"ML Predicted role for '{col}': {role} (confidence: {probs[best_idx]:.2f})")
            return predictions

    # Initialize Singleton Classifier
    classifier = MLColumnClassifier()
except Exception as e:
    logger.warning(f"ML Column Classifier could not be initialized: {str(e)}")
    classifier = None

def map_columns(schema_dict):
    """
    Dynamically identifies key columns (measure, group, discount) based on semantic meaning.
    Includes robust fallback logic to ensure mapping never fails for structured data.
    """
    if not schema_dict or "columns" not in schema_dict:
        return {}
        
    mapping = {}
    cols = schema_dict["columns"]
    numeric_cols = schema_dict.get("numeric", [])
    categorical_cols = schema_dict.get("categorical", [])
    
    # --- PHASE 1: Semantic Mapping ---
    for col in cols:
        c = col.lower()
        
        # 💰 Measure (Sales/Revenue/Amount)
        if "measure" not in mapping:
            if any(x in c for x in ["amount", "sales", "revenue", "price", "spend", "earning", "cost"]):
                mapping["measure"] = col
                
        # 📊 Grouping (Category/Type)
        if "group" not in mapping:
             if any(x in c for x in ["category", "type", "product", "segment", "group", "class"]):
                mapping["group"] = col
                
        # 🎯 Discount / Flag
        if "discount" not in mapping:
             if any(x in c for x in ["discount", "coupon", "promo", "is_member", "is_applied"]):
                mapping["discount"] = col

    # --- PHASE 1.5: ML Intelligence Layer ---
    if classifier:
        try:
            unmapped_cols = [c for c in cols if c not in mapping.values()]
            ml_predictions = classifier.predict(unmapped_cols, mapping)
            for role, col in ml_predictions.items():
                # Safety: Cross-verify data types for ML predictions
                if role == "measure" and col not in numeric_cols: continue
                if role == "group" and col not in categorical_cols: continue
                mapping[role] = col
        except Exception as e:
            logger.error(f"ML Mapping Phase Error: {str(e)}")

    # --- PHASE 2: Robust Fallback ---
            if "discount" in c or "coupon" in c or "promo" in c:
                mapping["discount"] = col

    # --- PHASE 2: Robust Fallback (If Step 1 fails) ---
    fallbacks_applied = []
    
    # 1. Measure Fallback -> First Numeric Column
    if "measure" not in mapping or mapping["measure"] not in cols:
        if numeric_cols:
            mapping["measure"] = numeric_cols[0]
            fallbacks_applied.append(f"measure -> {mapping['measure']} (first numeric)")
        else:
            # Absolute hard fallback to legacy
            mapping["measure"] = SCHEMA.get("metric_column", "purchase_amount")
            fallbacks_applied.append(f"measure -> {mapping['measure']} (legacy)")

    # 2. Group Fallback -> First Categorical Column
    if "group" not in mapping or mapping["group"] not in cols:
        if categorical_cols:
            mapping["group"] = categorical_cols[0]
            fallbacks_applied.append(f"group -> {mapping['group']} (first categorical)")
        else:
             # Absolute hard fallback to legacy
            mapping["group"] = SCHEMA.get("category_column", "purchase_category")
            fallbacks_applied.append(f"group -> {mapping['group']} (legacy)")
            
    # 3. Discount Fallback -> Optional/None
    if "discount" not in mapping or mapping["discount"] not in cols:
        mapping["discount"] = None # Safe default for unknown datasets
        fallbacks_applied.append("discount -> None")

    if fallbacks_applied:
        logger.info(f"Robust Fallback Applied: {', '.join(fallbacks_applied)}")
        
    logger.info(f"Final Column Mapping: {mapping}")
    return mapping

# --- Role Synonym Dictionary ---
ROLE_SYNONYMS = {
    "sales": "measure",
    "revenue": "measure",
    "spending": "measure",
    "price": "measure",
    "amount": "measure",
    "category": "group",
    "type": "group",
    "product": "group",
    "segment": "group",
    "class": "group",
    "discount": "discount",
    "coupon": "discount",
    "promo": "discount"
}

def refine_mapping_with_synonyms(query, base_mapping):
    """
    Refines column mapping by matching query terms to semantic roles.
    Allows user to specify intent using synonyms (e.g., 'revenue' -> measure).
    """
    if not query or not base_mapping:
        return base_mapping
        
    q_tokens = query.lower().split()
    refined = base_mapping.copy()
    mapped_roles = []
    
    for token in q_tokens:
        # Clean token from punctuation
        clean_token = "".join(c for c in token if c.isalnum())
        if clean_token in ROLE_SYNONYMS:
            role = ROLE_SYNONYMS[clean_token]
            if role in base_mapping:
                # We confirm the role intent from the query
                mapped_roles.append(f"{clean_token} -> {role}")
                
    if mapped_roles:
        logger.info(f"Query tokens mapped to roles: {', '.join(mapped_roles)}")
        
    return refined

def get_active_mapping():
    """
    Returns mapping with precedence: Override > Runtime > Default
    """
    if RUNTIME_OVERRIDE:
        return RUNTIME_OVERRIDE
        
    if RUNTIME_MAPPING:
        return RUNTIME_MAPPING
        
    # Default legacy mapping
    return {
        "measure": SCHEMA.get("metric_column", "purchase_amount"),
        "group": SCHEMA.get("category_column", "purchase_category"),
        "discount": SCHEMA.get("discount_column", "discount_used")
    }
