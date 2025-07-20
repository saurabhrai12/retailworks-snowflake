"""
Machine Learning Models - Snowpark Application
Description: ML models for customer analytics and sales forecasting
Version: 1.0
Date: 2025-07-19
"""

import pandas as pd
import numpy as np
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, sum, avg, count, max, min, datediff, current_date
from snowflake.ml.modeling.linear_model import LinearRegression
from snowflake.ml.modeling.ensemble import RandomForestRegressor
from snowflake.ml.modeling.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
import joblib
import logging
from datetime import datetime, timedelta
from typing import Dict, Tuple, Optional

class CustomerLifetimeValueModel:
    """Predict Customer Lifetime Value using historical purchase data"""
    
    def __init__(self, session: Session):
        self.session = session
        self.logger = logging.getLogger(__name__)
        self.model = None
        self.scaler = StandardScaler()
        
    def prepare_clv_features(self) -> pd.DataFrame:
        """Prepare features for CLV prediction"""
        try:
            # Get customer transaction data
            clv_data = self.session.sql("""
                SELECT 
                    c.CUSTOMER_ID,
                    c.CUSTOMER_TYPE,
                    c.SEGMENT_NAME,
                    c.ANNUAL_INCOME,
                    c.AGE_GROUP,
                    c.BILLING_COUNTRY,
                    COUNT(DISTINCT sf.ORDER_ID) as total_orders,
                    SUM(sf.LINE_TOTAL) as total_spent,
                    AVG(sf.LINE_TOTAL) as avg_order_value,
                    MAX(d.DATE_ACTUAL) as last_order_date,
                    MIN(d.DATE_ACTUAL) as first_order_date,
                    DATEDIFF('day', MIN(d.DATE_ACTUAL), MAX(d.DATE_ACTUAL)) as customer_lifespan_days,
                    COUNT(DISTINCT p.CATEGORY_NAME) as categories_purchased,
                    DATEDIFF('day', MAX(d.DATE_ACTUAL), CURRENT_DATE()) as days_since_last_order
                FROM RETAILWORKS_DB.ANALYTICS_SCHEMA.CUSTOMER_DIM c
                LEFT JOIN RETAILWORKS_DB.ANALYTICS_SCHEMA.SALES_FACT sf ON c.CUSTOMER_KEY = sf.CUSTOMER_KEY
                LEFT JOIN RETAILWORKS_DB.ANALYTICS_SCHEMA.DATE_DIM d ON sf.ORDER_DATE_KEY = d.DATE_KEY
                LEFT JOIN RETAILWORKS_DB.ANALYTICS_SCHEMA.PRODUCT_DIM p ON sf.PRODUCT_KEY = p.PRODUCT_KEY
                WHERE c.IS_CURRENT = TRUE
                GROUP BY c.CUSTOMER_ID, c.CUSTOMER_TYPE, c.SEGMENT_NAME, c.ANNUAL_INCOME, 
                         c.AGE_GROUP, c.BILLING_COUNTRY
                HAVING total_orders > 0
            """).to_pandas()
            
            # Handle missing values and create additional features
            clv_data['customer_lifespan_days'] = clv_data['customer_lifespan_days'].fillna(0)
            clv_data['days_since_last_order'] = clv_data['days_since_last_order'].fillna(0)
            clv_data['order_frequency'] = clv_data['total_orders'] / (clv_data['customer_lifespan_days'] + 1)
            clv_data['annual_income'] = clv_data['annual_income'].fillna(clv_data['annual_income'].median())
            
            # Encode categorical variables
            le_customer_type = LabelEncoder()
            le_segment = LabelEncoder()
            le_age_group = LabelEncoder()
            le_country = LabelEncoder()
            
            clv_data['customer_type_encoded'] = le_customer_type.fit_transform(clv_data['customer_type'].fillna('Unknown'))
            clv_data['segment_encoded'] = le_segment.fit_transform(clv_data['segment_name'].fillna('Unknown'))
            clv_data['age_group_encoded'] = le_age_group.fit_transform(clv_data['age_group'].fillna('Unknown'))
            clv_data['country_encoded'] = le_country.fit_transform(clv_data['billing_country'].fillna('Unknown'))
            
            # Save encoders for later use
            self.encoders = {
                'customer_type': le_customer_type,
                'segment': le_segment,
                'age_group': le_age_group,
                'country': le_country
            }
            
            self.logger.info(f"Prepared CLV features for {len(clv_data)} customers")
            return clv_data
            
        except Exception as e:
            self.logger.error(f"Error preparing CLV features: {str(e)}")
            raise
    
    def train_clv_model(self, features_df: pd.DataFrame) -> Dict:
        """Train Customer Lifetime Value prediction model"""
        try:
            # Define feature columns
            feature_columns = [
                'total_orders', 'avg_order_value', 'customer_lifespan_days',
                'categories_purchased', 'days_since_last_order', 'order_frequency',
                'annual_income', 'customer_type_encoded', 'segment_encoded',
                'age_group_encoded', 'country_encoded'
            ]
            
            X = features_df[feature_columns]
            y = features_df['total_spent']  # Target: total spent (proxy for CLV)
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
            
            # Scale features
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_test_scaled = self.scaler.transform(X_test)
            
            # Train Random Forest model
            self.model = RandomForestRegressor(n_estimators=100, random_state=42)
            self.model.fit(X_train_scaled, y_train)
            
            # Make predictions
            y_pred = self.model.predict(X_test_scaled)
            
            # Calculate metrics
            mse = mean_squared_error(y_test, y_pred)
            r2 = r2_score(y_test, y_pred)
            
            # Feature importance
            feature_importance = dict(zip(feature_columns, self.model.feature_importances_))
            
            results = {
                'model_type': 'Random Forest',
                'mse': float(mse),
                'r2_score': float(r2),
                'feature_importance': feature_importance,
                'train_samples': len(X_train),
                'test_samples': len(X_test),
                'training_time': datetime.now()
            }
            
            self.logger.info(f"CLV model trained successfully. R² Score: {r2:.4f}")
            return results
            
        except Exception as e:
            self.logger.error(f"Error training CLV model: {str(e)}")
            raise
    
    def predict_clv(self, customer_features: pd.DataFrame) -> pd.DataFrame:
        """Predict CLV for new customers"""
        try:
            if self.model is None:
                raise ValueError("Model not trained. Call train_clv_model first.")
            
            feature_columns = [
                'total_orders', 'avg_order_value', 'customer_lifespan_days',
                'categories_purchased', 'days_since_last_order', 'order_frequency',
                'annual_income', 'customer_type_encoded', 'segment_encoded',
                'age_group_encoded', 'country_encoded'
            ]
            
            X = customer_features[feature_columns]
            X_scaled = self.scaler.transform(X)
            
            predictions = self.model.predict(X_scaled)
            
            # Add predictions to original dataframe
            result_df = customer_features.copy()
            result_df['predicted_clv'] = predictions
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error predicting CLV: {str(e)}")
            raise


class ChurnPredictionModel:
    """Predict customer churn probability"""
    
    def __init__(self, session: Session):
        self.session = session
        self.logger = logging.getLogger(__name__)
        self.model = None
        self.scaler = StandardScaler()
        
    def prepare_churn_features(self, days_threshold: int = 90) -> pd.DataFrame:
        """Prepare features for churn prediction"""
        try:
            # Define churned customers as those who haven't ordered in X days
            churn_data = self.session.sql(f"""
                SELECT 
                    c.CUSTOMER_ID,
                    c.CUSTOMER_TYPE,
                    c.SEGMENT_NAME,
                    c.ANNUAL_INCOME,
                    c.AGE_GROUP,
                    COUNT(DISTINCT sf.ORDER_ID) as total_orders,
                    SUM(sf.LINE_TOTAL) as total_spent,
                    AVG(sf.LINE_TOTAL) as avg_order_value,
                    DATEDIFF('day', MAX(d.DATE_ACTUAL), CURRENT_DATE()) as days_since_last_order,
                    COUNT(DISTINCT p.CATEGORY_NAME) as categories_purchased,
                    STDDEV(sf.LINE_TOTAL) as order_value_std,
                    COUNT(DISTINCT DATE_TRUNC('month', d.DATE_ACTUAL)) as active_months,
                    CASE WHEN DATEDIFF('day', MAX(d.DATE_ACTUAL), CURRENT_DATE()) > {days_threshold} 
                         THEN 1 ELSE 0 END as is_churned
                FROM RETAILWORKS_DB.ANALYTICS_SCHEMA.CUSTOMER_DIM c
                LEFT JOIN RETAILWORKS_DB.ANALYTICS_SCHEMA.SALES_FACT sf ON c.CUSTOMER_KEY = sf.CUSTOMER_KEY
                LEFT JOIN RETAILWORKS_DB.ANALYTICS_SCHEMA.DATE_DIM d ON sf.ORDER_DATE_KEY = d.DATE_KEY
                LEFT JOIN RETAILWORKS_DB.ANALYTICS_SCHEMA.PRODUCT_DIM p ON sf.PRODUCT_KEY = p.PRODUCT_KEY
                WHERE c.IS_CURRENT = TRUE
                GROUP BY c.CUSTOMER_ID, c.CUSTOMER_TYPE, c.SEGMENT_NAME, c.ANNUAL_INCOME, c.AGE_GROUP
                HAVING total_orders > 0
            """).to_pandas()
            
            # Feature engineering
            churn_data['order_value_std'] = churn_data['order_value_std'].fillna(0)
            churn_data['order_frequency'] = churn_data['total_orders'] / churn_data['active_months']
            churn_data['avg_monthly_spend'] = churn_data['total_spent'] / churn_data['active_months']
            
            # Encode categorical variables
            le_customer_type = LabelEncoder()
            le_segment = LabelEncoder()
            le_age_group = LabelEncoder()
            
            churn_data['customer_type_encoded'] = le_customer_type.fit_transform(churn_data['customer_type'].fillna('Unknown'))
            churn_data['segment_encoded'] = le_segment.fit_transform(churn_data['segment_name'].fillna('Unknown'))
            churn_data['age_group_encoded'] = le_age_group.fit_transform(churn_data['age_group'].fillna('Unknown'))
            
            self.encoders = {
                'customer_type': le_customer_type,
                'segment': le_segment,
                'age_group': le_age_group
            }
            
            self.logger.info(f"Prepared churn features for {len(churn_data)} customers")
            return churn_data
            
        except Exception as e:
            self.logger.error(f"Error preparing churn features: {str(e)}")
            raise
    
    def train_churn_model(self, features_df: pd.DataFrame) -> Dict:
        """Train churn prediction model"""
        try:
            feature_columns = [
                'total_orders', 'total_spent', 'avg_order_value', 'days_since_last_order',
                'categories_purchased', 'order_value_std', 'active_months', 
                'order_frequency', 'avg_monthly_spend', 'annual_income',
                'customer_type_encoded', 'segment_encoded', 'age_group_encoded'
            ]
            
            X = features_df[feature_columns]
            y = features_df['is_churned']
            
            # Handle class imbalance
            churn_rate = y.mean()
            self.logger.info(f"Churn rate in dataset: {churn_rate:.2%}")
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
            
            # Scale features
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_test_scaled = self.scaler.transform(X_test)
            
            # Train Logistic Regression model
            self.model = LogisticRegression(random_state=42, class_weight='balanced')
            self.model.fit(X_train_scaled, y_train)
            
            # Make predictions
            y_pred = self.model.predict(X_test_scaled)
            y_pred_proba = self.model.predict_proba(X_test_scaled)[:, 1]
            
            # Calculate metrics
            from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
            
            accuracy = accuracy_score(y_test, y_pred)
            precision = precision_score(y_test, y_pred)
            recall = recall_score(y_test, y_pred)
            f1 = f1_score(y_test, y_pred)
            auc = roc_auc_score(y_test, y_pred_proba)
            
            results = {
                'model_type': 'Logistic Regression',
                'accuracy': float(accuracy),
                'precision': float(precision),
                'recall': float(recall),
                'f1_score': float(f1),
                'auc_score': float(auc),
                'churn_rate': float(churn_rate),
                'train_samples': len(X_train),
                'test_samples': len(X_test),
                'training_time': datetime.now()
            }
            
            self.logger.info(f"Churn model trained successfully. AUC: {auc:.4f}")
            return results
            
        except Exception as e:
            self.logger.error(f"Error training churn model: {str(e)}")
            raise


class SalesForecastingModel:
    """Sales forecasting using time series analysis"""
    
    def __init__(self, session: Session):
        self.session = session
        self.logger = logging.getLogger(__name__)
        self.model = None
        
    def prepare_sales_time_series(self, granularity: str = 'daily') -> pd.DataFrame:
        """Prepare sales time series data"""
        try:
            if granularity == 'daily':
                date_format = 'DATE_ACTUAL'
            elif granularity == 'weekly':
                date_format = 'DATE_TRUNC(\'week\', DATE_ACTUAL)'
            elif granularity == 'monthly':
                date_format = 'DATE_TRUNC(\'month\', DATE_ACTUAL)'
            else:
                raise ValueError("Granularity must be 'daily', 'weekly', or 'monthly'")
            
            sales_data = self.session.sql(f"""
                SELECT 
                    {date_format} as date_period,
                    SUM(sf.LINE_TOTAL) as total_sales,
                    COUNT(DISTINCT sf.ORDER_ID) as order_count,
                    AVG(sf.LINE_TOTAL) as avg_order_value,
                    COUNT(DISTINCT sf.CUSTOMER_KEY) as unique_customers,
                    d.DAY_OF_WEEK,
                    d.MONTH_NUMBER,
                    d.QUARTER_NUMBER,
                    d.IS_WEEKEND,
                    d.IS_HOLIDAY
                FROM RETAILWORKS_DB.ANALYTICS_SCHEMA.SALES_FACT sf
                JOIN RETAILWORKS_DB.ANALYTICS_SCHEMA.DATE_DIM d ON sf.ORDER_DATE_KEY = d.DATE_KEY
                GROUP BY {date_format}, d.DAY_OF_WEEK, d.MONTH_NUMBER, d.QUARTER_NUMBER, d.IS_WEEKEND, d.IS_HOLIDAY
                ORDER BY date_period
            """).to_pandas()
            
            # Convert to datetime
            sales_data['date_period'] = pd.to_datetime(sales_data['date_period'])
            
            # Create lag features
            sales_data['sales_lag_1'] = sales_data['total_sales'].shift(1)
            sales_data['sales_lag_7'] = sales_data['total_sales'].shift(7)
            sales_data['sales_ma_7'] = sales_data['total_sales'].rolling(window=7).mean()
            sales_data['sales_ma_30'] = sales_data['total_sales'].rolling(window=30).mean()
            
            # Create trend features
            sales_data['trend'] = range(len(sales_data))
            sales_data['month_sin'] = np.sin(2 * np.pi * sales_data['month_number'] / 12)
            sales_data['month_cos'] = np.cos(2 * np.pi * sales_data['month_number'] / 12)
            
            self.logger.info(f"Prepared {granularity} sales time series with {len(sales_data)} records")
            return sales_data
            
        except Exception as e:
            self.logger.error(f"Error preparing sales time series: {str(e)}")
            raise
    
    def train_sales_forecast_model(self, sales_data: pd.DataFrame) -> Dict:
        """Train sales forecasting model"""
        try:
            # Remove rows with NaN values (due to lag features)
            sales_data_clean = sales_data.dropna()
            
            feature_columns = [
                'order_count', 'avg_order_value', 'unique_customers',
                'day_of_week', 'month_number', 'quarter_number', 'is_weekend', 'is_holiday',
                'sales_lag_1', 'sales_lag_7', 'sales_ma_7', 'sales_ma_30',
                'trend', 'month_sin', 'month_cos'
            ]
            
            X = sales_data_clean[feature_columns]
            y = sales_data_clean['total_sales']
            
            # Split data (use last 20% for testing to maintain time order)
            split_point = int(len(X) * 0.8)
            X_train, X_test = X[:split_point], X[split_point:]
            y_train, y_test = y[:split_point], y[split_point:]
            
            # Train Random Forest model
            self.model = RandomForestRegressor(n_estimators=100, random_state=42)
            self.model.fit(X_train, y_train)
            
            # Make predictions
            y_pred = self.model.predict(X_test)
            
            # Calculate metrics
            mse = mean_squared_error(y_test, y_pred)
            r2 = r2_score(y_test, y_pred)
            mape = np.mean(np.abs((y_test - y_pred) / y_test)) * 100
            
            results = {
                'model_type': 'Random Forest (Time Series)',
                'mse': float(mse),
                'r2_score': float(r2),
                'mape': float(mape),
                'train_samples': len(X_train),
                'test_samples': len(X_test),
                'training_time': datetime.now()
            }
            
            self.logger.info(f"Sales forecast model trained successfully. R² Score: {r2:.4f}, MAPE: {mape:.2f}%")
            return results
            
        except Exception as e:
            self.logger.error(f"Error training sales forecast model: {str(e)}")
            raise


def main():
    """Main function to train and evaluate ML models"""
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Connection parameters (to be configured)
    connection_parameters = {
        "account": "your_account",
        "user": "your_user", 
        "password": "your_password",
        "role": "your_role",
        "warehouse": "your_warehouse",
        "database": "RETAILWORKS_DB",
        "schema": "ANALYTICS_SCHEMA"
    }
    
    try:
        # Create Snowpark session
        session = Session.builder.configs(connection_parameters).create()
        
        print("Training ML Models for RetailWorks...")
        
        # Train CLV Model
        print("\n1. Training Customer Lifetime Value Model...")
        clv_model = CustomerLifetimeValueModel(session)
        clv_features = clv_model.prepare_clv_features()
        clv_results = clv_model.train_clv_model(clv_features)
        print(f"CLV Model Results: {clv_results}")
        
        # Train Churn Model
        print("\n2. Training Customer Churn Prediction Model...")
        churn_model = ChurnPredictionModel(session)
        churn_features = churn_model.prepare_churn_features()
        churn_results = churn_model.train_churn_model(churn_features)
        print(f"Churn Model Results: {churn_results}")
        
        # Train Sales Forecast Model
        print("\n3. Training Sales Forecasting Model...")
        forecast_model = SalesForecastingModel(session)
        sales_data = forecast_model.prepare_sales_time_series('daily')
        forecast_results = forecast_model.train_sales_forecast_model(sales_data)
        print(f"Forecast Model Results: {forecast_results}")
        
        # Save models
        print("\n4. Saving trained models...")
        joblib.dump(clv_model, 'clv_model.pkl')
        joblib.dump(churn_model, 'churn_model.pkl')
        joblib.dump(forecast_model, 'forecast_model.pkl')
        
        print("All models trained and saved successfully!")
        
    except Exception as e:
        print(f"ML model training failed: {str(e)}")
        raise
    finally:
        if 'session' in locals():
            session.close()


if __name__ == "__main__":
    main()