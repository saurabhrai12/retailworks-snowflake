"""
Sales Dashboard - Streamlit Application
Description: Detailed sales analytics and performance dashboard
Version: 1.0
Date: 2025-07-19
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta, date
import sys
import os

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from snowflake_connection import get_snowflake_connection
from chart_utils import (
    create_kpi_card, format_currency, format_number, format_percentage,
    create_trend_chart, create_bar_chart, create_combo_chart
)

# Page configuration
st.set_page_config(
    page_title="RetailWorks Sales Dashboard",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

class SalesDashboard:
    def __init__(self):
        self.conn = None
        self.setup_connection()
    
    def setup_connection(self):
        """Setup Snowflake connection"""
        try:
            self.conn = get_snowflake_connection()
        except Exception as e:
            st.error(f"Failed to connect to Snowflake: {str(e)}")
            st.stop()
    
    def get_sales_overview_data(self, start_date: date, end_date: date):
        """Get sales overview data for date range"""
        try:
            query = f"""
                SELECT 
                    COUNT(DISTINCT sf.ORDER_ID) as total_orders,
                    COUNT(DISTINCT sf.CUSTOMER_KEY) as unique_customers,
                    SUM(sf.LINE_TOTAL) as total_revenue,
                    SUM(sf.PROFIT) as total_profit,
                    AVG(sf.LINE_TOTAL) as avg_order_value,
                    SUM(sf.QUANTITY) as total_units_sold,
                    COUNT(DISTINCT sf.PRODUCT_KEY) as products_sold,
                    ROUND((SUM(sf.PROFIT) / NULLIF(SUM(sf.LINE_TOTAL), 0)) * 100, 2) as profit_margin_percent
                FROM RETAILWORKS_DB.ANALYTICS_SCHEMA.SALES_FACT sf
                JOIN RETAILWORKS_DB.ANALYTICS_SCHEMA.DATE_DIM d ON sf.ORDER_DATE_KEY = d.DATE_KEY
                WHERE d.DATE_ACTUAL BETWEEN '{start_date}' AND '{end_date}'
            """
            
            df = pd.read_sql(query, self.conn)
            return df.iloc[0] if len(df) > 0 else None
            
        except Exception as e:
            st.error(f"Error fetching sales overview data: {str(e)}")
            return None
    
    def get_daily_sales_data(self, start_date: date, end_date: date):
        """Get daily sales data for trend analysis"""
        try:
            query = f"""
                SELECT 
                    d.DATE_ACTUAL,
                    d.DAY_OF_WEEK_NAME,
                    d.IS_WEEKEND,
                    d.IS_HOLIDAY,
                    COUNT(DISTINCT sf.ORDER_ID) as daily_orders,
                    SUM(sf.LINE_TOTAL) as daily_revenue,
                    SUM(sf.PROFIT) as daily_profit,
                    AVG(sf.LINE_TOTAL) as avg_order_value,
                    SUM(sf.QUANTITY) as units_sold
                FROM RETAILWORKS_DB.ANALYTICS_SCHEMA.SALES_FACT sf
                JOIN RETAILWORKS_DB.ANALYTICS_SCHEMA.DATE_DIM d ON sf.ORDER_DATE_KEY = d.DATE_KEY
                WHERE d.DATE_ACTUAL BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY d.DATE_ACTUAL, d.DAY_OF_WEEK_NAME, d.IS_WEEKEND, d.IS_HOLIDAY
                ORDER BY d.DATE_ACTUAL
            """
            
            df = pd.read_sql(query, self.conn)
            df['date_actual'] = pd.to_datetime(df['date_actual'])
            return df
            
        except Exception as e:
            st.error(f"Error fetching daily sales data: {str(e)}")
            return pd.DataFrame()
    
    def get_sales_rep_performance(self, start_date: date, end_date: date):
        """Get sales representative performance data"""
        try:
            query = f"""
                SELECT 
                    sr.SALES_REP_NAME,
                    sr.TERRITORY_NAME,
                    sr.REGION,
                    COUNT(DISTINCT sf.ORDER_ID) as total_orders,
                    COUNT(DISTINCT sf.CUSTOMER_KEY) as unique_customers,
                    SUM(sf.LINE_TOTAL) as total_revenue,
                    SUM(sf.PROFIT) as total_profit,
                    AVG(sf.LINE_TOTAL) as avg_order_value,
                    ROUND((SUM(sf.PROFIT) / NULLIF(SUM(sf.LINE_TOTAL), 0)) * 100, 2) as profit_margin_percent
                FROM RETAILWORKS_DB.ANALYTICS_SCHEMA.SALES_FACT sf
                JOIN RETAILWORKS_DB.ANALYTICS_SCHEMA.SALES_REP_DIM sr ON sf.SALES_REP_KEY = sr.SALES_REP_KEY
                JOIN RETAILWORKS_DB.ANALYTICS_SCHEMA.DATE_DIM d ON sf.ORDER_DATE_KEY = d.DATE_KEY
                WHERE d.DATE_ACTUAL BETWEEN '{start_date}' AND '{end_date}'
                  AND sr.IS_CURRENT = TRUE
                GROUP BY sr.SALES_REP_NAME, sr.TERRITORY_NAME, sr.REGION
                ORDER BY total_revenue DESC
            """
            
            df = pd.read_sql(query, self.conn)
            return df
            
        except Exception as e:
            st.error(f"Error fetching sales rep performance data: {str(e)}")
            return pd.DataFrame()
    
    def get_product_performance(self, start_date: date, end_date: date, limit: int = 20):
        """Get top performing products"""
        try:
            query = f"""
                SELECT 
                    p.PRODUCT_NAME,
                    p.CATEGORY_NAME,
                    p.SUPPLIER_NAME,
                    SUM(sf.QUANTITY) as total_units_sold,
                    SUM(sf.LINE_TOTAL) as total_revenue,
                    SUM(sf.PROFIT) as total_profit,
                    AVG(sf.UNIT_PRICE) as avg_selling_price,
                    COUNT(DISTINCT sf.ORDER_ID) as orders_count,
                    ROUND((SUM(sf.PROFIT) / NULLIF(SUM(sf.LINE_TOTAL), 0)) * 100, 2) as profit_margin_percent
                FROM RETAILWORKS_DB.ANALYTICS_SCHEMA.SALES_FACT sf
                JOIN RETAILWORKS_DB.ANALYTICS_SCHEMA.PRODUCT_DIM p ON sf.PRODUCT_KEY = p.PRODUCT_KEY
                JOIN RETAILWORKS_DB.ANALYTICS_SCHEMA.DATE_DIM d ON sf.ORDER_DATE_KEY = d.DATE_KEY
                WHERE d.DATE_ACTUAL BETWEEN '{start_date}' AND '{end_date}'
                  AND p.IS_CURRENT = TRUE
                GROUP BY p.PRODUCT_NAME, p.CATEGORY_NAME, p.SUPPLIER_NAME
                ORDER BY total_revenue DESC
                LIMIT {limit}
            """
            
            df = pd.read_sql(query, self.conn)
            return df
            
        except Exception as e:
            st.error(f"Error fetching product performance data: {str(e)}")
            return pd.DataFrame()
    
    def get_customer_analysis(self, start_date: date, end_date: date):
        """Get customer analysis data"""
        try:
            query = f"""
                SELECT 
                    c.SEGMENT_NAME,
                    c.CUSTOMER_TYPE,
                    COUNT(DISTINCT c.CUSTOMER_KEY) as customer_count,
                    COUNT(DISTINCT sf.ORDER_ID) as total_orders,
                    SUM(sf.LINE_TOTAL) as total_revenue,
                    AVG(sf.LINE_TOTAL) as avg_order_value,
                    ROUND(SUM(sf.LINE_TOTAL) / NULLIF(COUNT(DISTINCT c.CUSTOMER_KEY), 0), 2) as revenue_per_customer
                FROM RETAILWORKS_DB.ANALYTICS_SCHEMA.SALES_FACT sf
                JOIN RETAILWORKS_DB.ANALYTICS_SCHEMA.CUSTOMER_DIM c ON sf.CUSTOMER_KEY = c.CUSTOMER_KEY
                JOIN RETAILWORKS_DB.ANALYTICS_SCHEMA.DATE_DIM d ON sf.ORDER_DATE_KEY = d.DATE_KEY
                WHERE d.DATE_ACTUAL BETWEEN '{start_date}' AND '{end_date}'
                  AND c.IS_CURRENT = TRUE
                GROUP BY c.SEGMENT_NAME, c.CUSTOMER_TYPE
                ORDER BY total_revenue DESC
            """
            
            df = pd.read_sql(query, self.conn)
            return df
            
        except Exception as e:
            st.error(f"Error fetching customer analysis data: {str(e)}")
            return pd.DataFrame()
    
    def get_monthly_comparison(self, year: int):
        """Get monthly sales comparison for the year"""
        try:
            query = f"""
                SELECT 
                    d.MONTH_NUMBER,
                    d.MONTH_NAME,
                    SUM(sf.LINE_TOTAL) as monthly_revenue,
                    SUM(sf.PROFIT) as monthly_profit,
                    COUNT(DISTINCT sf.ORDER_ID) as monthly_orders,
                    AVG(sf.LINE_TOTAL) as avg_order_value
                FROM RETAILWORKS_DB.ANALYTICS_SCHEMA.SALES_FACT sf
                JOIN RETAILWORKS_DB.ANALYTICS_SCHEMA.DATE_DIM d ON sf.ORDER_DATE_KEY = d.DATE_KEY
                WHERE d.YEAR_NUMBER = {year}
                GROUP BY d.MONTH_NUMBER, d.MONTH_NAME
                ORDER BY d.MONTH_NUMBER
            """
            
            df = pd.read_sql(query, self.conn)
            return df
            
        except Exception as e:
            st.error(f"Error fetching monthly comparison data: {str(e)}")
            return pd.DataFrame()
    
    def render_sales_overview(self, overview_data):
        """Render sales overview KPIs"""
        if overview_data is None:
            st.warning("No sales overview data available")
            return
        
        st.subheader("📊 Sales Overview")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            create_kpi_card(
                "Total Revenue",
                format_currency(overview_data['total_revenue'])
            )
        
        with col2:
            create_kpi_card(
                "Total Orders",
                format_number(overview_data['total_orders'])
            )
        
        with col3:
            create_kpi_card(
                "Unique Customers",
                format_number(overview_data['unique_customers'])
            )
        
        with col4:
            create_kpi_card(
                "Avg Order Value",
                format_currency(overview_data['avg_order_value'])
            )
        
        # Second row
        col5, col6, col7, col8 = st.columns(4)
        
        with col5:
            create_kpi_card(
                "Total Profit",
                format_currency(overview_data['total_profit'])
            )
        
        with col6:
            create_kpi_card(
                "Profit Margin",
                format_percentage(overview_data['profit_margin_percent'])
            )
        
        with col7:
            create_kpi_card(
                "Units Sold",
                format_number(overview_data['total_units_sold'])
            )
        
        with col8:
            create_kpi_card(
                "Products Sold",
                format_number(overview_data['products_sold'])
            )
    
    def render_daily_trends(self, daily_data):
        """Render daily sales trends"""
        if daily_data.empty:
            st.warning("No daily sales data available")
            return
        
        st.subheader("📈 Daily Sales Trends")
        
        tab1, tab2, tab3 = st.tabs(["Revenue Trend", "Order Volume", "Performance Metrics"])
        
        with tab1:
            fig = create_combo_chart(
                daily_data,
                'date_actual',
                'daily_revenue',
                'daily_orders',
                'Daily Revenue vs Orders'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with tab2:
            # Orders by day of week
            dow_data = daily_data.groupby('day_of_week_name').agg({
                'daily_orders': 'mean',
                'daily_revenue': 'mean'
            }).reset_index()
            
            fig = create_bar_chart(
                dow_data,
                'day_of_week_name',
                'daily_orders',
                'Average Orders by Day of Week'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with tab3:
            col1, col2 = st.columns(2)
            
            with col1:
                # Weekend vs Weekday performance
                weekend_data = daily_data.groupby('is_weekend').agg({
                    'daily_revenue': 'mean',
                    'daily_orders': 'mean'
                }).reset_index()
                weekend_data['day_type'] = weekend_data['is_weekend'].map({True: 'Weekend', False: 'Weekday'})
                
                fig = px.bar(
                    weekend_data,
                    x='day_type',
                    y='daily_revenue',
                    title='Avg Revenue: Weekend vs Weekday',
                    color='daily_revenue',
                    color_continuous_scale='Blues'
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Holiday performance
                if daily_data['is_holiday'].any():
                    holiday_data = daily_data.groupby('is_holiday').agg({
                        'daily_revenue': 'mean',
                        'daily_orders': 'mean'
                    }).reset_index()
                    holiday_data['day_type'] = holiday_data['is_holiday'].map({True: 'Holiday', False: 'Regular'})
                    
                    fig = px.bar(
                        holiday_data,
                        x='day_type',
                        y='daily_revenue',
                        title='Avg Revenue: Holiday vs Regular',
                        color='daily_revenue',
                        color_continuous_scale='Reds'
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No holiday data in selected period")
    
    def render_sales_rep_performance(self, rep_data):
        """Render sales representative performance"""
        if rep_data.empty:
            st.warning("No sales rep data available")
            return
        
        st.subheader("👥 Sales Representative Performance")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Top performers by revenue
            top_reps = rep_data.head(10)
            fig = create_bar_chart(
                top_reps,
                'total_revenue',
                'sales_rep_name',
                'Top 10 Sales Reps by Revenue',
                orientation='h'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Performance scatter plot
            fig = px.scatter(
                rep_data,
                x='total_orders',
                y='avg_order_value',
                size='total_revenue',
                color='profit_margin_percent',
                hover_name='sales_rep_name',
                title='Orders vs AOV (Size = Revenue, Color = Margin)',
                labels={
                    'total_orders': 'Number of Orders',
                    'avg_order_value': 'Average Order Value ($)',
                    'profit_margin_percent': 'Profit Margin (%)'
                }
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Detailed table
        st.subheader("📋 Detailed Sales Rep Performance")
        
        # Format data for display
        display_data = rep_data.copy()
        display_data['total_revenue'] = display_data['total_revenue'].apply(format_currency)
        display_data['total_profit'] = display_data['total_profit'].apply(format_currency)
        display_data['avg_order_value'] = display_data['avg_order_value'].apply(format_currency)
        display_data['profit_margin_percent'] = display_data['profit_margin_percent'].apply(lambda x: f"{x:.1f}%")
        
        st.dataframe(
            display_data,
            hide_index=True,
            height=400
        )
    
    def render_product_analysis(self, product_data):
        """Render product performance analysis"""
        if product_data.empty:
            st.warning("No product data available")
            return
        
        st.subheader("🛍️ Product Performance Analysis")
        
        tab1, tab2, tab3 = st.tabs(["Top Products", "Category Analysis", "Profitability"])
        
        with tab1:
            col1, col2 = st.columns(2)
            
            with col1:
                # Top products by revenue
                top_products = product_data.head(10)
                fig = create_bar_chart(
                    top_products,
                    'total_revenue',
                    'product_name',
                    'Top 10 Products by Revenue',
                    orientation='h'
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Top products by units sold
                top_units = product_data.nlargest(10, 'total_units_sold')
                fig = create_bar_chart(
                    top_units,
                    'total_units_sold',
                    'product_name',
                    'Top 10 Products by Units Sold',
                    orientation='h'
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with tab2:
            # Category performance
            category_data = product_data.groupby('category_name').agg({
                'total_revenue': 'sum',
                'total_profit': 'sum',
                'total_units_sold': 'sum',
                'orders_count': 'sum'
            }).reset_index()
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.pie(
                    category_data,
                    values='total_revenue',
                    names='category_name',
                    title='Revenue Distribution by Category'
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = create_bar_chart(
                    category_data.sort_values('total_revenue', ascending=False),
                    'category_name',
                    'total_revenue',
                    'Revenue by Category'
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with tab3:
            # Profitability analysis
            fig = px.scatter(
                product_data,
                x='total_revenue',
                y='profit_margin_percent',
                size='total_units_sold',
                color='category_name',
                hover_name='product_name',
                title='Revenue vs Profit Margin (Size = Units Sold)',
                labels={
                    'total_revenue': 'Revenue ($)',
                    'profit_margin_percent': 'Profit Margin (%)',
                    'total_units_sold': 'Units Sold'
                }
            )
            st.plotly_chart(fig, use_container_width=True)
    
    def render_customer_analysis(self, customer_data):
        """Render customer segment analysis"""
        if customer_data.empty:
            st.warning("No customer analysis data available")
            return
        
        st.subheader("👥 Customer Segment Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Revenue by segment
            fig = px.pie(
                customer_data,
                values='total_revenue',
                names='segment_name',
                title='Revenue by Customer Segment'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Revenue per customer by segment
            fig = create_bar_chart(
                customer_data.sort_values('revenue_per_customer', ascending=False),
                'segment_name',
                'revenue_per_customer',
                'Revenue per Customer by Segment'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Customer metrics table
        st.subheader("Customer Segment Metrics")
        
        display_data = customer_data.copy()
        display_data['total_revenue'] = display_data['total_revenue'].apply(format_currency)
        display_data['avg_order_value'] = display_data['avg_order_value'].apply(format_currency)
        display_data['revenue_per_customer'] = display_data['revenue_per_customer'].apply(format_currency)
        
        st.dataframe(display_data, hide_index=True)
    
    def run(self):
        """Main dashboard function"""
        st.title("💰 RetailWorks Sales Dashboard")
        st.markdown("---")
        
        # Sidebar filters
        st.sidebar.header("📅 Date Filters")
        
        # Date range picker
        col1, col2 = st.sidebar.columns(2)
        with col1:
            start_date = st.date_input("Start Date", value=date.today() - timedelta(days=30))
        with col2:
            end_date = st.date_input("End Date", value=date.today())
        
        # Additional filters
        st.sidebar.header("📊 Display Options")
        show_trends = st.sidebar.checkbox("Show Daily Trends", value=True)
        show_reps = st.sidebar.checkbox("Show Sales Rep Performance", value=True)
        show_products = st.sidebar.checkbox("Show Product Analysis", value=True)
        show_customers = st.sidebar.checkbox("Show Customer Analysis", value=True)
        
        # Refresh button
        if st.sidebar.button("🔄 Refresh Data"):
            st.rerun()
        
        # Validate date range
        if start_date > end_date:
            st.error("Start date must be before end date")
            return
        
        # Load data
        with st.spinner("Loading sales data..."):
            overview_data = self.get_sales_overview_data(start_date, end_date)
            
            if show_trends:
                daily_data = self.get_daily_sales_data(start_date, end_date)
            
            if show_reps:
                rep_data = self.get_sales_rep_performance(start_date, end_date)
            
            if show_products:
                product_data = self.get_product_performance(start_date, end_date)
            
            if show_customers:
                customer_data = self.get_customer_analysis(start_date, end_date)
        
        # Render dashboard sections
        self.render_sales_overview(overview_data)
        
        if show_trends:
            st.markdown("---")
            self.render_daily_trends(daily_data)
        
        if show_reps:
            st.markdown("---")
            self.render_sales_rep_performance(rep_data)
        
        if show_products:
            st.markdown("---")
            self.render_product_analysis(product_data)
        
        if show_customers:
            st.markdown("---")
            self.render_customer_analysis(customer_data)
        
        # Footer
        st.markdown("---")
        st.markdown(f"""
            <div style='text-align: center; color: #666; font-size: 0.8rem;'>
                Sales Dashboard | Period: {start_date} to {end_date} | Updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            </div>
        """, unsafe_allow_html=True)


def main():
    """Main function"""
    dashboard = SalesDashboard()
    dashboard.run()


if __name__ == "__main__":
    main()