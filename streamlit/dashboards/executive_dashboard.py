"""
Executive Dashboard - Streamlit Application
Description: High-level KPIs and executive overview dashboard
Version: 1.0
Date: 2025-07-19
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector
from datetime import datetime, timedelta
import sys
import os

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from snowflake_connection import get_snowflake_connection
from chart_utils import create_kpi_card, format_currency, format_number

# Page configuration
st.set_page_config(
    page_title="RetailWorks Executive Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
.metric-card {
    background-color: #f0f2f6;
    padding: 1rem;
    border-radius: 0.5rem;
    border-left: 4px solid #1f77b4;
    margin: 0.5rem 0;
}
.big-font {
    font-size: 2rem;
    font-weight: bold;
    color: #1f77b4;
}
.metric-label {
    font-size: 0.9rem;
    color: #666;
    margin-bottom: 0.5rem;
}
</style>
""", unsafe_allow_html=True)

class ExecutiveDashboard:
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
    
    def get_kpi_data(self, period_filter: str = "Current Month"):
        """Get KPI data for dashboard"""
        try:
            query = f"""
                SELECT 
                    period_type,
                    period_name,
                    total_orders,
                    unique_customers,
                    total_revenue,
                    total_profit,
                    avg_order_value,
                    profit_margin_percent,
                    products_sold
                FROM RETAILWORKS_DB.ANALYTICS_SCHEMA.VW_EXECUTIVE_KPI_DASHBOARD
                WHERE period_type = '{period_filter}'
            """
            
            df = pd.read_sql(query, self.conn)
            return df.iloc[0] if len(df) > 0 else None
            
        except Exception as e:
            st.error(f"Error fetching KPI data: {str(e)}")
            return None
    
    def get_sales_trend_data(self, days: int = 30):
        """Get sales trend data for specified number of days"""
        try:
            query = f"""
                SELECT 
                    date_actual,
                    daily_revenue,
                    daily_orders,
                    avg_order_value,
                    is_weekend,
                    is_holiday
                FROM RETAILWORKS_DB.ANALYTICS_SCHEMA.VW_SALES_TREND_ANALYSIS
                WHERE date_actual >= CURRENT_DATE() - {days}
                ORDER BY date_actual
            """
            
            df = pd.read_sql(query, self.conn)
            df['date_actual'] = pd.to_datetime(df['date_actual'])
            return df
            
        except Exception as e:
            st.error(f"Error fetching sales trend data: {str(e)}")
            return pd.DataFrame()
    
    def get_top_categories_data(self, limit: int = 10):
        """Get top categories by revenue"""
        try:
            query = f"""
                SELECT 
                    category_name,
                    total_revenue,
                    total_profit,
                    profit_margin_percent,
                    total_units_sold
                FROM RETAILWORKS_DB.ANALYTICS_SCHEMA.VW_CATEGORY_PERFORMANCE
                ORDER BY total_revenue DESC
                LIMIT {limit}
            """
            
            df = pd.read_sql(query, self.conn)
            return df
            
        except Exception as e:
            st.error(f"Error fetching category data: {str(e)}")
            return pd.DataFrame()
    
    def get_territory_performance_data(self):
        """Get territory performance data"""
        try:
            query = """
                SELECT 
                    territory_name,
                    region,
                    country,
                    total_revenue,
                    total_orders,
                    unique_customers,
                    revenue_per_customer
                FROM RETAILWORKS_DB.ANALYTICS_SCHEMA.VW_TERRITORY_SALES_ANALYSIS
                ORDER BY total_revenue DESC
            """
            
            df = pd.read_sql(query, self.conn)
            return df
            
        except Exception as e:
            st.error(f"Error fetching territory data: {str(e)}")
            return pd.DataFrame()
    
    def render_kpi_section(self, kpi_data):
        """Render KPI cards section"""
        if kpi_data is None:
            st.warning("No KPI data available")
            return
        
        st.subheader("📈 Key Performance Indicators")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown("""
                <div class="metric-card">
                    <div class="metric-label">Total Revenue</div>
                    <div class="big-font">{}</div>
                </div>
            """.format(format_currency(kpi_data['total_revenue'])), unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
                <div class="metric-card">
                    <div class="metric-label">Total Orders</div>
                    <div class="big-font">{}</div>
                </div>
            """.format(format_number(kpi_data['total_orders'])), unsafe_allow_html=True)
        
        with col3:
            st.markdown("""
                <div class="metric-card">
                    <div class="metric-label">Unique Customers</div>
                    <div class="big-font">{}</div>
                </div>
            """.format(format_number(kpi_data['unique_customers'])), unsafe_allow_html=True)
        
        with col4:
            st.markdown("""
                <div class="metric-card">
                    <div class="metric-label">Avg Order Value</div>
                    <div class="big-font">{}</div>
                </div>
            """.format(format_currency(kpi_data['avg_order_value'])), unsafe_allow_html=True)
        
        # Second row of KPIs
        col5, col6, col7, col8 = st.columns(4)
        
        with col5:
            st.markdown("""
                <div class="metric-card">
                    <div class="metric-label">Total Profit</div>
                    <div class="big-font">{}</div>
                </div>
            """.format(format_currency(kpi_data['total_profit'])), unsafe_allow_html=True)
        
        with col6:
            st.markdown("""
                <div class="metric-card">
                    <div class="metric-label">Profit Margin</div>
                    <div class="big-font">{}%</div>
                </div>
            """.format(round(kpi_data['profit_margin_percent'], 1)), unsafe_allow_html=True)
        
        with col7:
            st.markdown("""
                <div class="metric-card">
                    <div class="metric-label">Products Sold</div>
                    <div class="big-font">{}</div>
                </div>
            """.format(format_number(kpi_data['products_sold'])), unsafe_allow_html=True)
        
        with col8:
            st.markdown("""
                <div class="metric-card">
                    <div class="metric-label">Period</div>
                    <div class="big-font" style="font-size: 1.2rem;">{}</div>
                </div>
            """.format(kpi_data['period_name']), unsafe_allow_html=True)
    
    def render_sales_trend_chart(self, trend_data):
        """Render sales trend chart"""
        if trend_data.empty:
            st.warning("No sales trend data available")
            return
        
        st.subheader("📊 Sales Trend Analysis")
        
        # Create subplot with secondary y-axis
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        # Add revenue trend
        fig.add_trace(
            go.Scatter(
                x=trend_data['date_actual'],
                y=trend_data['daily_revenue'],
                mode='lines+markers',
                name='Daily Revenue',
                line=dict(color='#1f77b4', width=2),
                marker=dict(size=4)
            ),
            secondary_y=False
        )
        
        # Add order count
        fig.add_trace(
            go.Scatter(
                x=trend_data['date_actual'],
                y=trend_data['daily_orders'],
                mode='lines+markers',
                name='Daily Orders',
                line=dict(color='#ff7f0e', width=2),
                marker=dict(size=4)
            ),
            secondary_y=True
        )
        
        # Highlight weekends and holidays
        for idx, row in trend_data.iterrows():
            if row['is_holiday']:
                fig.add_vline(
                    x=row['date_actual'],
                    line_color='red',
                    line_dash='dash',
                    annotation_text='Holiday'
                )
            elif row['is_weekend']:
                fig.add_vrect(
                    x0=row['date_actual'] - timedelta(hours=12),
                    x1=row['date_actual'] + timedelta(hours=12),
                    fillcolor='lightblue',
                    opacity=0.2,
                    layer='below',
                    line_width=0
                )
        
        # Update layout
        fig.update_layout(
            title='Daily Sales Trend',
            height=400,
            hovermode='x unified'
        )
        
        fig.update_yaxes(title_text="Revenue ($)", secondary_y=False)
        fig.update_yaxes(title_text="Number of Orders", secondary_y=True)
        
        st.plotly_chart(fig, use_container_width=True)
    
    def render_category_performance_chart(self, category_data):
        """Render category performance chart"""
        if category_data.empty:
            st.warning("No category data available")
            return
        
        st.subheader("🏷️ Category Performance")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Revenue by category
            fig1 = px.bar(
                category_data,
                x='total_revenue',
                y='category_name',
                orientation='h',
                title='Revenue by Category',
                labels={'total_revenue': 'Revenue ($)', 'category_name': 'Category'},
                color='total_revenue',
                color_continuous_scale='Blues'
            )
            fig1.update_layout(height=400)
            st.plotly_chart(fig1, use_container_width=True)
        
        with col2:
            # Profit margin by category
            fig2 = px.scatter(
                category_data,
                x='total_revenue',
                y='profit_margin_percent',
                size='total_units_sold',
                hover_name='category_name',
                title='Revenue vs Profit Margin',
                labels={
                    'total_revenue': 'Revenue ($)',
                    'profit_margin_percent': 'Profit Margin (%)',
                    'total_units_sold': 'Units Sold'
                }
            )
            fig2.update_layout(height=400)
            st.plotly_chart(fig2, use_container_width=True)
    
    def render_territory_performance_map(self, territory_data):
        """Render territory performance visualization"""
        if territory_data.empty:
            st.warning("No territory data available")
            return
        
        st.subheader("🌍 Territory Performance")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Territory revenue treemap
            fig1 = px.treemap(
                territory_data,
                path=['region', 'territory_name'],
                values='total_revenue',
                title='Revenue by Territory',
                color='total_revenue',
                color_continuous_scale='Viridis'
            )
            fig1.update_layout(height=400)
            st.plotly_chart(fig1, use_container_width=True)
        
        with col2:
            # Territory metrics table
            st.subheader("Territory Metrics")
            formatted_data = territory_data.copy()
            formatted_data['total_revenue'] = formatted_data['total_revenue'].apply(format_currency)
            formatted_data['revenue_per_customer'] = formatted_data['revenue_per_customer'].apply(format_currency)
            
            st.dataframe(
                formatted_data[['territory_name', 'total_revenue', 'total_orders', 'revenue_per_customer']],
                hide_index=True,
                height=350
            )
    
    def run(self):
        """Main dashboard function"""
        st.title("🏢 RetailWorks Executive Dashboard")
        st.markdown("---")
        
        # Sidebar filters
        st.sidebar.header("📅 Filters")
        
        period_options = ["Current Month", "Current Quarter", "Year to Date"]
        selected_period = st.sidebar.selectbox("Time Period", period_options)
        
        trend_days = st.sidebar.slider("Sales Trend Days", 7, 90, 30)
        
        top_categories = st.sidebar.slider("Top Categories", 5, 20, 10)
        
        # Refresh button
        if st.sidebar.button("🔄 Refresh Data"):
            st.rerun()
        
        # Load data
        with st.spinner("Loading dashboard data..."):
            kpi_data = self.get_kpi_data(selected_period)
            trend_data = self.get_sales_trend_data(trend_days)
            category_data = self.get_top_categories_data(top_categories)
            territory_data = self.get_territory_performance_data()
        
        # Render dashboard sections
        self.render_kpi_section(kpi_data)
        
        st.markdown("---")
        
        self.render_sales_trend_chart(trend_data)
        
        st.markdown("---")
        
        self.render_category_performance_chart(category_data)
        
        st.markdown("---")
        
        self.render_territory_performance_map(territory_data)
        
        # Footer
        st.markdown("---")
        st.markdown("""
            <div style='text-align: center; color: #666; font-size: 0.8rem;'>
                RetailWorks Executive Dashboard | Data refreshed: {}
            </div>
        """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")), unsafe_allow_html=True)


def main():
    """Main function"""
    dashboard = ExecutiveDashboard()
    dashboard.run()


if __name__ == "__main__":
    main()