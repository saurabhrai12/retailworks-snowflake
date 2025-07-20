"""
Chart Utilities
Description: Utility functions for creating charts and formatting data
Version: 1.0
Date: 2025-07-19
"""

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import streamlit as st
from datetime import datetime
import numpy as np

def format_currency(value, currency_symbol="$"):
    """Format numeric value as currency"""
    if pd.isna(value) or value is None:
        return f"{currency_symbol}0"
    
    if abs(value) >= 1e9:
        return f"{currency_symbol}{value/1e9:.1f}B"
    elif abs(value) >= 1e6:
        return f"{currency_symbol}{value/1e6:.1f}M"
    elif abs(value) >= 1e3:
        return f"{currency_symbol}{value/1e3:.1f}K"
    else:
        return f"{currency_symbol}{value:,.0f}"

def format_number(value):
    """Format numeric value with appropriate suffixes"""
    if pd.isna(value) or value is None:
        return "0"
    
    if abs(value) >= 1e9:
        return f"{value/1e9:.1f}B"
    elif abs(value) >= 1e6:
        return f"{value/1e6:.1f}M"
    elif abs(value) >= 1e3:
        return f"{value/1e3:.1f}K"
    else:
        return f"{value:,.0f}"

def format_percentage(value, decimal_places=1):
    """Format numeric value as percentage"""
    if pd.isna(value) or value is None:
        return "0.0%"
    return f"{value:.{decimal_places}f}%"

def create_kpi_card(title, value, delta=None, delta_color="normal"):
    """Create a KPI card with optional delta"""
    return st.metric(
        label=title,
        value=value,
        delta=delta,
        delta_color=delta_color
    )

def create_trend_chart(df, x_col, y_col, title="Trend Chart", color="#1f77b4"):
    """Create a trend line chart"""
    fig = px.line(
        df, 
        x=x_col, 
        y=y_col,
        title=title,
        line_shape='spline'
    )
    
    fig.update_traces(line_color=color, line_width=3)
    fig.update_layout(
        hovermode='x unified',
        showlegend=False,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig

def create_bar_chart(df, x_col, y_col, title="Bar Chart", orientation='v', color_col=None):
    """Create a bar chart"""
    if orientation == 'h':
        fig = px.bar(
            df, 
            x=y_col, 
            y=x_col, 
            title=title,
            orientation='h',
            color=color_col if color_col else y_col,
            color_continuous_scale='Blues'
        )
    else:
        fig = px.bar(
            df, 
            x=x_col, 
            y=y_col, 
            title=title,
            color=color_col if color_col else y_col,
            color_continuous_scale='Blues'
        )
    
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        showlegend=False
    )
    
    return fig

def create_pie_chart(df, values_col, names_col, title="Distribution"):
    """Create a pie chart"""
    fig = px.pie(
        df, 
        values=values_col, 
        names=names_col, 
        title=title,
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    
    fig.update_traces(textposition='inside', textinfo='percent+label')
    fig.update_layout(
        showlegend=True,
        legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.01)
    )
    
    return fig

def create_donut_chart(df, values_col, names_col, title="Distribution"):
    """Create a donut chart"""
    fig = px.pie(
        df, 
        values=values_col, 
        names=names_col, 
        title=title,
        color_discrete_sequence=px.colors.qualitative.Set3,
        hole=0.4
    )
    
    fig.update_traces(textposition='inside', textinfo='percent+label')
    fig.update_layout(
        showlegend=True,
        legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.01)
    )
    
    return fig

def create_scatter_plot(df, x_col, y_col, size_col=None, color_col=None, title="Scatter Plot"):
    """Create a scatter plot"""
    fig = px.scatter(
        df,
        x=x_col,
        y=y_col,
        size=size_col,
        color=color_col,
        title=title,
        hover_data=df.columns
    )
    
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig

def create_heatmap(df, x_col, y_col, values_col, title="Heatmap"):
    """Create a heatmap"""
    pivot_df = df.pivot(index=y_col, columns=x_col, values=values_col)
    
    fig = px.imshow(
        pivot_df,
        title=title,
        color_continuous_scale='Blues',
        aspect='auto'
    )
    
    return fig

def create_gauge_chart(value, title="Gauge", max_value=100, threshold_colors=None):
    """Create a gauge chart"""
    if threshold_colors is None:
        threshold_colors = ["red", "yellow", "green"]
    
    fig = go.Figure(go.Indicator(
        mode = "gauge+number+delta",
        value = value,
        domain = {'x': [0, 1], 'y': [0, 1]},
        title = {'text': title},
        delta = {'reference': max_value * 0.8},
        gauge = {
            'axis': {'range': [None, max_value]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [0, max_value * 0.6], 'color': "lightgray"},
                {'range': [max_value * 0.6, max_value * 0.8], 'color': "gray"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': max_value * 0.9
            }
        }
    ))
    
    return fig

def create_waterfall_chart(categories, values, title="Waterfall Chart"):
    """Create a waterfall chart"""
    fig = go.Figure(go.Waterfall(
        name = title,
        orientation = "v",
        measure = ["relative"] * (len(categories) - 1) + ["total"],
        x = categories,
        textposition = "outside",
        text = [format_currency(v) for v in values],
        y = values,
        connector = {"line":{"color":"rgb(63, 63, 63)"}},
    ))
    
    fig.update_layout(title=title, showlegend=False)
    
    return fig

def create_funnel_chart(df, x_col, y_col, title="Funnel Chart"):
    """Create a funnel chart"""
    fig = go.Figure(go.Funnel(
        y = df[y_col],
        x = df[x_col],
        textinfo = "value+percent initial"
    ))
    
    fig.update_layout(title=title)
    
    return fig

def create_multi_line_chart(df, x_col, y_cols, title="Multi-Line Chart"):
    """Create a multi-line chart"""
    fig = go.Figure()
    
    colors = px.colors.qualitative.Set1
    
    for i, y_col in enumerate(y_cols):
        fig.add_trace(go.Scatter(
            x=df[x_col],
            y=df[y_col],
            mode='lines+markers',
            name=y_col,
            line=dict(color=colors[i % len(colors)], width=2),
            marker=dict(size=4)
        ))
    
    fig.update_layout(
        title=title,
        hovermode='x unified',
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig

def create_combo_chart(df, x_col, y1_col, y2_col, title="Combo Chart"):
    """Create a combination chart with dual y-axes"""
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Add bar chart
    fig.add_trace(
        go.Bar(x=df[x_col], y=df[y1_col], name=y1_col),
        secondary_y=False,
    )
    
    # Add line chart
    fig.add_trace(
        go.Scatter(x=df[x_col], y=df[y2_col], mode='lines+markers', name=y2_col),
        secondary_y=True,
    )
    
    # Update layout
    fig.update_layout(title=title)
    fig.update_yaxes(title_text=y1_col, secondary_y=False)
    fig.update_yaxes(title_text=y2_col, secondary_y=True)
    
    return fig

def create_box_plot(df, x_col, y_col, title="Box Plot"):
    """Create a box plot"""
    fig = px.box(df, x=x_col, y=y_col, title=title)
    
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig

def create_violin_plot(df, x_col, y_col, title="Violin Plot"):
    """Create a violin plot"""
    fig = px.violin(df, x=x_col, y=y_col, title=title, box=True)
    
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig

def apply_custom_theme(fig, theme="default"):
    """Apply custom theme to plotly figure"""
    themes = {
        "default": {
            "plot_bgcolor": "rgba(0,0,0,0)",
            "paper_bgcolor": "rgba(0,0,0,0)",
            "font_family": "Arial, sans-serif",
            "font_color": "#2E2E2E",
            "gridcolor": "#E5E5E5"
        },
        "dark": {
            "plot_bgcolor": "#2E2E2E",
            "paper_bgcolor": "#1E1E1E",
            "font_family": "Arial, sans-serif",
            "font_color": "#FFFFFF",
            "gridcolor": "#404040"
        },
        "minimal": {
            "plot_bgcolor": "rgba(0,0,0,0)",
            "paper_bgcolor": "rgba(0,0,0,0)",
            "font_family": "Helvetica, sans-serif",
            "font_color": "#333333",
            "gridcolor": "#F0F0F0",
            "showgrid": False
        }
    }
    
    if theme in themes:
        theme_config = themes[theme]
        fig.update_layout(
            plot_bgcolor=theme_config["plot_bgcolor"],
            paper_bgcolor=theme_config["paper_bgcolor"],
            font_family=theme_config["font_family"],
            font_color=theme_config["font_color"]
        )
        
        if "gridcolor" in theme_config:
            fig.update_xaxes(gridcolor=theme_config["gridcolor"])
            fig.update_yaxes(gridcolor=theme_config["gridcolor"])
        
        if theme_config.get("showgrid") is False:
            fig.update_xaxes(showgrid=False)
            fig.update_yaxes(showgrid=False)
    
    return fig

def add_annotations(fig, annotations):
    """Add annotations to plotly figure"""
    for annotation in annotations:
        fig.add_annotation(
            x=annotation.get("x"),
            y=annotation.get("y"),
            text=annotation.get("text"),
            showarrow=annotation.get("showarrow", True),
            arrowhead=annotation.get("arrowhead", 2),
            arrowsize=annotation.get("arrowsize", 1),
            arrowwidth=annotation.get("arrowwidth", 2),
            arrowcolor=annotation.get("arrowcolor", "#636363"),
            font=dict(
                family=annotation.get("font_family", "Arial, sans-serif"),
                size=annotation.get("font_size", 12),
                color=annotation.get("font_color", "#2E2E2E")
            )
        )
    
    return fig