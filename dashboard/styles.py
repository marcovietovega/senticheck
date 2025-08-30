#!/usr/bin/env python3

import streamlit as st


def apply_clean_styles():
    """Apply styling to the dashboard"""
    st.markdown(
        """
        <style>

        .metric-card {
            background-color: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 16px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
            transition: box-shadow 0.2s ease;
            display: flex;
            flex-direction: column;
            justify-content: flex-start;
            align-items: center;
            text-align: center;
            height: 140px;
            width: 100%;
            margin: 0 auto 16px auto;
            position: relative;
            overflow: visible;
        }
        
        .metric-card:hover {
            box-shadow: 0 4px 12px rgba(0,0,0,0.08);
        }
        
        .metric-header {
            display: flex;
            justify-content: center;
            align-items: center;
            margin-bottom: 12px;
            width: 100%;
            position: relative;
        }
        
        .metric-title {
            font-size: 14px;
            font-weight: 500;
            color: #6b7280;
            text-align: center;
            flex-grow: 1;
            line-height: 1.4;
        }
        
        .metric-help-container {
            position: absolute;
            right: 0;
            top: 50%;
            transform: translateY(-50%);
            z-index: 10001;
        }
        
        .metric-help {
            width: 16px;
            height: 16px;
            border-radius: 50%;
            background-color: #f3f4f6;
            color: #6b7280;
            font-size: 10px;
            font-weight: bold;
            text-align: center;
            line-height: 16px;
            cursor: help;
            opacity: 0.7;
            display: block;
            transition: opacity 0.2s ease;
            position: relative;
        }
        
        .metric-help:hover {
            opacity: 1;
            background-color: #e5e7eb;
        }
        
        .metric-tooltip {
            position: absolute;
            bottom: 30px;
            right: -50px;
            background-color: #1f2937;
            color: #ffffff;
            padding: 10px 14px;
            border-radius: 8px;
            font-size: 12px;
            line-height: 1.4;
            width: 240px;
            z-index: 10002;
            opacity: 0;
            visibility: hidden;
            transform: translateY(10px);
            transition: all 0.3s ease;
            box-shadow: 0 10px 20px rgba(0,0,0,0.25);
            text-align: left;
            pointer-events: none;
        }
        
        .metric-tooltip::before {
            content: '';
            position: absolute;
            bottom: -6px;
            right: 60px;
            width: 12px;
            height: 12px;
            background-color: #1f2937;
            transform: rotate(45deg);
            z-index: -1;
        }
        
        .metric-help-container:hover .metric-tooltip {
            opacity: 1;
            visibility: visible;
            transform: translateY(0);
        }
        
        .metric-value {
            font-size: 28px;
            font-weight: 600;
            color: #111827;
            margin-bottom: 12px;
            line-height: 1.1;
            text-align: center;
        }
        
        .metric-delta {
            font-size: 12px;
            font-weight: 500;
            padding: 4px 8px;
            border-radius: 12px;
            display: inline-block;
            text-align: center;
            line-height: 1.2;
        }
        
        .metric-delta-positive {
            background-color: #d1fae5;
            color: #059669;
        }
        
        .metric-delta-negative {
            background-color: #fee2e2;
            color: #dc2626;
        }
        
        .metric-delta-neutral {
            background-color: #f3f4f6;
            color: #6b7280;
        }
        
        h2 {
            margin: 2rem 0 0 0 !important;
            padding: 0 !important;
        }
        
        hr {
            margin: 0.5rem 0 2rem 0px !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def apply_dropdown_styles():
    """Apply Streamlit dropdown styling"""
    pass
