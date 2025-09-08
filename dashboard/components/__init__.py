#!/usr/bin/env python3
"""
Dashboard components package.

This package contains reusable UI components for the SentiCheck dashboard.
"""

from .keyword_selector import render_keyword_selector
from .insights_section import render_insights_section

__all__ = ["render_keyword_selector", "render_insights_section"]