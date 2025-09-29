"""
config.py
Centralized configuration for the taxi_drivers project.
All cross-folder options and paths should be defined here.
"""
import os

# Base directory of the project
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Weekly Data directories
DATA_DIR = os.path.join(BASE_DIR, 'data')
ETL_DATA_DIR = os.path.join(BASE_DIR, 'etl', 'data')
ETL_OUTPUT_DIR = os.path.join(BASE_DIR, 'etl', 'output')
INPUT_DIR = os.path.join(BASE_DIR, 'data')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')

# Weekly directories
WEEKLY_INPUT_DIR = os.path.join(INPUT_DIR, 'weekly_data')
WEEKLY_OUTPUT_DIR = os.path.join(OUTPUT_DIR, 'weekly')


# Add more config options as needed
WEEKLY_REPORT_INPUT_FILENAME = os.path.join(WEEKLY_INPUT_DIR, 'week_di_test/Pagine_e_schermate_Percorso_pagina_e_classe_schermata (1).csv')
WEEKLY_REPORT_OUTPUT_FILENAME = os.path.join(WEEKLY_OUTPUT_DIR, 'weekly_report.csv')

# GA4 METRICS
WEEKLY_REPORT_METRICS = ["activeUsers", "screenPageViews", 'engagementRate', 'bounceRate', 'averageSessionDuration']

# OTHER PARAMETERS
WEEKLY_REPORT_DATA_RANGE = ("2025-09-17", "2025-09-23")


# Monthly directories
MONTHLY_OUTPUT_DIR = os.path.join(OUTPUT_DIR, 'monthly')

# MONTHLY PARAMETERS
MONTHLY_PARAMETERS_MONTH = "August"
MONTHLY_REPORT_METRICS = ["activeUsers", "screenPageViews", 'engagementRate', 'bounceRate', 'averageSessionDuration']