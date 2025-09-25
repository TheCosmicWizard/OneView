
# Real-Time Analytics Dashboard System - Fixed Version
# Complete implementation with CSV/Excel/Google Sheets integration and real-time updates

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlite3
import time
import threading
import hashlib
import json
import os
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
from io import BytesIO, StringIO
import schedule
from typing import List, Dict, Optional, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Handles all database operations"""
    
    def __init__(self, db_path: str = "analytics_data.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize database with necessary tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Data sources table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS data_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_name TEXT NOT NULL,
                connection_config TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1,
                auto_refresh BOOLEAN DEFAULT 1
            )
        ''')
        
        # Dynamic data table (stores actual metrics data)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS analytics_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER,
                data_hash TEXT,
                raw_data TEXT,
                processed_data TEXT,
                time_column TEXT,
                metric_columns TEXT,
                record_count INTEGER DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (source_id) REFERENCES data_sources (id)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def save_data_source(self, user_id: str, source_type: str, source_name: str, 
                        config: Dict = None, auto_refresh: bool = True) -> int:
        """Save data source configuration"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        config_json = json.dumps(config) if config else None
        cursor.execute('''
            INSERT INTO data_sources (user_id, source_type, source_name, connection_config, auto_refresh)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, source_type, source_name, config_json, auto_refresh))
        
        source_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return source_id
    
    def save_analytics_data(self, source_id: int, df: pd.DataFrame, 
                          time_col: str, metric_cols: List[str]):
        """Save processed analytics data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Create data hash for change detection
            data_hash = hashlib.md5(df.to_string().encode()).hexdigest()
            
            # Check if data exists for this source
            cursor.execute('SELECT id, data_hash FROM analytics_data WHERE source_id = ?', (source_id,))
            result = cursor.fetchone()
            
            if result and result[1] == data_hash:
                conn.close()
                return False  # No changes
            
            # Prepare data for storage
            df_for_json = df.copy()
            if time_col in df_for_json.columns:
                df_for_json[time_col] = df_for_json[time_col].astype(str)
            
            raw_data = df_for_json.to_json(orient='records', date_format='iso')
            processed_data = json.dumps({
                'time_column': time_col,
                'metric_columns': metric_cols,
                'data_types': {k: str(v) for k, v in df.dtypes.to_dict().items()}
            })
            
            if result:  # Update existing
                cursor.execute('''
                    UPDATE analytics_data 
                    SET data_hash = ?, raw_data = ?, processed_data = ?, 
                        time_column = ?, metric_columns = ?, record_count = ?, 
                        last_updated = CURRENT_TIMESTAMP
                    WHERE source_id = ?
                ''', (data_hash, raw_data, processed_data, time_col, 
                      json.dumps(metric_cols), len(df), source_id))
            else:  # Insert new
                cursor.execute('''
                    INSERT INTO analytics_data 
                    (source_id, data_hash, raw_data, processed_data, time_column, metric_columns, record_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (source_id, data_hash, raw_data, processed_data, time_col, 
                      json.dumps(metric_cols), len(df)))
            
            # Update source last_updated
            cursor.execute('''
                UPDATE data_sources SET last_updated = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (source_id,))
            
            conn.commit()
            logger.info(f"Successfully saved data for source_id: {source_id}, records: {len(df)}")
            return True  # Data updated
            
        except Exception as e:
            logger.error(f"Error saving analytics data: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def get_analytics_data(self, source_id: int) -> Optional[Tuple[pd.DataFrame, str, List[str]]]:
        """Retrieve analytics data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT raw_data, time_column, metric_columns 
                FROM analytics_data WHERE source_id = ?
            ''', (source_id,))
            
            result = cursor.fetchone()
            
            if not result or not result[0]:
                logger.warning(f"No data found for source_id: {source_id}")
                return None
            
            # Parse the data
            df = pd.read_json(result[0], orient='records')
            time_col = result[1]
            metric_cols = json.loads(result[2])
            
            # Convert time column back to datetime
            if time_col in df.columns:
                df[time_col] = pd.to_datetime(df[time_col])
            
            logger.info(f"Successfully retrieved data for source_id: {source_id}, rows: {len(df)}")
            return df, time_col, metric_cols
            
        except Exception as e:
            logger.error(f"Error retrieving analytics data for source_id {source_id}: {e}")
            return None
        finally:
            conn.close()
    
    def get_user_sources(self, user_id: str) -> List[Dict]:
        """Get all data sources for a user"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT ds.id, ds.source_type, ds.source_name, ds.last_updated, ds.is_active,
                   ds.connection_config, ds.auto_refresh, ad.record_count
            FROM data_sources ds
            LEFT JOIN analytics_data ad ON ds.id = ad.source_id
            WHERE ds.user_id = ? AND ds.is_active = 1
        ''', (user_id,))
        
        results = cursor.fetchall()
        conn.close()
        
        sources = []
        for r in results:
            config = json.loads(r[5]) if r[5] else {}
            sources.append({
                'id': r[0], 
                'type': r[1], 
                'name': r[2], 
                'last_updated': r[3], 
                'active': r[4],
                'config': config,
                'auto_refresh': r[6],
                'record_count': r[7] or 0
            })
        
        return sources

class DataProcessor:
    """Handles data processing and validation"""
    
    @staticmethod
    def detect_time_column(df: pd.DataFrame) -> Optional[str]:
        """Automatically detect time/date column"""
        # Check for common time column names first
        time_indicators = ['date', 'time', 'timestamp', 'created_at', 'updated_at', 
                          'day', 'week', 'month', 'year', 'datetime']
        
        for col in df.columns:
            col_lower = col.lower()
            if any(indicator in col_lower for indicator in time_indicators):
                try:
                    pd.to_datetime(df[col].dropna().head(10))
                    return col
                except:
                    continue
        
        # If no obvious time column, try all columns
        for col in df.columns:
            try:
                pd.to_datetime(df[col].dropna().head(10))
                return col
            except:
                continue
        return None
    
    @staticmethod
    def detect_metric_columns(df: pd.DataFrame, time_col: str) -> List[str]:
        """Detect numeric columns for metrics"""
        metric_cols = []
        for col in df.columns:
            if col != time_col and pd.api.types.is_numeric_dtype(df[col]):
                metric_cols.append(col)
        return metric_cols
    
    @staticmethod
    def process_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, str, List[str]]:
        """Process uploaded dataframe"""
        if df.empty:
            raise ValueError("DataFrame is empty")
            
        # Clean column names
        df.columns = df.columns.str.strip()
        
        # Remove completely empty rows
        df = df.dropna(how='all').reset_index(drop=True)
        
        if df.empty:
            raise ValueError("No valid data rows found")
        
        # Detect time column
        time_col = DataProcessor.detect_time_column(df)
        if not time_col:
            raise ValueError("No time/date column detected. Please ensure your data has a date/time column.")
        
        # Convert time column to datetime
        try:
            df[time_col] = pd.to_datetime(df[time_col])
        except Exception as e:
            raise ValueError(f"Could not convert '{time_col}' to datetime: {e}")
        
        # Detect metric columns
        metric_cols = DataProcessor.detect_metric_columns(df, time_col)
        if not metric_cols:
            raise ValueError("No numeric metric columns detected. Please ensure your data has numeric columns for analysis.")
        
        # Sort by time column
        df = df.sort_values(by=time_col).reset_index(drop=True)
        
        # Remove rows with invalid time values
        df = df.dropna(subset=[time_col]).reset_index(drop=True)
        
        return df, time_col, metric_cols

class GoogleSheetsConnector:
    """Handles Google Sheets integration"""
    
    def __init__(self):
        self.credentials = None
        self.client = None
    
    def authenticate(self, credentials_json: Dict) -> bool:
        """Authenticate with Google Sheets API"""
        try:
            scope = ['https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive']
            
            self.credentials = Credentials.from_service_account_info(
                credentials_json, scopes=scope)
            self.client = gspread.authorize(self.credentials)
            return True
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            return False
    
    def test_connection(self, sheet_url: str) -> bool:
        """Test if sheet is accessible"""
        try:
            if not self.client:
                return False
            
            sheet = self.client.open_by_url(sheet_url)
            worksheet = sheet.get_worksheet(0)
            worksheet.get_all_records(head=1)  # Test read
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def fetch_data(self, sheet_url: str) -> pd.DataFrame:
        """Fetch data from Google Sheet"""
        try:
            sheet = self.client.open_by_url(sheet_url)
            worksheet = sheet.get_worksheet(0)
            data = worksheet.get_all_records()
            df = pd.DataFrame(data)
            
            if df.empty:
                raise ValueError("Google Sheet is empty")
                
            return df
        except Exception as e:
            logger.error(f"Data fetch failed: {e}")
            raise

class RealTimeUpdater:
    """Handles real-time data updates"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.google_connector = GoogleSheetsConnector()
        self.update_thread = None
        self.running = False
        self.update_interval = 10  # seconds
    
    def start_updates(self):
        """Start the update thread"""
        if not self.running:
            self.running = True
            self.update_thread = threading.Thread(target=self._update_loop)
            self.update_thread.daemon = True
            self.update_thread.start()
            logger.info("Real-time updater started")
    
    def stop_updates(self):
        """Stop the update thread"""
        self.running = False
        logger.info("Real-time updater stopped")
    
    def _update_loop(self):
        """Main update loop"""
        while self.running:
            try:
                self._check_for_updates()
                time.sleep(self.update_interval)
            except Exception as e:
                logger.error(f"Update loop error: {e}")
                time.sleep(self.update_interval)
    
    def _check_for_updates(self):
        """Check all data sources for updates"""
        try:
            # Get all sources that need auto-refresh
            sources = self.db_manager.get_user_sources("demo_user")  # In production, track all users
            
            for source in sources:
                if not source['auto_refresh'] or source['type'] not in ['google_sheets']:
                    continue
                
                try:
                    self._update_google_sheets_source(source)
                except Exception as e:
                    logger.error(f"Error updating source {source['id']}: {e}")
                    
        except Exception as e:
            logger.error(f"Error in _check_for_updates: {e}")
    
    def _update_google_sheets_source(self, source: Dict):
        """Update a Google Sheets data source"""
        try:
            config = source['config']
            if 'credentials' not in config or 'sheet_url' not in config:
                return
            
            # Authenticate
            connector = GoogleSheetsConnector()
            if not connector.authenticate(config['credentials']):
                logger.error(f"Failed to authenticate for source {source['id']}")
                return
            
            # Fetch fresh data
            df = connector.fetch_data(config['sheet_url'])
            processed_df, time_col, metric_cols = DataProcessor.process_dataframe(df)
            
            # Save updated data
            updated = self.db_manager.save_analytics_data(
                source['id'], processed_df, time_col, metric_cols
            )
            
            if updated:
                logger.info(f"Updated Google Sheets source {source['id']} with {len(processed_df)} records")
            
        except Exception as e:
            logger.error(f"Error updating Google Sheets source {source['id']}: {e}")

class DashboardVisualizer:
    """Handles dashboard visualizations"""
    
    @staticmethod
    def create_time_series_chart(df: pd.DataFrame, time_col: str, 
                               metric_cols: List[str], chart_type: str = "line"):
        """Create time series visualization"""
        if df.empty:
            return go.Figure().add_annotation(text="No data available", showarrow=False)
            
        try:
            if chart_type == "line":
                fig = px.line(df, x=time_col, y=metric_cols, 
                             title="Metrics Over Time",
                             labels={time_col: "Time"})
            elif chart_type == "bar":
                # Use only first metric for bar chart
                fig = px.bar(df, x=time_col, y=metric_cols[0], 
                            title=f"{metric_cols[0]} Over Time",
                            labels={time_col: "Time"})
            else:  # dual-axis
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                
                fig.add_trace(
                    go.Scatter(x=df[time_col], y=df[metric_cols[0]], 
                              name=metric_cols[0], mode='lines'),
                    secondary_y=False,
                )
                
                if len(metric_cols) > 1:
                    fig.add_trace(
                        go.Scatter(x=df[time_col], y=df[metric_cols[1]], 
                                  name=metric_cols[1], mode='lines'),
                        secondary_y=True,
                    )
                    
                    fig.update_yaxes(title_text=metric_cols[0], secondary_y=False)
                    fig.update_yaxes(title_text=metric_cols[1], secondary_y=True)
                
                fig.update_xaxes(title_text="Time")
                fig.update_layout(title="Metrics Over Time (Dual Axis)")
            
            fig.update_layout(height=500, showlegend=True)
            return fig
            
        except Exception as e:
            logger.error(f"Chart creation error: {e}")
            return go.Figure().add_annotation(text=f"Error creating chart: {e}", showarrow=False)
    
    @staticmethod
    def create_custom_scatter_chart(df: pd.DataFrame, x_col: str, y_col: str, 
                                  time_col: str = None, title: str = "Custom Chart"):
        """Create customizable scatter/line chart with user-selected axes"""
        try:
            if df.empty or x_col not in df.columns or y_col not in df.columns:
                return go.Figure().add_annotation(text="Invalid column selection", showarrow=False)
            
            # If x_col is the time column, create a time series
            if time_col and x_col == time_col:
                fig = px.line(df, x=x_col, y=y_col, title=title,
                             labels={x_col: x_col, y_col: y_col},
                             markers=True)
            else:
                # Create scatter plot for non-time x-axis
                fig = px.scatter(df, x=x_col, y=y_col, title=title,
                               labels={x_col: x_col, y_col: y_col},
                               trendline="ols" if pd.api.types.is_numeric_dtype(df[x_col]) else None)
            
            fig.update_layout(height=400, showlegend=True)
            return fig
            
        except Exception as e:
            logger.error(f"Custom chart creation error: {e}")
            return go.Figure().add_annotation(text=f"Error creating chart: {e}", showarrow=False)
    
    @staticmethod
    def create_comparison_chart(df: pd.DataFrame, metric_cols: List[str]):
        """Create metric comparison chart"""
        try:
            if df.empty:
                return go.Figure().add_annotation(text="No data available", showarrow=False)
                
            summary_data = []
            for col in metric_cols:
                if col in df.columns:
                    summary_data.append({
                        'Metric': col,
                        'Total': df[col].sum(),
                        'Average': df[col].mean(),
                        'Max': df[col].max(),
                        'Min': df[col].min()
                    })
            
            if not summary_data:
                return go.Figure().add_annotation(text="No valid metrics found", showarrow=False)
            
            summary_df = pd.DataFrame(summary_data)
            fig = px.bar(summary_df, x='Metric', y='Total', 
                        title='Metric Totals Comparison',
                        text='Total')
            
            fig.update_traces(texttemplate='%{text:.2f}', textposition='outside')
            fig.update_layout(height=400)
            return fig
            
        except Exception as e:
            logger.error(f"Comparison chart error: {e}")
            return go.Figure().add_annotation(text=f"Error creating chart: {e}", showarrow=False)

def main():
    """Main Streamlit application"""
    st.set_page_config(
        page_title="Real-Time Analytics Dashboard",
        page_icon="📊",
        layout="wide"
    )
    
    # Initialize session state
    if 'db_manager' not in st.session_state:
        st.session_state.db_manager = DatabaseManager()
    if 'updater' not in st.session_state:
        st.session_state.updater = RealTimeUpdater(st.session_state.db_manager)
        st.session_state.updater.start_updates()
    if 'user_id' not in st.session_state:
        st.session_state.user_id = "demo_user"
    if 'last_refresh' not in st.session_state:
        st.session_state.last_refresh = time.time()
    # Initialize metric selection states
    if 'selected_metrics' not in st.session_state:
        st.session_state.selected_metrics = []
    if 'custom_x_axis' not in st.session_state:
        st.session_state.custom_x_axis = None
    if 'custom_y_axis' not in st.session_state:
        st.session_state.custom_y_axis = None
    
    st.title("📊 Real-Time Analytics Dashboard")
    st.markdown("Connect your data sources and watch your metrics update automatically!")
    
    # Auto-refresh mechanism
    if st.button("🔄 Force Refresh", help="Manually refresh the dashboard"):
        st.session_state.last_refresh = time.time()
        st.rerun()
    
    # Sidebar for data source management
    with st.sidebar:
        st.header("📂 Data Sources")
        
        source_type = st.selectbox(
            "Choose Data Source",
            ["Upload CSV File", "Upload Excel File", "Connect Google Sheets"]
        )
        
        if source_type == "Upload CSV File":
            handle_csv_upload()
        elif source_type == "Upload Excel File":
            handle_excel_upload()
        else:
            handle_google_sheets_connection()
        
        # Show existing sources
        st.subheader("Connected Sources")
        sources = st.session_state.db_manager.get_user_sources(st.session_state.user_id)
        
        if sources:
            for source in sources:
                with st.expander(f"📊 {source['name']} ({source['type']})", expanded=False):
                    st.write(f"**Type:** {source['type']}")
                    st.write(f"**Records:** {source['record_count']}")
                    st.write(f"**Last Updated:** {source['last_updated']}")
                    st.write(f"**Auto-Refresh:** {'✅' if source['auto_refresh'] else '❌'}")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button(f"📈 View", key=f"view_{source['id']}"):
                            st.session_state.selected_source = source['id']
                            # Reset selections when switching sources
                            st.session_state.selected_metrics = []
                            st.session_state.custom_x_axis = None
                            st.session_state.custom_y_axis = None
                            st.rerun()
                    with col2:
                        if st.button(f"🔄 Refresh", key=f"refresh_{source['id']}"):
                            refresh_single_source(source['id'])
            
            # Quick selector for active source
            if 'selected_source' in st.session_state:
                current_source = next((s for s in sources if s['id'] == st.session_state.selected_source), None)
                if current_source:
                    st.success(f"🎯 Active: {current_source['name']}")
        else:
            st.info("No data sources connected yet")
    
    # Main dashboard area
    if 'selected_source' in st.session_state:
        display_dashboard(st.session_state.selected_source)
    else:
        display_welcome_screen()

def handle_csv_upload():
    """Handle CSV file upload"""
    uploaded_file = st.file_uploader(
        "Upload CSV File", 
        type=['csv'],
        help="Upload your CSV file with time series data"
    )
    
    if uploaded_file:
        try:
            # Try different encodings
            content = uploaded_file.read()
            
            # Try UTF-8 first
            try:
                csv_string = content.decode('utf-8')
            except UnicodeDecodeError:
                # Try other common encodings
                for encoding in ['latin-1', 'cp1252', 'iso-8859-1']:
                    try:
                        csv_string = content.decode(encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    st.error("Could not decode the CSV file. Please check the file encoding.")
                    return
            
            # Read CSV
            df = pd.read_csv(StringIO(csv_string))
            st.success(f"File uploaded: {uploaded_file.name}")
            
            # Preview data
            st.subheader("Data Preview")
            st.dataframe(df.head(), use_container_width=True)
            
            # Process data
            processed_df, time_col, metric_cols = DataProcessor.process_dataframe(df)
            
            st.info(f"✅ Time column detected: **{time_col}**")
            st.info(f"✅ Metric columns detected: **{', '.join(metric_cols)}**")
            
            if st.button("Save Data Source", key="save_csv"):
                with st.spinner("Saving data source..."):
                    save_file_source(uploaded_file.name, "csv", processed_df, time_col, metric_cols)
                
        except Exception as e:
            st.error(f"Error processing CSV file: {e}")
            logger.error(f"Error processing CSV file: {e}")

def handle_excel_upload():
    """Handle Excel file upload"""
    uploaded_file = st.file_uploader(
        "Upload Excel File", 
        type=['xlsx', 'xls'],
        help="Upload your Excel file with time series data"
    )
    
    if uploaded_file:
        try:
            # Read Excel file
            df = pd.read_excel(uploaded_file)
            st.success(f"File uploaded: {uploaded_file.name}")
            
            # Preview data
            st.subheader("Data Preview")
            st.dataframe(df.head(), use_container_width=True)
            
            # Process data
            processed_df, time_col, metric_cols = DataProcessor.process_dataframe(df)
            
            st.info(f"✅ Time column detected: **{time_col}**")
            st.info(f"✅ Metric columns detected: **{', '.join(metric_cols)}**")
            
            if st.button("Save Data Source", key="save_excel"):
                with st.spinner("Saving data source..."):
                    save_file_source(uploaded_file.name, "excel", processed_df, time_col, metric_cols)
                
        except Exception as e:
            st.error(f"Error processing Excel file: {e}")
            logger.error(f"Error processing Excel file: {e}")

def save_file_source(filename: str, file_type: str, df: pd.DataFrame, time_col: str, metric_cols: List[str]):
    """Save uploaded file as data source"""
    try:
        # Save to database
        source_id = st.session_state.db_manager.save_data_source(
            st.session_state.user_id,
            file_type,
            filename,
            auto_refresh=False  # File uploads don't auto-refresh
        )
        
        # Save the data
        success = st.session_state.db_manager.save_analytics_data(
            source_id, df, time_col, metric_cols
        )
        
        if success:
            st.success(f"✅ {file_type.upper()} data source saved successfully!")
            st.session_state.selected_source = source_id
            # Reset metric selections for new source
            st.session_state.selected_metrics = []
            st.session_state.custom_x_axis = None
            st.session_state.custom_y_axis = None
            time.sleep(1)
            st.rerun()
        else:
            st.warning("Data source already exists with same data")
            
    except Exception as e:
        st.error(f"Error saving data source: {e}")
        logger.error(f"Error saving {file_type} source: {e}")

def handle_google_sheets_connection():
    """Handle Google Sheets connection"""
    st.subheader("Google Sheets Setup")
    
    # Credentials upload
    creds_file = st.file_uploader(
        "Upload credentials.json",
        type=['json'],
        help="Upload your Google Service Account credentials"
    )
    
    sheet_url = st.text_input(
        "Google Sheet URL",
        placeholder="https://docs.google.com/spreadsheets/d/..."
    )
    
    if creds_file and sheet_url:
        try:
            # Parse credentials
            creds_data = json.loads(creds_file.read().decode())
            
            # Test connection
            connector = GoogleSheetsConnector()
            
            with st.spinner("Testing connection..."):
                if connector.authenticate(creds_data):
                    if connector.test_connection(sheet_url):
                        st.success("✅ Connection successful!")
                        
                        # Fetch and preview data
                        df = connector.fetch_data(sheet_url)
                        st.subheader("Data Preview")
                        st.dataframe(df.head(), use_container_width=True)
                        
                        # Process data
                        processed_df, time_col, metric_cols = DataProcessor.process_dataframe(df)
                        
                        st.info(f"✅ Time column detected: **{time_col}**")
                        st.info(f"✅ Metric columns detected: **{', '.join(metric_cols)}**")
                        
                        if st.button("Save Connection", key="save_sheets"):
                            with st.spinner("Saving Google Sheets connection..."):
                                try:
                                    # Save connection config
                                    config = {
                                        'credentials': creds_data,
                                        'sheet_url': sheet_url
                                    }
                                    
                                    source_id = st.session_state.db_manager.save_data_source(
                                        st.session_state.user_id,
                                        "google_sheets",
                                        f"Google Sheets - {sheet_url.split('/')[-2][:10]}...",
                                        config,
                                        auto_refresh=True  # Google Sheets can auto-refresh
                                    )
                                    
                                    success = st.session_state.db_manager.save_analytics_data(
                                        source_id, processed_df, time_col, metric_cols
                                    )
                                    
                                    if success:
                                        st.success("✅ Google Sheets connection saved!")
                                        st.session_state.selected_source = source_id
                                        time.sleep(1)
                                        st.rerun()
                                    else:
                                        st.warning("Connection already exists with same data")
                                        
                                except Exception as e:
                                    st.error(f"Error saving connection: {e}")
                                    logger.error(f"Error saving Google Sheets connection: {e}")
                    else:
                        st.error("❌ Cannot access the sheet. Check URL and permissions.")
                else:
                    st.error("❌ Authentication failed. Check your credentials.")
        except Exception as e:
            st.error(f"Connection error: {e}")

def refresh_single_source(source_id: int):
    """Refresh a single data source manually"""
    try:
        sources = st.session_state.db_manager.get_user_sources(st.session_state.user_id)
        source = next((s for s in sources if s['id'] == source_id), None)
        
        if not source:
            st.error("Source not found")
            return
        
        if source['type'] == 'google_sheets':
            with st.spinner("Refreshing Google Sheets data..."):
                config = source['config']
                if 'credentials' in config and 'sheet_url' in config:
                    connector = GoogleSheetsConnector()
                    if connector.authenticate(config['credentials']):
                        df = connector.fetch_data(config['sheet_url'])
                        processed_df, time_col, metric_cols = DataProcessor.process_dataframe(df)
                        
                        updated = st.session_state.db_manager.save_analytics_data(
                            source_id, processed_df, time_col, metric_cols
                        )
                        
                        if updated:
                            st.success(f"✅ Refreshed with {len(processed_df)} records")
                        else:
                            st.info("📊 No changes detected")
                    else:
                        st.error("❌ Authentication failed")
                else:
                    st.error("❌ Invalid configuration")
        else:
            st.info("📁 File sources don't support refresh. Please re-upload the file.")
            
    except Exception as e:
        st.error(f"Error refreshing source: {e}")
        logger.error(f"Error refreshing source {source_id}: {e}")

def display_dashboard(source_id: int):
    """Display the main dashboard"""
    try:
        # Check for auto-refresh
        current_time = time.time()
        if current_time - st.session_state.last_refresh > 10:  # Auto-refresh every 10 seconds
            st.session_state.last_refresh = current_time
            st.rerun()
        
        # Add refresh countdown
        time_since_refresh = int(current_time - st.session_state.last_refresh)
        next_refresh = max(0, 10 - time_since_refresh)
        
        data_result = st.session_state.db_manager.get_analytics_data(source_id)
        if not data_result:
            st.error(f"❌ No data found for source ID: {source_id}")
            
            # Debug: Check if source exists
            sources = st.session_state.db_manager.get_user_sources(st.session_state.user_id)
            source_exists = any(s['id'] == source_id for s in sources)
            
            if not source_exists:
                st.error("❌ Source ID does not exist")
                if st.button("🔙 Back to Sources"):
                    if 'selected_source' in st.session_state:
                        del st.session_state.selected_source
                    st.rerun()
            else:
                st.info("✅ Source exists but no analytics data found")
                st.info("💡 Try re-uploading your data or check the connection")
            return
        
        df, time_col, metric_cols = data_result
        
        if df.empty:
            st.warning("⚠️ Data source exists but contains no records")
            return
        
        # Get source info
        sources = st.session_state.db_manager.get_user_sources(st.session_state.user_id)
        current_source = next((s for s in sources if s['id'] == source_id), None)
        
        # Dashboard header
        col1, col2, col3, col4, col5 = st.columns([2, 1, 1, 1, 1])
        with col1:
            st.header("📈 Live Dashboard")
            if current_source:
                st.caption(f"Source: {current_source['name']}")
        with col2:
            st.metric("Total Records", len(df))
        with col3:
            st.metric("Metrics Available", len(metric_cols))
        with col4:
            if current_source and current_source['auto_refresh']:
                st.metric("Next Refresh", f"{next_refresh}s")
            else:
                st.metric("Auto-Refresh", "Off")
        with col5:
            if st.button("🔄 Refresh Now"):
                refresh_single_source(source_id)
                st.rerun()
        
        # Show data info
        time_range_str = f"{df[time_col].min().strftime('%Y-%m-%d')} to {df[time_col].max().strftime('%Y-%m-%d')}"
        st.info(f"📅 Time Range: {time_range_str} | Last Updated: {current_source['last_updated'] if current_source else 'Unknown'}")
        
        # Metric selection
        st.subheader("Select Metrics to Display")
        col1, col2 = st.columns([3, 1])
        
        with col1:
            selected_metrics = st.multiselect(
                "Choose metrics (2-3 recommended for best visualization)",
                metric_cols,
                default=metric_cols[:min(3, len(metric_cols))],
                help="Select the metrics you want to analyze and compare"
            )
        
        with col2:
            if st.button("📊 Select All Metrics"):
                selected_metrics = metric_cols
                st.rerun()
        
        if not selected_metrics:
            st.warning("Please select at least one metric to display")
            return
        
        # Chart and filter options
        col1, col2, col3 = st.columns(3)
        with col1:
            chart_type = st.selectbox(
                "Chart Type",
                ["line", "bar", "dual-axis"],
                help="Choose how to visualize your data"
            )
        with col2:
            time_range = st.selectbox(
                "Time Range",
                ["All", "Last 7 Days", "Last 30 Days", "Last 90 Days"],
                help="Filter data by time period"
            )
        with col3:
            show_trend = st.checkbox("Show Trend Line", help="Add trend lines to charts")
        
        # Filter data by time range
        filtered_df = filter_by_time_range(df, time_col, time_range)
        
        if filtered_df.empty:
            st.warning(f"⚠️ No data available for selected time range: {time_range}")
            st.info("Try selecting 'All' or a different time range")
            return
        
        # Main visualization
        st.subheader("📊 Metrics Over Time")
        
        try:
            chart = DashboardVisualizer.create_time_series_chart(
                filtered_df, time_col, selected_metrics, chart_type
            )
            
            # Add trend lines if requested
            if show_trend and chart_type == "line":
                for i, metric in enumerate(selected_metrics):
                    if metric in filtered_df.columns:
                        # Simple linear trend
                        x_numeric = pd.to_numeric(filtered_df[time_col])
                        z = np.polyfit(x_numeric, filtered_df[metric], 1)
                        p = np.poly1d(z)
                        chart.add_trace(
                            go.Scatter(
                                x=filtered_df[time_col],
                                y=p(x_numeric),
                                mode='lines',
                                name=f'{metric} Trend',
                                line=dict(dash='dash', width=2),
                                opacity=0.7
                            )
                        )
            
            st.plotly_chart(chart, use_container_width=True)
        except Exception as e:
            st.error(f"Error creating main chart: {e}")
            logger.error(f"Chart creation error: {e}")
        
        # Additional charts and metrics
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📈 Metric Comparison")
            if len(selected_metrics) > 1:
                try:
                    comparison_chart = DashboardVisualizer.create_comparison_chart(
                        filtered_df, selected_metrics
                    )
                    st.plotly_chart(comparison_chart, use_container_width=True)
                except Exception as e:
                    st.error(f"Error creating comparison chart: {e}")
            else:
                st.info("Select multiple metrics to see comparison")
        
        with col2:
            st.subheader("📋 Summary Statistics")
            
            # Create a summary table
            summary_data = []
            for metric in selected_metrics:
                if metric in filtered_df.columns:
                    try:
                        current_val = filtered_df[metric].iloc[-1] if len(filtered_df) > 0 else 0
                        prev_val = filtered_df[metric].iloc[-2] if len(filtered_df) > 1 else current_val
                        change = current_val - prev_val if len(filtered_df) > 1 else 0
                        change_pct = (change / prev_val * 100) if prev_val != 0 else 0
                        
                        summary_data.append({
                            'Metric': metric,
                            'Current': f"{current_val:.2f}",
                            'Change': f"{change:+.2f}",
                            'Change %': f"{change_pct:+.1f}%",
                            'Total': f"{filtered_df[metric].sum():.2f}",
                            'Average': f"{filtered_df[metric].mean():.2f}",
                            'Max': f"{filtered_df[metric].max():.2f}",
                            'Min': f"{filtered_df[metric].min():.2f}"
                        })
                    except Exception as e:
                        logger.error(f"Error calculating stats for {metric}: {e}")
                        summary_data.append({
                            'Metric': metric,
                            'Current': 'Error',
                            'Change': 'Error',
                            'Change %': 'Error',
                            'Total': 'Error',
                            'Average': 'Error',
                            'Max': 'Error',
                            'Min': 'Error'
                        })
            
            if summary_data:
                summary_df = pd.DataFrame(summary_data)
                st.dataframe(summary_df, use_container_width=True, hide_index=True)
        
        # Correlation analysis for multiple metrics
        if len(selected_metrics) > 1:
            st.subheader("🔗 Correlation Analysis")
            try:
                corr_data = filtered_df[selected_metrics].corr()
                
                # Create correlation heatmap
                fig_corr = px.imshow(
                    corr_data,
                    text_auto=True,
                    aspect="auto",
                    title="Metric Correlations",
                    color_continuous_scale='RdBu_r'
                )
                fig_corr.update_layout(height=400)
                st.plotly_chart(fig_corr, use_container_width=True)
                
                # Show strongest correlations
                strong_corr = []
                for i, metric1 in enumerate(selected_metrics):
                    for j, metric2 in enumerate(selected_metrics):
                        if i < j:  # Avoid duplicates
                            corr_val = corr_data.iloc[i, j]
                            if abs(corr_val) > 0.5:
                                strong_corr.append({
                                    'Metric 1': metric1,
                                    'Metric 2': metric2,
                                    'Correlation': f"{corr_val:.3f}",
                                    'Strength': 'Strong' if abs(corr_val) > 0.7 else 'Moderate'
                                })
                
                if strong_corr:
                    st.subheader("💪 Strong Correlations")
                    corr_df = pd.DataFrame(strong_corr)
                    st.dataframe(corr_df, use_container_width=True, hide_index=True)
                else:
                    st.info("No strong correlations found between selected metrics")
                    
            except Exception as e:
                st.error(f"Error creating correlation analysis: {e}")
        
        # Data table with search and filtering
        with st.expander("📄 Raw Data Explorer", expanded=False):
            st.subheader(f"Data Table ({len(filtered_df)} records)")
            
            # Search functionality
            search_term = st.text_input("🔍 Search in data", placeholder="Enter search term...")
            
            display_df = filtered_df[[time_col] + selected_metrics].copy()
            
            if search_term:
                # Search in all string columns
                mask = display_df.astype(str).apply(
                    lambda x: x.str.contains(search_term, case=False, na=False)
                ).any(axis=1)
                display_df = display_df[mask]
                st.info(f"Found {len(display_df)} records matching '{search_term}'")
            
            # Show pagination info
            if len(display_df) > 100:
                st.info(f"Showing first 100 of {len(display_df)} records. Use search to filter.")
                display_df = display_df.head(100)
            
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            # Download functionality
            col1, col2 = st.columns(2)
            with col1:
                csv_data = display_df.to_csv(index=False)
                st.download_button(
                    "📥 Download as CSV",
                    csv_data,
                    f"analytics_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    "text/csv"
                )
            with col2:
                excel_buffer = BytesIO()
                display_df.to_excel(excel_buffer, index=False)
                st.download_button(
                    "📥 Download as Excel",
                    excel_buffer.getvalue(),
                    f"analytics_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        
        # Status and refresh info
        st.divider()
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if current_source and current_source['auto_refresh']:
                st.success("🔄 Auto-refresh: ON")
                st.caption("Dashboard updates every 10 seconds")
            else:
                st.info("🔄 Auto-refresh: OFF")
                st.caption("Manual refresh only")
        
        with col2:
            st.info(f"📊 Displaying {len(filtered_df)} records")
            st.caption(f"From {len(df)} total records")
        
        with col3:
            if st.button("🔙 Back to Sources"):
                if 'selected_source' in st.session_state:
                    del st.session_state.selected_source
                st.rerun()
        
    except Exception as e:
        st.error(f"Error displaying dashboard: {e}")
        logger.error(f"Dashboard display error: {e}")
        
        # Show back button on error
        if st.button("🔙 Back to Sources"):
            if 'selected_source' in st.session_state:
                del st.session_state.selected_source
            st.rerun()

def filter_by_time_range(df: pd.DataFrame, time_col: str, time_range: str) -> pd.DataFrame:
    """Filter dataframe by time range"""
    if time_range == "All" or df.empty:
        return df
    
    try:
        max_date = df[time_col].max()
        if time_range == "Last 7 Days":
            cutoff = max_date - timedelta(days=7)
        elif time_range == "Last 30 Days":
            cutoff = max_date - timedelta(days=30)
        elif time_range == "Last 90 Days":
            cutoff = max_date - timedelta(days=90)
        else:
            return df
        
        return df[df[time_col] >= cutoff].copy()
    except Exception as e:
        logger.error(f"Error filtering by time range: {e}")
        return df

def display_welcome_screen():
    """Display welcome screen when no source is selected"""
    st.markdown("""
    ## 👋 Welcome to Real-Time Analytics Dashboard
    
    ### 🚀 Key Features
    - **📂 Multiple Data Sources**: Upload CSV/Excel files or connect Google Sheets
    - **🔄 Real-Time Updates**: Automatic refresh every 10 seconds for Google Sheets
    - **📊 Interactive Visualizations**: Line charts, bar charts, and dual-axis plots
    - **📈 Advanced Analytics**: Correlation analysis and trend lines
    - **⏰ Time Range Filtering**: Focus on specific time periods
    - **🔍 Data Explorer**: Search and filter your raw data
    - **📥 Export Options**: Download data as CSV or Excel
    
    ### 🏗️ How It Works
    1. **Connect Data**: Choose from CSV, Excel, or Google Sheets
    2. **Auto-Detection**: System identifies time and metric columns automatically
    3. **Live Dashboard**: Watch your data update in real-time (Google Sheets)
    4. **Analyze**: Select metrics, apply filters, and explore correlations
    5. **Export**: Download filtered data for further analysis
    
    ### 📋 Data Requirements
    - **Time Column**: Date/time column (automatically detected)
    - **Metric Columns**: Numeric columns for analysis
    - **Supported Formats**: 
      - CSV files (.csv)
      - Excel files (.xlsx, .xls)
      - Google Sheets (with service account)
    
    ### 🔧 Google Sheets Setup
    For Google Sheets integration:
    1. Create a service account in Google Cloud Console
    2. Enable Google Sheets API
    3. Download credentials JSON file
    4. Share your sheet with the service account email
    
    **👈 Get started by choosing a data source from the sidebar!**
    
    ---
    
    ### 📊 Sample Data Structure
    Your data should look like this:
    
    | Date       | Revenue | Clicks | Conversions | Spend |
    |------------|---------|--------|-------------|-------|
    | 2024-01-01 | 1500.00 | 245    | 12          | 450   |
    | 2024-01-02 | 1750.25 | 289    | 15          | 520   |
    | 2024-01-03 | 1250.50 | 198    | 8           | 380   |
    
    The system will automatically:
    - Detect "Date" as the time column
    - Identify "Revenue", "Clicks", "Conversions", "Spend" as metrics
    - Sort data chronologically
    - Enable real-time updates (for Google Sheets)
    """)
    
    # Show some example charts
    st.subheader("📈 Dashboard Preview")
    
    # Create sample data for demonstration
    dates = pd.date_range('2024-01-01', periods=30, freq='D')
    np.random.seed(42)
    
    sample_data = {
        'Date': dates,
        'Revenue': 1000 + np.cumsum(np.random.randn(30) * 50),
        'Clicks': 200 + np.cumsum(np.random.randn(30) * 10),
        'Conversions': 10 + np.cumsum(np.random.randn(30) * 2)
    }
    sample_df = pd.DataFrame(sample_data)
    
    # Create sample charts
    col1, col2 = st.columns(2)
    
    with col1:
        fig1 = px.line(sample_df, x='Date', y=['Revenue', 'Clicks'], 
                      title="Sample: Multiple Metrics Over Time")
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        fig2 = px.bar(sample_df.tail(7), x='Date', y='Conversions',
                     title="Sample: Recent Conversions")
        st.plotly_chart(fig2, use_container_width=True)

if __name__ == "__main__":
    main()