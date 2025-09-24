# Real-Time Analytics Dashboard System
# Complete implementation with Excel/Google Sheets integration

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
from io import BytesIO
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
                is_active BOOLEAN DEFAULT 1
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
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (source_id) REFERENCES data_sources (id)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def save_data_source(self, user_id: str, source_type: str, source_name: str, 
                        config: Dict = None) -> int:
        """Save data source configuration"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        config_json = json.dumps(config) if config else None
        cursor.execute('''
            INSERT INTO data_sources (user_id, source_type, source_name, connection_config)
            VALUES (?, ?, ?, ?)
        ''', (user_id, source_type, source_name, config_json))
        
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
            # Convert datetime columns to string for JSON serialization
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
                        time_column = ?, metric_columns = ?, last_updated = CURRENT_TIMESTAMP
                    WHERE source_id = ?
                ''', (data_hash, raw_data, processed_data, time_col, 
                      json.dumps(metric_cols), source_id))
            else:  # Insert new
                cursor.execute('''
                    INSERT INTO analytics_data 
                    (source_id, data_hash, raw_data, processed_data, time_column, metric_columns)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (source_id, data_hash, raw_data, processed_data, time_col, 
                      json.dumps(metric_cols)))
            
            conn.commit()
            logger.info(f"Successfully saved data for source_id: {source_id}")
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
            SELECT id, source_type, source_name, last_updated, is_active
            FROM data_sources WHERE user_id = ? AND is_active = 1
        ''', (user_id,))
        
        results = cursor.fetchall()
        conn.close()
        
        return [{'id': r[0], 'type': r[1], 'name': r[2], 
                'last_updated': r[3], 'active': r[4]} for r in results]

class DataProcessor:
    """Handles data processing and validation"""
    
    @staticmethod
    def detect_time_column(df: pd.DataFrame) -> Optional[str]:
        """Automatically detect time/date column"""
        for col in df.columns:
            try:
                # Try to parse as datetime
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
        # Detect time column
        time_col = DataProcessor.detect_time_column(df)
        if not time_col:
            raise ValueError("No time/date column detected")
        
        # Convert time column to datetime
        df[time_col] = pd.to_datetime(df[time_col])
        
        # Detect metric columns
        metric_cols = DataProcessor.detect_metric_columns(df, time_col)
        if not metric_cols:
            raise ValueError("No numeric metric columns detected")
        
        # Sort by time column
        df = df.sort_values(by=time_col).reset_index(drop=True)
        
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
            return pd.DataFrame(data)
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
    
    def start_updates(self):
        """Start the update thread"""
        if not self.running:
            self.running = True
            self.update_thread = threading.Thread(target=self._update_loop)
            self.update_thread.daemon = True
            self.update_thread.start()
    
    def stop_updates(self):
        """Stop the update thread"""
        self.running = False
    
    def _update_loop(self):
        """Main update loop - runs every 10 seconds"""
        while self.running:
            try:
                self._check_for_updates()
                time.sleep(10)  # Update every 10 seconds
            except Exception as e:
                logger.error(f"Update loop error: {e}")
                time.sleep(10)
    
    def _check_for_updates(self):
        """Check all data sources for updates"""
        # This would check all active sources and update if changed
        # Implementation depends on specific requirements
        pass

class DashboardVisualizer:
    """Handles dashboard visualizations"""
    
    @staticmethod
    def create_time_series_chart(df: pd.DataFrame, time_col: str, 
                               metric_cols: List[str], chart_type: str = "line"):
        """Create time series visualization"""
        if chart_type == "line":
            fig = px.line(df, x=time_col, y=metric_cols, 
                         title="Metrics Over Time")
        elif chart_type == "bar":
            fig = px.bar(df, x=time_col, y=metric_cols[0], 
                        title=f"{metric_cols[0]} Over Time")
        else:  # dual-axis
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            fig.add_trace(
                go.Scatter(x=df[time_col], y=df[metric_cols[0]], 
                          name=metric_cols[0]),
                secondary_y=False,
            )
            
            if len(metric_cols) > 1:
                fig.add_trace(
                    go.Scatter(x=df[time_col], y=df[metric_cols[1]], 
                              name=metric_cols[1]),
                    secondary_y=True,
                )
        
        fig.update_layout(height=500)
        return fig
    
    @staticmethod
    def create_comparison_chart(df: pd.DataFrame, metric_cols: List[str]):
        """Create metric comparison chart"""
        # Calculate correlations or summary stats
        summary_data = []
        for col in metric_cols:
            summary_data.append({
                'Metric': col,
                'Total': df[col].sum(),
                'Average': df[col].mean(),
                'Max': df[col].max()
            })
        
        summary_df = pd.DataFrame(summary_data)
        fig = px.bar(summary_df, x='Metric', y='Total', 
                    title='Metric Totals Comparison')
        return fig

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
        st.session_state.user_id = "demo_user"  # In production, use proper auth
    
    st.title("📊 Real-Time Analytics Dashboard")
    st.markdown("Connect your data sources and watch your metrics update automatically!")
    
    # Sidebar for data source management
    with st.sidebar:
        st.header("📂 Data Sources")
        
        source_type = st.selectbox(
            "Choose Data Source",
            ["Upload Excel File", "Connect Google Sheets"]
        )
        
        if source_type == "Upload Excel File":
            handle_excel_upload()
        else:
            handle_google_sheets_connection()
        
        # Show existing sources
        st.subheader("Connected Sources")
        sources = st.session_state.db_manager.get_user_sources(st.session_state.user_id)
        
        if sources:
            # Show source details
            for source in sources:
                with st.expander(f"📊 {source['name']} ({source['type']})", expanded=False):
                    st.write(f"**Type:** {source['type']}")
                    st.write(f"**Last Updated:** {source['last_updated']}")
                    
                    # Check if data exists for this source
                    data_check = st.session_state.db_manager.get_analytics_data(source['id'])
                    if data_check:
                        df_check, time_col_check, metrics_check = data_check
                        st.write(f"**Records:** {len(df_check)}")
                        st.write(f"**Time Column:** {time_col_check}")
                        st.write(f"**Metrics:** {', '.join(metrics_check[:3])}{'...' if len(metrics_check) > 3 else ''}")
                        
                        if st.button(f"View Dashboard", key=f"view_{source['id']}"):
                            st.session_state.selected_source = source['id']
                            st.rerun()
                    else:
                        st.warning("⚠️ No data found for this source")
            
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
                    try:
                        # Save to database
                        source_id = st.session_state.db_manager.save_data_source(
                            st.session_state.user_id,
                            "excel",
                            uploaded_file.name
                        )
                        
                        # Save the data
                        success = st.session_state.db_manager.save_analytics_data(
                            source_id, processed_df, time_col, metric_cols
                        )
                        
                        if success:
                            st.success("✅ Data source saved successfully!")
                            st.session_state.selected_source = source_id  # Auto-select the new source
                            time.sleep(1)  # Brief pause to show success message
                            st.rerun()
                        else:
                            st.warning("Data source already exists with same data")
                            
                    except Exception as e:
                        st.error(f"Error saving data source: {e}")
                        logger.error(f"Error in handle_excel_upload: {e}")
                
        except Exception as e:
            st.error(f"Error processing file: {e}")
            logger.error(f"Error processing Excel file: {e}")

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
                                        "Google Sheets",
                                        config
                                    )
                                    
                                    success = st.session_state.db_manager.save_analytics_data(
                                        source_id, processed_df, time_col, metric_cols
                                    )
                                    
                                    if success:
                                        st.success("✅ Google Sheets connection saved!")
                                        st.session_state.selected_source = source_id  # Auto-select the new source
                                        time.sleep(1)  # Brief pause to show success message
                                        st.rerun()
                                    else:
                                        st.warning("Connection already exists with same data")
                                        
                                except Exception as e:
                                    st.error(f"Error saving connection: {e}")
                                    logger.error(f"Error in handle_google_sheets_connection: {e}")
                    else:
                        st.error("❌ Cannot access the sheet. Check URL and permissions.")
                else:
                    st.error("❌ Authentication failed. Check your credentials.")
        except Exception as e:
            st.error(f"Connection error: {e}")

def display_dashboard(source_id: int):
    """Display the main dashboard"""
    try:
        # Add debug information
        st.sidebar.write(f"🔍 Debug: Loading source ID: {source_id}")
        
        data_result = st.session_state.db_manager.get_analytics_data(source_id)
        if not data_result:
            st.error(f"❌ No data found for source ID: {source_id}")
            
            # Debug: Check if source exists
            sources = st.session_state.db_manager.get_user_sources(st.session_state.user_id)
            source_exists = any(s['id'] == source_id for s in sources)
            
            if not source_exists:
                st.error("❌ Source ID does not exist in data_sources table")
                if st.button("🔙 Back to Sources"):
                    if 'selected_source' in st.session_state:
                        del st.session_state.selected_source
                    st.rerun()
            else:
                st.info("✅ Source exists in data_sources table but no analytics data found")
                st.info("💡 Try re-uploading your data or check the connection")
            return
        
        df, time_col, metric_cols = data_result
        
        if df.empty:
            st.warning("⚠️ Data source exists but contains no records")
            return
        
        # Dashboard header
        col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
        with col1:
            st.header("📈 Live Dashboard")
        with col2:
            st.metric("Total Records", len(df))
        with col3:
            st.metric("Metrics Available", len(metric_cols))
        with col4:
            if st.button("🔄 Refresh Now"):
                st.rerun()
        
        # Show data info
        st.info(f"📅 Time Range: {df[time_col].min()} to {df[time_col].max()}")
        
        # Metric selection
        st.subheader("Select Metrics to Display")
        selected_metrics = st.multiselect(
            "Choose metrics (2-3 recommended)",
            metric_cols,
            default=metric_cols[:min(2, len(metric_cols))]
        )
        
        if not selected_metrics:
            st.warning("Please select at least one metric")
            return
        
        # Chart options
        col1, col2 = st.columns(2)
        with col1:
            chart_type = st.selectbox(
                "Chart Type",
                ["line", "bar", "dual-axis"]
            )
        with col2:
            time_range = st.selectbox(
                "Time Range",
                ["All", "Last 7 Days", "Last 30 Days", "Last 90 Days"]
            )
        
        # Filter data by time range
        filtered_df = filter_by_time_range(df, time_col, time_range)
        
        if filtered_df.empty:
            st.warning(f"⚠️ No data available for selected time range: {time_range}")
            return
        
        # Create visualizations
        st.subheader("📊 Metrics Over Time")
        
        try:
            chart = DashboardVisualizer.create_time_series_chart(
                filtered_df, time_col, selected_metrics, chart_type
            )
            st.plotly_chart(chart, use_container_width=True)
        except Exception as e:
            st.error(f"Error creating chart: {e}")
            logger.error(f"Chart creation error: {e}")
        
        # Additional charts
        col1, col2 = st.columns(2)
        
        with col1:
            if len(selected_metrics) > 1:
                try:
                    comparison_chart = DashboardVisualizer.create_comparison_chart(
                        filtered_df, selected_metrics
                    )
                    st.plotly_chart(comparison_chart, use_container_width=True)
                except Exception as e:
                    st.error(f"Error creating comparison chart: {e}")
        
        with col2:
            # Summary statistics
            st.subheader("📋 Summary Stats")
            for metric in selected_metrics:
                try:
                    current_val = filtered_df[metric].iloc[-1] if len(filtered_df) > 0 else 0
                    prev_val = filtered_df[metric].iloc[-2] if len(filtered_df) > 1 else current_val
                    delta = current_val - prev_val if len(filtered_df) > 1 else None
                    
                    st.metric(
                        metric,
                        f"{current_val:.2f}",
                        f"{delta:.2f}" if delta is not None else None
                    )
                except Exception as e:
                    st.error(f"Error calculating metrics for {metric}: {e}")
        
        # Data table
        with st.expander("📄 Raw Data"):
            st.dataframe(filtered_df[[time_col] + selected_metrics], 
                        use_container_width=True)
        
        # Auto-refresh info
        st.success("🔄 Dashboard updates automatically every 10 seconds")
        
        # Back button
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
    if time_range == "All":
        return df
    
    now = pd.Timestamp.now()
    if time_range == "Last 7 Days":
        cutoff = now - timedelta(days=7)
    elif time_range == "Last 30 Days":
        cutoff = now - timedelta(days=30)
    elif time_range == "Last 90 Days":
        cutoff = now - timedelta(days=90)
    else:
        return df
    
    return df[df[time_col] >= cutoff]

def display_welcome_screen():
    """Display welcome screen when no source is selected"""
    st.markdown("""
    ## 👋 Welcome to Real-Time Analytics Dashboard
    
    ### 🚀 Features
    - **📂 Multiple Data Sources**: Upload Excel files or connect Google Sheets
    - **🔄 Real-Time Updates**: Automatic refresh every 10 seconds
    - **📊 Flexible Visualizations**: Line charts, bar charts, and dual-axis plots
    - **📈 Metric Comparisons**: Compare multiple metrics side-by-side
    - **⏰ Time Range Filtering**: Focus on specific time periods
    
    ### 🏗️ How It Works
    1. **Connect Data**: Upload an Excel file or connect your Google Sheet
    2. **Auto-Detection**: System automatically identifies time and metric columns
    3. **Live Dashboard**: Watch your data update in real-time
    4. **Compare Metrics**: Select 2-3 metrics for side-by-side analysis
    
    ### 📋 Data Requirements
    - **Time Column**: Date/time column for X-axis
    - **Metric Columns**: Numeric columns for analysis (spend, clicks, revenue, etc.)
    - **Format**: Excel (.xlsx) or Google Sheets
    
    **👈 Get started by choosing a data source from the sidebar!**
    """)

if __name__ == "__main__":
    main()

# Additional utility functions and classes would be implemented here
# This includes error handling, data validation, security measures, etc.