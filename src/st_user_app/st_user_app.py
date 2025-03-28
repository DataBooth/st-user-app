# st_user_app.py
import streamlit as st
from loguru import logger
from .propelauth_app import auth
from .storage_factory import StorageFactory
from pathlib import Path
import os


class StreamlitUserApp:
    def __init__(self, storage_type: str = "duckdb"):
        self.user = None
        self.storage = None
        self.storage_type = storage_type
        self._configure_logging()
        self._initialize_components()

    def _configure_logging(self):
        # TODO: Fix log path use project root
        """Configure Loguru logging with rotation"""
        log_path = Path("logs/app.log")
        log_path.parent.mkdir(parents=True, exist_ok=True)

        logger.add(
            str(log_path),
            rotation="1 MB",
            retention="1 week",
            compression="zip",
            enqueue=True,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        )

    def _initialize_components(self):
        """Initialize authentication and storage"""
        try:
            self.user = auth.get_user()
            self.storage = StorageFactory.create(
                self.storage_type, location=os.getenv("STORAGE_LOCATION", "local")
            )
            logger.info(f"Initialized {self.storage_type} storage")
        except Exception as e:
            logger.error(f"Initialization failed: {str(e)}")
            st.error("Application initialization failed. Please contact support.")
            st.stop()

    def run(self):
        """Main application entry point"""
        if not self.user:
            self._show_unauthorized()
        else:
            self._show_authenticated_ui()

    def _show_unauthorized(self):
        """Display unauthorized message"""
        st.error("🚫 Unauthorized - Please log in to access the application")
        st.stop()

    def _show_authenticated_ui(self):
        """Display authenticated user interface"""
        self._setup_sidebar()
        self._setup_main_content()

    def _setup_sidebar(self):
        """Configure the sidebar components"""
        with st.sidebar:
            st.link_button(
                "🔑 Account Settings", auth.get_account_url(), use_container_width=True
            )
            st.write(f"👤 Logged in as: **{self.user.email}**")

            if st.button("🚪 Logout", use_container_width=True):
                self._handle_logout()

    def _handle_logout(self):
        """Handle logout process"""
        try:
            auth.logout()
            self.storage.close()
            logger.info(f"User {self.user.email} logged out")
            st.markdown(
                """
                <meta http-equiv="refresh" content="0; URL='/api/auth/logout'" />
                """,
                unsafe_allow_html=True,
            )
        except Exception as e:
            logger.error(f"Logout failed: {str(e)}")
            st.error("Logout failed. Please try again.")

    def _setup_main_content(self):
        """Configure main content area"""
        st.title("🔐 Secure User Application")
        st.write(f"Welcome back, **{self.user.email.split('@')[0]}**!")

        tab1, tab2 = st.tabs(["💾 Store Data", "📊 View Data"])

        with tab1:
            self._data_input_section()

        with tab2:
            self._data_display_section()

    def _data_input_section(self):
        """Data input and storage section"""
        st.header("Store Your Data")
        user_input = st.text_area(
            "Enter your data:", placeholder="Type here...", height=100
        )

        if st.button("💾 Save Data", type="primary"):
            self._save_user_data(user_input)

    def _save_user_data(self, data: str):
        """Handle data storage"""
        try:
            if not data.strip():
                st.warning("Please enter some data to save")
                return

            self.storage.save_data(self.user.user_id, data)
            logger.info(f"Data saved for {self.user.email}")
            st.toast("✅ Data saved successfully!", icon="✔️")
        except Exception as e:
            logger.error(f"Data save failed: {str(e)}")
            st.error("Failed to save data. Please try again.")

    def _data_display_section(self):
        """Data retrieval and display section"""
        st.header("Your Stored Data")
        try:
            user_data = self.storage.get_user_data(self.user.user_id)
            if user_data:
                self._display_data_table(user_data)
            else:
                st.info("📭 No data found - Start by saving some data!")
        except Exception as e:
            logger.error(f"Data retrieval failed: {str(e)}")
            st.error("Failed to load data. Please try again later.")

    def _display_data_table(self, data: list):
        """Display data in a formatted table"""
        import pandas as pd

        df = pd.DataFrame(data, columns=["Data", "Timestamp"])
        df["Timestamp"] = pd.to_datetime(df["Timestamp"]).dt.strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Data": "Your Data",
                "Timestamp": st.column_config.DatetimeColumn(
                    "Timestamp", format="YYYY-MM-DD HH:mm:ss"
                ),
            },
        )

    def __del__(self):
        """Cleanup resources on exit"""
        if self.storage:
            try:
                self.storage.close()
                logger.info("Storage connection closed")
            except Exception as e:
                logger.error(f"Storage closure failed: {str(e)}")
