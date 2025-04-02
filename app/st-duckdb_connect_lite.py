import streamlit as st
from pathlib import Path
from loguru import logger
from st_user_app.storage.duckdb_connection_lite import DuckDBConnectionLite
import os
import pandas as pd
from dotenv import load_dotenv

# Configuration
CONNECTION_EXAMPLES = [
    {
        "name": "Memory Example",
        "description": "Simple in-memory database query",
        "connection_target": ":memory:",
        "query": "CREATE TABLE camino_stjames AS FROM '/Users/mjboothaus/data/local/umami/umami_camino-stjames_2Apr2025PM.csv';"
    },
    {
        "name": "File Example",
        "description": "Query from local DuckDB file",
        "connection_target": "services.duckdb",
        "query": "SELECT * FROM my_table LIMIT 10;"
    },
    {
        "name": "MotherDuck Example",
        "description": "Query from MotherDuck database",
        "connection_target": "md:dementia_data",
        "query": "SHOW DATABASES;"
    },
    {
        "name": "URL Example",
        "description": "Create a table dynamically from a URL",
        "connection_target": "https://blobs.duckdb.org/nl-railway/services-2024.csv.gz",
        "query": "SELECT * FROM auto_table LIMIT 10;"
    }
]

@st.cache_resource
def load_readme():
    try:
        return Path("docs/README_DuckDBConnectionLite.md").read_text()
    except Exception as e:
        logger.error(f"Error reading README: {e}")
        return "## Documentation\nREADME file not found."

def create_conn(example, idx):
    """Create a connection for a given example."""
    try:
        if example["connection_target"] and isinstance(example["connection_target"], str) and example["connection_target"].startswith("md:"):
            load_dotenv()
            md_token = os.getenv("MOTHERDUCK_TOKEN")
        
        # Pass a unique name for each connection
        conn = st.connection(
            name=f"example_{idx}",  # Unique (dummy) name for each example
            type=DuckDBConnectionLite,
            connection_target=example["connection_target"],
            md_token=md_token if example["connection_target"].startswith("md:") else None,
        )
        
        logger.info(f"Connection created for {example['name']} with target: {example['connection_target']}")
        
        return conn
    except Exception as e:
        logger.error(f"Failed to create connection for {example['name']}: {e}")
        raise

def main():
    st.set_page_config(page_title="`DuckDBConnectionLite` Demos", layout="wide")
    
    # Create tabs
    tab_main, *example_tabs = st.tabs(["README"] + [ex["name"] for ex in CONNECTION_EXAMPLES])
    
    with tab_main:
        st.markdown(load_readme())
    
    # Example tabs
    for idx, (example, tab) in enumerate(zip(CONNECTION_EXAMPLES, example_tabs)):
        with tab:
            st.subheader(example["name"])
            st.caption(example["description"])
            
            col1, col2 = st.columns([3, 1])
            with col1:
                query = st.text_area(
                    f"SQL Query ({example['name']})",
                    value=example["query"],
                    height=150,
                    key=f"query_{idx}"
                )
            
            with col2:
                if st.button("🚀 Run Query", key=f"run_{idx}"):
                    try:
                        # Create connection
                        conn = create_conn(example, idx)
                        
                        # Execute query and store result in session state
                        result = conn.query(query)
                        st.session_state[f"result_{idx}"] = result
                        
                        # Log successful query execution
                        logger.info(f"Query executed successfully for {example['name']}")
                        
                        # Close connection explicitly
                        conn.close()
                        logger.info(f"Connection closed for {example['name']}")
                    except Exception as e:
                        logger.error(f"Query error for {example['name']}: {e}")
                        st.error(f"Query failed: {e}")
            
            # Display results if available
            if f"result_{idx}" in st.session_state:
                st.subheader("Results")
                df = st.session_state[f"result_{idx}"]
                
                # Display dataframe
                st.dataframe(df, use_container_width=True)
                
                # Download button
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download CSV",
                    data=csv,
                    file_name=f"results_{example['name'].lower().replace(' ', '_')}.csv",
                    mime='text/csv',
                    key=f"download_{idx}"
                )

if __name__ == "__main__":
    main()
