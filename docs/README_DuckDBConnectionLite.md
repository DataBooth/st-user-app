# DuckDBConnectionLite Documentation

A lightweight Streamlit connection class for DuckDB databases.

## Features

- Simple connection management
- Supports in-memory, file-based and MotherDuck connections
- Minimal dependencies

## Usage Example

```{python}
# Basic in-memory connection
conn = st.connection("", type=DuckDBConnectionLite)
result = conn.query("SELECT 'Hello, World!' AS greeting;")
conn.close()
```

## Connection Targets

- `None`: In-memory database
- `*.duckdb`: Local DuckDB file
- `md:database_name`: MotherDuck connection (requires MOTHERDUCK_TOKEN)


This implementation provides:

- Clear separation between documentation and examples
- Reusable components for different connection types
- Built-in result persistence between runs
- Error handling with user feedback
- Clean resource management
