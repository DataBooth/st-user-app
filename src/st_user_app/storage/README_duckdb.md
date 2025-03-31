# DuckDB Connection Manager

## Purpose

- Unified interface for DuckDB connections (local/memory/MotherDuck)
- Automatic schema creation from remote sources
- Metrics-driven performance monitoring

## Configuration

```{toml}
[connections]
data_uri = "scheme://path"  # Required
source_url = "https://..."  # Optional
create_table = true|false|"name"
motherduck_token = "..."    # For MotherDuck
```

**URI Schemes**:

- `file://`: Persistent database
- `memory://`: Ephemeral storage
- `md://`: MotherDuck connection

## Performance Tips

1. First-run penalty: File-based connections with source URLs will be slower
2. Monitor `metrics.connection_time_ms` for bottlenecks
3. For large datasets:
   - Pre-warm connections during app initialization
   - Use file-based over in-memory for datasets > 100MB

### Key Decisions

1. **URI Scheme Validation**: Ensures only supported connection types
2. **Pydantic Integration**: Catches configuration errors during initialisation
3. **Metrics Collection**: Enables data-driven optimisations
4. **Conditional Table Creation**: Avoids redundant schema operations