"""Helper functions for Lakeflow DQ."""

def table_fqn(catalog: str, schema: str, table_name: str) -> str:
    """
    Return a fully-qualified table name.
    """
    return f"{catalog}.{schema}.{table_name}"
