from pandas import DataFrame
import json


def debug_log(data: DataFrame, indent: int = 2, max_rows: int | None = None):
    """Pretty print DataFrame as JSON objects row by row."""
    print(">>>" * 40)
    rows = data.to_dict(orient="records")
    if max_rows:
        rows = rows[:max_rows]
    for i, row in enumerate(rows):
        print(f"Row {i}:")
        print(json.dumps(row, indent=indent, default=str))
        print("-" * 60)
    print(">>>" * 40)
