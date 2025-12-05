import pandas as pd
import json


def debug_print(data):
    """Pretty-print any data (including DataFrames) in full JSON format with clear separators."""
    separator_top = ">>>" * 40
    separator_bottom = "<<<" * 40
    print(separator_top)

    # Convert DataFrame to JSON
    if isinstance(data, pd.DataFrame):
        json_data = data.to_dict(orient="records")
        print(json.dumps(json_data, indent=2, default=str))
    # Convert lists or dicts directly
    elif isinstance(data, (dict, list)):
        print(json.dumps(data, indent=2, default=str))
    else:
        # Fallback to string representation
        print(str(data))

    print(separator_bottom)
