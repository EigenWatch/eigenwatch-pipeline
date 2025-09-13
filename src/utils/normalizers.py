import pandas as pd
from web3 import Web3


def normalize_bytes_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert all bytes-like columns in a DataFrame to hex strings using Web3.toHex."""
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: (
                Web3.to_hex(x) if isinstance(x, (bytes, bytearray, memoryview)) else x
            )
        )
    return df
