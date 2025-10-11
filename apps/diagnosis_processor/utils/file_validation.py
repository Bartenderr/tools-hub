"""
File validation utilities for diagnosis processor app
"""
from typing import Tuple, Optional
import pandas as pd

REQUIRED_COLUMNS = {
    'Diagnosis',
    'Standard Diagnosis',
    'Claim Representation',
    'Drools Syntax'
}

def validate_file_content(df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
    """
    Validate uploaded file content
    Returns (is_valid, error_message)
    """
    # Check for required columns
    missing_cols = REQUIRED_COLUMNS - set(df.columns)
    if missing_cols:
        return False, f"Missing required columns: {', '.join(missing_cols)}"
    
    # Check for empty required values
    for col in ['Claim Representation', 'Drools Syntax']:
        if df[col].isna().any():
            empty_rows = df[df[col].isna()].index.tolist()
            return False, f"Empty values found in {col} column at rows: {empty_rows}"
    
    return True, None