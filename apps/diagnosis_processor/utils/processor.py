"""
Data processing utilities for diagnosis processor
"""
import re
import uuid
from datetime import datetime
from typing import Dict, List, Tuple
import pandas as pd
from .drools_parser import parse_drools_syntax

def clean_sheet_name(name: str) -> str:
    """Convert string to valid Excel sheet name"""
    # Excel sheet name rules: max 31 chars, no special chars
    safe_name = re.sub(r'[\\/*\[\]:?]', '_', str(name))
    return (safe_name or 'Sheet')[:31]

def process_diagnosis_data(df: pd.DataFrame) -> Tuple[Dict[str, pd.DataFrame], List[str]]:
    """
    Process diagnosis data and return sheet data and diagnosis list
    Returns (sheets_dict, diagnosis_list)
    """
    # Extract unique diagnosis list with commas
    diagnosis_list = []
    for diagnosis in df['Standard Diagnosis'].unique():
        if pd.notna(diagnosis):
            # Add both versions (with and without comma)
            diagnosis_list.append(f'"{diagnosis}"')
            diagnosis_list.append(f'"{diagnosis},"')
    
    # Process drools syntax
    df['claim_item'] = None
    df['max_quantity'] = None
    
    for idx, row in df.iterrows():
        claim_item, max_quantity = parse_drools_syntax(row['Drools Syntax'])
        df.at[idx, 'claim_item'] = claim_item
        df.at[idx, 'max_quantity'] = max_quantity
    
    # Group by Claim Representation
    sheets = {}
    seen_names = set()
    
    for claim_rep, group_df in df.groupby('Claim Representation'):
        sheet_name = clean_sheet_name(claim_rep)
        
        # Handle duplicate sheet names
        base_name = sheet_name
        counter = 1
        while sheet_name in seen_names:
            sheet_name = f"{base_name[:27]}_{counter}"
            counter += 1
        
        seen_names.add(sheet_name)
        sheets[sheet_name] = group_df.copy()
    
    return sheets, diagnosis_list

def generate_filenames() -> Tuple[str, str]:
    """Generate unique filenames for outputs"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    unique_id = str(uuid.uuid4())[:8]
    
    txt_filename = f'diagnosis_list_{timestamp}_{unique_id}.txt'
    excel_filename = f'processed_output_{timestamp}_{unique_id}.xlsx'
    
    return txt_filename, excel_filename