#!/usr/bin/env python3
"""
Excel Splitter Application
Tool for splitting Excel files based on categories.
"""

import re
from io import BytesIO
import pandas as pd
from flask import Blueprint, render_template, request, send_file
from werkzeug.utils import secure_filename

def create_splitter_app():
    """Create the splitter application blueprint"""
    # Note: Using the main app's template folder
    splitter_bp = Blueprint('splitter', __name__)

    @splitter_bp.route('/', methods=['GET', 'POST'])
    def split_excel():
        if request.method == 'POST':
            try:
                file = request.files.get('file')
                if not file or file.filename.strip() == '':
                    return render_template('split_excel.html', error="Please choose an Excel file (.xlsx).")

                # Read file directly from memory without saving
                file_content = BytesIO(file.read())
                
                # Read input directly from memory
                df = pd.read_excel(file_content, engine='openpyxl')

                category_column = 'tariff_type'
                if category_column not in df.columns:
                    return render_template('split_excel.html', 
                                        error=f"Column '{category_column}' not found in the uploaded file.")

                sensitive_col = ["target code", "SNOMED CODE", "target_code", "snomed code"]
                for col in sensitive_col:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64").astype(str)
                
                unique_categories = pd.Series(df[category_column].unique()).fillna('Unspecified')

                timestamp = pd.Timestamp.now().strftime('%Y%m%d')
                output_filename = f'classified_{timestamp}_split.xlsx'

                def clean_sheet_name(name: str) -> str:
                    # Excel sheet name rules: max 31 chars, cannot contain : \ / ? * [ ]
                    safe = re.sub(r'[:\\/?*\[\]]', '-', str(name))
                    return (safe or 'Sheet')[:31]

                # Create output file in memory
                output_buffer = BytesIO()
                
                with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
                    # Write ONLY category sheets, drop the category column in each, no index
                    for raw_category in unique_categories:
                        category = 'Unspecified' if pd.isna(raw_category) else raw_category
                        subset_df = df[df[category_column].fillna('Unspecified') == category].copy()
                        # Drop the category column in outputs
                        if category_column in subset_df.columns:
                            subset_df.drop(columns=[category_column], inplace=True, errors='ignore')
                        subset_df.to_excel(writer, sheet_name=clean_sheet_name(str(category)), 
                                        index=False)

                # Reset buffer position to beginning
                output_buffer.seek(0)

                # Return file directly to user
                return send_file(
                    output_buffer,
                    as_attachment=True,
                    download_name=output_filename,
                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )

            except Exception as e:
                return render_template('split_excel.html', error=str(e))

        return render_template('split_excel.html')

    return splitter_bp