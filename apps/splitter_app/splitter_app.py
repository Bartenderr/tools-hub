#!/usr/bin/env python3
"""
Excel Splitter Application
Tool for splitting Excel files based on categories.
"""

import re
import base64
from io import BytesIO
import pandas as pd
from flask import Blueprint, render_template, request, jsonify

import re
import base64
import logging
from io import BytesIO
import pandas as pd
from flask import Blueprint, render_template, request, jsonify

# Configure logger for this module
logger = logging.getLogger(__name__)

def create_splitter_app():
    """Create the splitter application blueprint"""
    splitter_bp = Blueprint('splitter', __name__)

    @splitter_bp.route('/', methods=['GET', 'POST'])
    def split_excel():
        if request.method == 'POST':
            try:
                logger.info("Starting Excel split process")
                
                # 1. File Presence Check
                if 'file' not in request.files:
                    logger.warning("No file part in request")
                    return jsonify({"error": "No file part in the request."}), 400
                
                file = request.files.get('file')
                if not file or file.filename.strip() == '':
                    logger.warning("No file selected for upload")
                    return jsonify({"error": "Please choose an Excel file (.xlsx)."}), 400

                # 2. Extension Check
                if not file.filename.lower().endswith(('.xlsx', '.xls', '.xlsm')):
                    logger.warning(f"Invalid file extension: {file.filename}")
                    return jsonify({"error": "Invalid file format. Please upload an Excel file (.xlsx, .xls, .xlsm)."}), 400

                # Read file directly from memory
                try:
                    file_content = BytesIO(file.read())
                    df = pd.read_excel(file_content, engine='openpyxl')
                except Exception as e:
                    logger.error(f"Failed to read Excel file: {str(e)}")
                    return jsonify({"error": f"Failed to read Excel file: {str(e)}"}), 400

                # 3. Data Integrity Check
                if df.empty:
                    logger.warning("Uploaded Excel file is empty")
                    return jsonify({"error": "The uploaded Excel file is empty."}), 400

                # 4. Column Check
                category_column = 'tariff_type'
                if category_column not in df.columns:
                    available_cols = ", ".join(df.columns.tolist()[:5])
                    logger.warning(f"Column '{category_column}' not found. Available: {available_cols}")
                    return jsonify({"error": f"Required column '{category_column}' not found. Please ensure your file has this column."}), 400

                # Clean sensitive columns (formatting)
                sensitive_col = ["target code", "SNOMED CODE", "target_code", "snomed code"]
                for col in sensitive_col:
                    if col in df.columns:
                        try:
                            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64").astype(str).replace('<NA>', '')
                        except Exception as e:
                            logger.debug(f"Formatting column {col} failed: {e}")
                
                # 5. Category Validation
                unique_categories = df[category_column].unique()
                if len(unique_categories) == 0 or (len(unique_categories) == 1 and pd.isna(unique_categories[0])):
                    logger.warning("No valid categories found in column")
                    return jsonify({"error": "No valid data found in the 'tariff_type' column to split by."}), 400

                timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
                output_filename = f'split_report_{timestamp}.xlsx'

                def clean_sheet_name(name: str) -> str:
                    # Excel sheet name rules: max 31 chars, cannot contain : \ / ? * [ ]
                    if pd.isna(name) or str(name).strip() == '':
                        return 'Unspecified'
                    safe = re.sub(r'[:\\/?*\[\]]', '-', str(name))
                    return (safe or 'Sheet')[:31]

                # 6. Splitting Logic
                output_buffer = BytesIO()
                try:
                    with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
                        for raw_category in unique_categories:
                            category_name = 'Unspecified' if pd.isna(raw_category) else str(raw_category)
                            subset_df = df[df[category_column].fillna('Unspecified') == raw_category].copy()
                            
                            if subset_df.empty:
                                continue

                            # Drop the category column in outputs
                            subset_df.drop(columns=[category_column], inplace=True, errors='ignore')
                            
                            sheet_name = clean_sheet_name(category_name)
                            subset_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                    logger.info(f"Successfully generated split file with {len(unique_categories)} sheets")
                except Exception as e:
                    logger.error(f"Error during Excel writing: {str(e)}")
                    return jsonify({"error": f"Error generating split file: {str(e)}"}), 500

                # Reset buffer position to beginning
                output_buffer.seek(0)

                # 7. Secure Remote Download (Base64)
                # This approach ensures the file is transferred as a data payload,
                # bypassing many remote server/proxy restrictions on binary downloads.
                encoded = base64.b64encode(output_buffer.read()).decode('utf-8')
                return jsonify({
                    "success": True,
                    "file": encoded, 
                    "filename": output_filename,
                    "summary": {
                        "total_rows": len(df),
                        "sheets_created": len(unique_categories)
                    }
                })

            except Exception as e:
                logger.exception("Unexpected error in split_excel")
                return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

        return render_template('split_excel.html')

    return splitter_bp

    return splitter_bp