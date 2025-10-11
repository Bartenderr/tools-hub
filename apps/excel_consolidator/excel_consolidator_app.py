"""
Excel Consolidator Application Blueprint
"""

import io
import logging
import tempfile
from typing import Dict, List
import pandas as pd
from flask import Blueprint, render_template, request, send_file, jsonify

# Configure logging
logger = logging.getLogger(__name__)

class ExcelProcessor:
    def __init__(self):
        self.consolidated_df = None
        self.processing_log = []
    
    def log_message(self, message):
        self.processing_log.append(message)
        print(message)
    
    def find_column_mapping(self, columns):
        """Intelligently map column names to required fields"""
        mapping = {}
        columns_lower = [col.lower().strip() for col in columns]
        
        patterns = {
            'tariff_name': ['tariff name', 'tariff_name', 'name', 'item name', 'procedure'],
            'tariff_type': ['tariff type', 'tariff_type', 'type', 'category', 'class'],
            'snomed_code': ['snomed code', 'snomed_code', 'code', 'snomed', 'procedure code'],
            'snomed_description': ['snomed description en', 'snomed description', 'snomed_description', 'description', 'desc', 'procedure description']
        }
        
        for field, possible_names in patterns.items():
            for i, col_lower in enumerate(columns_lower):
                for pattern in possible_names:
                    if pattern == col_lower:
                        mapping[field] = columns[i]
                        break
                if field in mapping:
                    break
        
        return mapping
    
    def validate_and_clean_data(self, df, sheet_name=None):
        """Validate and clean the data according to constraints"""
        original_count = len(df)
        
        # Drop rows with any missing values
        df_clean = df.dropna()
        
        # Convert SNOMED CODE to string and remove decimals
        if 'SNOMED CODE' in df_clean.columns:
            df_clean['SNOMED CODE'] = df_clean['SNOMED CODE'].astype(str)
            df_clean['SNOMED CODE'] = df_clean['SNOMED CODE'].str.replace(r'\.0$', '', regex=True)
        
        # Convert TARIFF TYPE to uppercase
        if 'TARIFF TYPE' in df_clean.columns:
            df_clean['TARIFF TYPE'] = df_clean['TARIFF TYPE'].astype(str).str.upper()
        
        # If sheet_name provided and TARIFF TYPE is missing, use sheet name
        if sheet_name and 'TARIFF TYPE' in df_clean.columns:
            df_clean['TARIFF TYPE'] = df_clean['TARIFF TYPE'].fillna(sheet_name.upper())
            df_clean.loc[df_clean['TARIFF TYPE'].str.strip() == '', 'TARIFF TYPE'] = sheet_name.upper()
        
        dropped_count = original_count - len(df_clean)
        if dropped_count > 0:
            self.log_message(f"  → Dropped {dropped_count} rows with missing values")
        
        return df_clean
    
    def process_excel_file(self, file_content, filename):
        """Process a single Excel file"""
        self.log_message(f"\n📄 Processing: {filename}")
        
        try:
            # Read Excel file to get sheet names
            excel_file = pd.ExcelFile(io.BytesIO(file_content))
            all_data = []
            
            for sheet_name in excel_file.sheet_names:
                self.log_message(f"  📋 Processing sheet: {sheet_name}")
                
                # Read the sheet
                df = pd.read_excel(io.BytesIO(file_content), sheet_name=sheet_name)
                
                if df.empty:
                    self.log_message("  → Sheet is empty, skipping")
                    continue
                
                # Find column mapping
                column_mapping = self.find_column_mapping(df.columns.tolist())
                
                if not column_mapping:
                    self.log_message("  → No valid columns found, skipping sheet")
                    continue
                
                # Rename columns
                df = df[list(column_mapping.values())].rename(columns={v: k for k, v in column_mapping.items()})
                
                # Clean data
                df_clean = self.validate_and_clean_data(df, sheet_name)
                
                if not df_clean.empty:
                    all_data.append(df_clean)
                    self.log_message(f"  ✅ Successfully processed {len(df_clean)} rows")
                else:
                    self.log_message("  → No valid data after cleaning")
            
            if all_data:
                return pd.concat(all_data, ignore_index=True)
            else:
                self.log_message("❌ No valid data found in file")
                return None
                
        except Exception as e:
            self.log_message(f"❌ Error processing file: {str(e)}")
            return None

def create_excel_consolidator_app():
    """Create the excel consolidator application blueprint"""
    excel_consolidator_bp = Blueprint('excel_consolidator', __name__)
    processor = ExcelProcessor()
    
    @excel_consolidator_bp.route('/')
    def index():
        """Serve the upload page"""
        return render_template('excel_consolidator.html')
    
    @excel_consolidator_bp.route('/process', methods=['POST'])
    def process_files():
        if 'files[]' not in request.files:
            return jsonify({'success': False, 'detail': 'No files provided'}), 400
        
        files = request.files.getlist('files[]')
        if not files:
            return jsonify({'success': False, 'detail': 'No files selected'}), 400
        
        processor.processing_log = []
        processor.log_message("🚀 Starting file processing...")
        
        all_dataframes = []
        processed_count = 0
        
        for file in files:
            if not file.filename.endswith(('.xlsx', '.xls')):
                processor.log_message(f"❌ Skipping {file.filename}: Not an Excel file")
                continue
            
            try:
                file_content = file.read()
                df = processor.process_excel_file(file_content, file.filename)
                if df is not None:
                    all_dataframes.append(df)
                    processed_count += 1
            except Exception as e:
                processor.log_message(f"❌ Error processing {file.filename}: {str(e)}")
        
        if all_dataframes:
            processor.consolidated_df = pd.concat(all_dataframes, ignore_index=True)
            
            # Remove duplicates
            original_count = len(processor.consolidated_df)
            processor.consolidated_df.drop_duplicates(inplace=True)
            duplicate_count = original_count - len(processor.consolidated_df)
            
            processor.log_message(f"\n🎯 CONSOLIDATION COMPLETE!")
            processor.log_message(f"📊 Total rows in consolidated data: {len(processor.consolidated_df)}")
            if duplicate_count > 0:
                processor.log_message(f"🧹 Removed {duplicate_count} duplicate rows")
            processor.log_message(f"✅ Successfully processed {processed_count}/{len(files)} files")
            
            return jsonify({
                "success": True,
                "log": processor.processing_log,
                "stats": {
                    "total_rows": len(processor.consolidated_df),
                    "files_processed": processed_count,
                    "duplicates_removed": duplicate_count
                }
            })
        else:
            processor.log_message(f"\n❌ No valid data found in any files!")
            return jsonify({
                "success": False,
                "log": processor.processing_log,
                "detail": "No valid data could be extracted from the selected files."
            })
    
    @excel_consolidator_bp.route('/download')
    def download_consolidated_file():
        if processor.consolidated_df is None or processor.consolidated_df.empty:
            return jsonify({'error': 'No consolidated data available'}), 400
        
        # Create Excel file in memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            processor.consolidated_df.to_excel(writer, sheet_name='Consolidated_Data', index=False)
        
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='consolidated_tariff_data.xlsx'
        )
    
    return excel_consolidator_bp