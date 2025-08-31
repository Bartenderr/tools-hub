#!/usr/bin/env python3
"""
Claims Data Processing Tool - Main Application
Web-based automation tool for merging and analyzing tariff data with claims.
"""

import os
import uuid
import logging
import threading
import time
from pathlib import Path
from io import BytesIO
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
import gc

import pandas as pd
import numpy as np
from flask import Flask, request, jsonify, send_file, render_template
from werkzeug.utils import secure_filename
from werkzeug.exceptions import RequestEntityTooLarge
import openpyxl

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 3 * 1024 * 1024  # 3MB limit
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-key-change-in-production')

# Global storage for processing results (in-memory only)
processing_results: Dict[str, Dict] = {}
processing_lock = threading.Lock()

# Cleanup thread
def cleanup_old_results():
    """Remove results older than 5 minutes"""
    while True:
        try:
            current_time = datetime.now()
            with processing_lock:
                expired_keys = []
                for process_id, data in processing_results.items():
                    if current_time - data['created_at'] > timedelta(minutes=5):
                        expired_keys.append(process_id)
                
                for key in expired_keys:
                    del processing_results[key]
                    logger.info(f"Cleaned up expired result: {key}")
            
            # Force garbage collection
            gc.collect()
        except Exception as e:
            logger.error(f"Error in cleanup thread: {e}")
        
        time.sleep(60)  # Check every minute

# Start cleanup thread
cleanup_thread = threading.Thread(target=cleanup_old_results, daemon=True)
cleanup_thread.start()


class FileValidator:
    """Handles file validation and quality checks"""
    
    ALLOWED_EXTENSIONS = {'.xlsx', '.xls'}
    MAX_FILE_SIZE = 3 * 1024 * 1024  # 3MB
    
    @staticmethod
    def validate_file_basic(file) -> Tuple[bool, str]:
        """Basic file validation"""
        if not file or not file.filename:
            return False, "No file provided"
        
        # Check extension
        filename = secure_filename(file.filename)
        if not any(filename.lower().endswith(ext) for ext in FileValidator.ALLOWED_EXTENSIONS):
            return False, f"Invalid file type. Only {', '.join(FileValidator.ALLOWED_EXTENSIONS)} are allowed"
        
        # Check size (Flask handles this automatically, but we can add explicit check)
        file.seek(0, 2)  # Seek to end
        size = file.tell()
        file.seek(0)  # Reset to beginning
        
        if size > FileValidator.MAX_FILE_SIZE:
            return False, f"File too large. Maximum size is {FileValidator.MAX_FILE_SIZE / (1024*1024):.1f}MB"
        
        return True, "Valid"
    
    @staticmethod
    def validate_excel_content(file_content: bytes, expected_columns: List[str], 
                             file_type: str) -> Tuple[bool, str, Optional[pd.DataFrame]]:
        """Validate Excel file content and structure"""
        try:
            # Try to read the Excel file
            excel_file = pd.ExcelFile(BytesIO(file_content))
            
            if file_type == "original_tariff":
                return FileValidator._validate_tariff_file(excel_file)
            elif file_type == "claims_export":
                return FileValidator._validate_claims_file(excel_file)
            elif file_type == "ds_output":
                return FileValidator._validate_ds_output_file(excel_file)
            else:
                return False, "Unknown file type", None
                
        except Exception as e:
            return False, f"Error reading Excel file: {str(e)}", None
    
    @staticmethod
    def _validate_tariff_file(excel_file) -> Tuple[bool, str, Optional[pd.DataFrame]]:
        """Validate original tariff file structure"""
        try:
            # Check if there are sheets other than SUMMARY
            valid_sheets = [sheet for sheet in excel_file.sheet_names if sheet != 'SUMMARY']
            if not valid_sheets:
                return False, "No valid tariff sheets found (excluding SUMMARY)", None
            
            # Check first valid sheet for TARIFF NAME column
            first_sheet = excel_file.parse(valid_sheets[0])
            tariff_name_col = None
            
            # Look for TARIFF NAME column (flexible naming)
            for col in first_sheet.columns:
                if 'tariff' in col.lower() and 'name' in col.lower():
                    tariff_name_col = col
                    break
            
            if tariff_name_col is None:
                return False, "TARIFF NAME column not found in tariff sheets", None
            
            # Check data quality
            empty_ratio = first_sheet[tariff_name_col].isna().sum() / len(first_sheet)
            if empty_ratio >= 0.2:
                return False, f"Too many empty values in TARIFF NAME column ({empty_ratio:.1%})", None
            elif empty_ratio > 0.05:
                logger.warning(f"Some empty values in TARIFF NAME column ({empty_ratio:.1%})")
            
            return True, "Valid tariff file", first_sheet
            
        except Exception as e:
            return False, f"Error validating tariff file: {str(e)}", None
    
    @staticmethod
    def _validate_claims_file(excel_file) -> Tuple[bool, str, Optional[pd.DataFrame]]:
        """Validate claims export file structure"""
        try:
            # Assume single sheet or first sheet
            df = excel_file.parse(excel_file.sheet_names[0])
            
            # Look for required columns
            required_cols = {
            'claim_text': 'Claim Text',
            'submission_date': 'Submission Date'
        }
            
            # Validate presence
            missing = [v for v in required_cols.values() if v not in df.columns]
            if missing:
                return False, f"Missing required columns: {missing}", None
            
            return True, "Validation successful", df
                
        except Exception as e:
            return False, f"Error validating claims file: {str(e)}", None
    
    @staticmethod
    def _validate_ds_output_file(excel_file) -> Tuple[bool, str, Optional[pd.DataFrame]]:
        """Validate data science output file structure"""
        try:
            # Assume single sheet or first sheet
            df = excel_file.parse(excel_file.sheet_names[0])
            
            # Look for raw_input column
            raw_input_col = None
            for col in df.columns:
                if 'raw' in col.lower() and 'input' in col.lower():
                    raw_input_col = col
                    break
            
            if raw_input_col is None:
                return False, "raw_input column not found", None
            
            return True, "Valid DS output file", df
            
        except Exception as e:
            return False, f"Error validating DS output file: {str(e)}", None


class ClaimsProcessor:
    """Main processing engine for claims data"""
    
    def __init__(self, match_threshold: float = 0.83):
        self.match_threshold = match_threshold
        self.progress_callback = None
    
    def set_progress_callback(self, callback):
        """Set callback function for progress updates"""
        self.progress_callback = callback
    
    def _update_progress(self, progress: int, step: str):
        """Update progress if callback is set"""
        if self.progress_callback:
            self.progress_callback(progress, step)
    
    def process_claims_data(self, original_tariff_content: bytes, 
                          claims_content: bytes, 
                          ds_output_content: bytes) -> Dict[str, Any]:
        """
        Main processing function that merges and analyzes the three input files
        """
        try:
            self._update_progress(10, "Loading original tariff...")
            current_tariff = self._process_original_tariff(original_tariff_content)
            
            self._update_progress(30, "Processing claims export...")
            clean_met_df = self._process_claims_export(claims_content)
            
            self._update_progress(50, "Loading DS model output...")
            ds_output = self._process_ds_output(ds_output_content)
            
            self._update_progress(70, "Merging datasets...")
            merged_df = self._merge_datasets(current_tariff, clean_met_df, ds_output)
            
            self._update_progress(85, "Calculating match percentages...")
            final_df = self._apply_business_logic(merged_df)
            
            self._update_progress(95, "Creating output files...")
            result_sheets = self._create_output_sheets(final_df)
            
            self._update_progress(100, "Processing complete!")
            
            return {
                'success': True,
                'sheets': result_sheets,
                'statistics': self._calculate_statistics(result_sheets)
            }
            
        except Exception as e:
            logger.error(f"Processing error: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _process_original_tariff(self, content: bytes) -> pd.DataFrame:
        """Process original tariff file"""
        excel_file = pd.ExcelFile(BytesIO(content))
        full_tariff = []
        
        for sheet_name in excel_file.sheet_names:
            if sheet_name != 'SUMMARY':
                df = excel_file.parse(sheet_name)
                
                # Find TARIFF NAME column
                tariff_name_col = None
                for col in df.columns:
                    if 'tariff' in col.lower() and 'name' in col.lower():
                        tariff_name_col = col
                        break
                
                if tariff_name_col:
                    df['tariff_type'] = sheet_name
                    df['TARIFF NAME'] = df[tariff_name_col].str.lower()
                    df['curry'] = 1
                    full_tariff.append(df)
        
        if not full_tariff:
            raise ValueError("No valid tariff sheets found")
        
        return pd.concat(full_tariff, ignore_index=True)
    
    def _process_claims_export(self, content: bytes) -> pd.DataFrame:
        """Process claims export file"""
        excel_file = pd.ExcelFile(BytesIO(content))
        df = excel_file.parse(excel_file.sheet_names[0])
        
        # Find required columns
        col_mapping = {}
        for col in df.columns:
            col_lower = col.lower().replace(' ', '_')
            if 'claim' in col_lower and 'text' in col_lower:
                col_mapping['Claim Text'] = col
            elif 'submission' in col_lower and 'date' in col_lower:
                col_mapping['Submission Date'] = col
            elif 'unit' in col_lower and 'price' in col_lower:
                col_mapping['Unit Price'] = col
            elif 'encounter' in col_lower:
                col_mapping['Encountered At'] = col
        
        # Rename columns for consistency
        df = df.rename(columns={v: k for k, v in col_mapping.items()})
        
        # Sort by submission date and deduplicate
        if 'Submission Date' in df.columns:
            df = df.sort_values(by='Submission Date').reset_index(drop=True)
        
        # Deduplicate by Claim Text, keeping the last occurrence
        df_deduplicated = df.drop_duplicates(subset=["Claim Text"], keep="last")
        
        # Select relevant columns
        columns_to_keep = ['Claim Text']
        for col in ['Submission Date', 'Unit Price', 'Encountered At']:
            if col in df_deduplicated.columns:
                columns_to_keep.append(col)
        
        return df_deduplicated[columns_to_keep]
    
    def _process_ds_output(self, content: bytes) -> pd.DataFrame:
        """Process data science model output file"""
        excel_file = pd.ExcelFile(BytesIO(content))
        df = excel_file.parse(excel_file.sheet_names[0])
        
        # Find raw_input column
        raw_input_col = None
        for col in df.columns:
            if 'raw' in col.lower() and 'input' in col.lower():
                raw_input_col = col
                break
        
        if raw_input_col and raw_input_col != 'raw_input':
            df = df.rename(columns={raw_input_col: 'raw_input'})
        
        return df
    
    def _merge_datasets(self, tariff_df: pd.DataFrame, 
                       claims_df: pd.DataFrame, 
                       ds_df: pd.DataFrame) -> pd.DataFrame:
        """Merge the three datasets"""
        
        tariff_df['tariff_name_key'] = tariff_df['TARIFF NAME'].astype(str).map(str.lower)
        ds_df['raw_input_key'] = ds_df['raw_input'].astype(str).map(str.lower)
        claims_df['claim_text_key'] = claims_df['Claim Text'].astype(str).map(str.lower)
                
        # First merge: tariff with DS output
        logger.debug(f"Merging tariff ({len(tariff_df)}) with DS output ({len(ds_df)})")
        merged_1 = pd.merge(
            tariff_df,
            ds_df,
            left_on='tariff_name_key',
            right_on='raw_input_key',
            how='left'
        )
        logger.debug(f"First merge result: {merged_1.shape}")
        
        # Clean up the merge key column
        if 'key_0' in merged_1.columns:
            merged_1 = merged_1.drop(columns=['key_0'])
        
        # Second merge: result with claims
        logger.debug(f"Merging merged_1 with claims ({len(claims_df)})")
        merged_2 = pd.merge(
            merged_1,
            claims_df,
            left_on='tariff_name_key',
            right_on='claim_text_key',
            how='left'
        )
        logger.debug(f"Final merged result: {merged_2.shape}")
        
        # Clean up the merge key column
        if 'key_0' in merged_2.columns:
            merged_2 = merged_2.drop(columns=['key_0'])
        
        return merged_2
    
    def _apply_business_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply business logic and create mapping codes"""
        
        # Create found in 1 year indicator
        df['1_y'] = np.where(df['Claim Text'].notna(), 1, 0)
        
        # Sort by priority
        df = df.sort_values(
            by=['curry', 'tariff_active', '1_y', 'match_percent'], 
            ascending=False
        ).reset_index(drop=True)
        
        # Create mapping codes
        df['mapping_code'] = df.apply(self._build_mapping_code, axis=1)
        
        # Create mapping descriptions
        mapping_descriptions = {
            '11G1Y': 'current, tariffactive6month, goodmatch, found in 1year',
            '11B1Y': 'current, tariffactive6month, badmatch, found in 1year',
            '11G0Y': 'current, tariffactive6month, goodmatch, not found in 1 year',
            '11B0Y': 'current, tariffactive6month, badmatch, not found in 1year',
            '10G1Y': 'current, not in tariffactive 6month, goodmatch, found in 1year',
            '10B1Y': 'current, not in tariffactive 6month, badmatch, found in 1year',
            '10G0Y': 'current, not in tariffactive 6month, goodmatch, not found in 1year',
            '10B0Y': 'current, not in tariffactive 6month, badmatch, not found in 1year',
            '1NN1Y': 'current, not in model output, not semi-standardized, found in 1year',
            '1NN0Y': 'current, not in model output, not semi-standardized, not found in 1year'
        }
        
        df['mapping_description'] = df['mapping_code'].map(mapping_descriptions).fillna('Unknown mapping code')
        
        # Clean up unwanted columns
        cols_to_drop = [col for col in df.columns if col in [
            'Unnamed: 0', 's/n', 'median_price', 'tariff_type', 'location_ref',
            'matched_tariff', 'provider_type_name', 'input', 'Encountered At',
            'Submission Date', 'Unit Price', 'Claim Text', 'claim_text_key', 'tariff_name_key'
        ]]
        
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
        
        df.rename(columns={"tariff_type_x":"tariff_type"}, inplace=True)
        
        return df
    
    def _build_mapping_code(self, row) -> str:
        """Build mapping code for a single row"""
        code_parts = []
        
        # Current: curry column
        code_parts.append('1' if row.get('curry') == 1 else '0')
        
        # Tariff Active (6 month): tariff_active column
        if pd.isna(row.get('tariff_active')):
            code_parts.append('N')
        elif row.get('tariff_active') == 1:
            code_parts.append('1')
        else:
            code_parts.append('0')
        
        # Match Quality: match_percent column
        if pd.isna(row.get('match_percent')):
            code_parts.append('N')
        elif row.get('match_percent', 0) >= self.match_threshold:
            code_parts.append('G')
        else:
            code_parts.append('B')
        
        # Found in 1 year: 1_y column
        code_parts.append('1Y' if row.get('1_y') == 1 else '0Y')
        
        return ''.join(code_parts)
    
    def _create_output_sheets(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Create the three output sheets"""
        
        # Good match sheet
        good_match_df = df[df['mapping_description'].str.contains('good', case=False, na=False)].copy()
        
        # Poor match sheet
        poor_match_condition = (
            df['mapping_description'].str.contains('bad', case=False, na=False) |
            df['mapping_description'].str.contains('not semi-standardized', case=False, na=False)
        )
        poor_match_df = df[poor_match_condition].copy()
        
        return {
            'good_match': good_match_df,
            'poor_match': poor_match_df,
            'full_data': df
        }
    
    def _calculate_statistics(self, sheets: Dict[str, pd.DataFrame]) -> Dict[str, int]:
        """Calculate processing statistics"""
        return {
            'total_records': len(sheets['full_data']),
            'good_matches': len(sheets['good_match']),
            'poor_matches': len(sheets['poor_match'])
        }


# Flask routes
@app.route('/')
def index():
    """Serve the tools hub landing page"""
    return render_template('tools_hub_landing.html')

@app.route('/standard-workflow')
def standard_workflow():
    """Serve the claims processing page"""
    return render_template('claims_processing_mockup.html')

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

@app.route('/api/process-claims', methods=['POST'])
def process_claims():
    """Main processing endpoint"""
    try:
        # Check if files are present
        required_files = ['originalTariff', 'claimsExport', 'dsOutput']
        for file_key in required_files:
            if file_key not in request.files:
                return jsonify({'error': f'Missing file: {file_key}'}), 400
        
        # Get match threshold
        match_threshold = float(request.form.get('matchThreshold', 0.83))
        
        # Validate files
        files_content = {}
        file_types = {
            'originalTariff': 'original_tariff',
            'claimsExport': 'claims_export', 
            'dsOutput': 'ds_output'
        }
        
        for file_key in required_files:
            file = request.files[file_key]
            
            # Basic validation
            is_valid, error_msg = FileValidator.validate_file_basic(file)
            if not is_valid:
                return jsonify({'error': f'{file_key}: {error_msg}'}), 400
            
            # Read file content
            file_content = file.read()
            files_content[file_key] = file_content
            
            # Content validation
            file_type = file_types[file_key]
            is_valid, error_msg, _ = FileValidator.validate_excel_content(
                file_content, [], file_type
            )
            if not is_valid:
                return jsonify({'error': f'{file_key}: {error_msg}'}), 400
        
        # Create processing job
        process_id = str(uuid.uuid4())
        
        with processing_lock:
            processing_results[process_id] = {
                'status': 'processing',
                'progress': 0,
                'current_step': 'Initializing...',
                'created_at': datetime.now(),
                'result': None,
                'error': None
            }
        
        # Start processing in background thread
        def background_process():
            try:
                processor = ClaimsProcessor(match_threshold)
                
                # Set progress callback
                def update_progress(progress, step):
                    with processing_lock:
                        if process_id in processing_results:
                            processing_results[process_id]['progress'] = progress
                            processing_results[process_id]['current_step'] = step
                
                processor.set_progress_callback(update_progress)
                
                # Process the data
                result = processor.process_claims_data(
                    files_content['originalTariff'],
                    files_content['claimsExport'],
                    files_content['dsOutput']
                )
                
                # Store result
                with processing_lock:
                    if process_id in processing_results:
                        processing_results[process_id]['status'] = 'completed'
                        processing_results[process_id]['result'] = result
                        processing_results[process_id]['progress'] = 100
                        
            except Exception as e:
                logger.error(f"Background processing error: {str(e)}")
                with processing_lock:
                    if process_id in processing_results:
                        processing_results[process_id]['status'] = 'error'
                        processing_results[process_id]['error'] = str(e)
        
        # Start background thread
        thread = threading.Thread(target=background_process)
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'processing_id': process_id,
            'status': 'processing',
            'estimated_time': 10
        })
        
    except RequestEntityTooLarge:
        return jsonify({'error': 'File too large. Maximum size is 3MB per file.'}), 413
    except Exception as e:
        logger.error(f"Processing request error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/processing-status/<process_id>')
def get_processing_status(process_id):
    """Get processing status"""
    with processing_lock:
        if process_id not in processing_results:
            return jsonify({'error': 'Processing ID not found'}), 404
        
        data = processing_results[process_id]
        response = {
            'status': data['status'],
            'progress': data['progress'],
            'current_step': data['current_step']
        }
        
        if data['status'] == 'completed' and data['result']:
            response['statistics'] = data['result'].get('statistics', {})
        elif data['status'] == 'error':
            response['error'] = data['error']
        
        return jsonify(response)

@app.route('/api/download/<process_id>')
def download_results(process_id):
    """Download processing results"""
    sheet_type = request.args.get('sheet', 'all')
    custom_filename = request.args.get('filename', '')
    
    with processing_lock:
        if process_id not in processing_results:
            return jsonify({'error': 'Processing ID not found'}), 404
        
        data = processing_results[process_id]
        if data['status'] != 'completed' or not data['result']:
            return jsonify({'error': 'Processing not completed or failed'}), 400
        
        if not data['result']['success']:
            return jsonify({'error': data['result'].get('error', 'Processing failed')}), 400
    
    try:
        sheets = data['result']['sheets']
        
        # Create Excel file in memory
        output = BytesIO()
        
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            if sheet_type == 'all':
                sheets['good_match'].to_excel(writer, sheet_name='Good Match', index=False)
                sheets['poor_match'].to_excel(writer, sheet_name='Poor Match', index=False)
                sheets['full_data'].to_excel(writer, sheet_name='Full Data', index=False)
                filename = custom_filename or f'tariff_st_analysis_complete_{datetime.now().strftime("%Y-%m-%d_%H-%M")}.xlsx'
            elif sheet_type == 'good_match':
                sheets['good_match'].to_excel(writer, sheet_name='Good Match', index=False)
                filename = custom_filename or f'tariff_st_analysis_good_{datetime.now().strftime("%Y-%m-%d_%H-%M")}.xlsx'
            elif sheet_type == 'poor_match':
                sheets['poor_match'].to_excel(writer, sheet_name='Poor Match', index=False)
                filename = custom_filename or f'tariff_st_analysis_poor_{datetime.now().strftime("%Y-%m-%d_%H-%M")}.xlsx'
            elif sheet_type == 'full_data':
                sheets['full_data'].to_excel(writer, sheet_name='Full Data', index=False)
                filename = custom_filename or f'tariff_st_analysis_full_{datetime.now().strftime("%Y-%m-%d_%H-%M")}.xlsx'
            else:
                return jsonify({'error': 'Invalid sheet type'}), 400
        
        output.seek(0)
        
        # Ensure filename has .xlsx extension
        if not filename.endswith('.xlsx'):
            filename += '.xlsx'
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        logger.error(f"Download error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.errorhandler(413)
def request_entity_too_large(error):
    return jsonify({'error': 'File too large. Maximum size is 3MB per file.'}), 413

@app.errorhandler(500)
def internal_server_error(error):
    logger.error(f"Internal server error: {str(error)}")
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8030, debug=False)