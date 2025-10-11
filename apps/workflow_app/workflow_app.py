#!/usr/bin/env python3
"""
Claims Data Processing Tool - Workflow Application
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
from flask import Blueprint, request, jsonify, send_file, render_template
from werkzeug.utils import secure_filename
from werkzeug.exceptions import RequestEntityTooLarge
import openpyxl

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global storage for processing results (in-memory only)
processing_results: Dict[str, Dict] = {}
processing_lock = threading.Lock()

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

    # Include all other FileValidator methods here...

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
    
    # Include all other ClaimsProcessor methods here...

def create_workflow_app():
    """Create the workflow application blueprint"""
    workflow_bp = Blueprint('workflow', __name__)

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

    @workflow_bp.route('/')
    def index():
        """Serve the claims processing page"""
        return render_template('claims_processing_mockup.html')

    @workflow_bp.route('/api/health')
    def health_check():
        """Health check endpoint"""
        return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

    @workflow_bp.route('/api/process-claims', methods=['POST'])
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
            
            # Process in background thread
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

    @workflow_bp.route('/api/processing-status/<process_id>')
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

    @workflow_bp.route('/api/download/<process_id>')
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

    return workflow_bp