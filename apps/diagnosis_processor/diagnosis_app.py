"""
Diagnosis Processing Application Blueprint
"""

import os
import uuid
import logging
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import threading
import time

import pandas as pd
from flask import Blueprint, request, jsonify, send_file, render_template
from werkzeug.utils import secure_filename

from .utils.file_validation import validate_file_content
from .utils.processor import process_diagnosis_data, generate_filenames

# Configure logging
logger = logging.getLogger(__name__)

# Global storage for processing results
processing_results: Dict[str, Dict] = {}
processing_lock = threading.Lock()

def create_diagnosis_app():
    """Create the diagnosis processor application blueprint"""
    diagnosis_bp = Blueprint('diagnosis', __name__)
    
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
            except Exception as e:
                logger.error(f"Error in cleanup thread: {e}")
            
            time.sleep(60)  # Check every minute
    
    # Start cleanup thread
    cleanup_thread = threading.Thread(target=cleanup_old_results, daemon=True)
    cleanup_thread.start()
    
    @diagnosis_bp.route('/')
    def index():
        """Serve the upload page"""
        return render_template('diagnosis_upload.html')
    
    @diagnosis_bp.route('/api/process', methods=['POST'])
    def process_file():
        """Process uploaded file"""
        try:
            if 'file' not in request.files:
                return jsonify({'error': 'No file provided'}), 400
            
            file = request.files['file']
            if not file or not file.filename:
                return jsonify({'error': 'No file selected'}), 400
            
            # Read file
            file_content = BytesIO(file.read())
            
            # Determine file type and read
            if file.filename.endswith('.csv'):
                df = pd.read_csv(file_content)
            else:
                df = pd.read_excel(file_content)
            
            # Validate content
            is_valid, error_msg = validate_file_content(df)
            if not is_valid:
                return jsonify({'error': error_msg}), 400
            
            # Create process ID
            process_id = str(uuid.uuid4())
            
            # Process in background
            def background_process():
                try:
                    # Process data
                    sheets, diagnosis_list = process_diagnosis_data(df)
                    
                    # Generate filenames
                    txt_filename, excel_filename = generate_filenames()
                    
                    # Create text file with Python variable format
                    diagnosis_list = [item.strip() for item in sorted(diagnosis_list)]  # Clean any whitespace
                    diagnosis_items = []
                    for item in diagnosis_list:
                        if item:  # Only add non-empty items
                            diagnosis_items.append(f'{item}')
                            if not item.endswith(','):  # Add comma version only if it doesn't already end with comma
                                diagnosis_items.append(f'{item}')
                    
                    txt_content = f'diagnosis = [{", ".join(diagnosis_items)}]'
                    txt_buffer = BytesIO(txt_content.encode('utf-8'))
                    
                    # Create Excel file
                    excel_buffer = BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                        for sheet_name, sheet_df in sheets.items():
                            sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                    excel_buffer.seek(0)
                    txt_buffer.seek(0)
                    
                    # Store results
                    with processing_lock:
                        processing_results[process_id] = {
                            'status': 'completed',
                            'created_at': datetime.now(),
                            'txt_file': {
                                'content': txt_buffer,
                                'filename': txt_filename
                            },
                            'excel_file': {
                                'content': excel_buffer,
                                'filename': excel_filename
                            },
                            'preview_data': {
                                sheet: df.head(10).to_dict('records') 
                                for sheet, df in sheets.items()
                            }
                        }
                        
                except Exception as e:
                    logger.error(f"Processing error: {str(e)}")
                    with processing_lock:
                        if process_id in processing_results:
                            processing_results[process_id] = {
                                'status': 'error',
                                'error': str(e),
                                'created_at': datetime.now()
                            }
            
            # Start background processing
            thread = threading.Thread(target=background_process)
            thread.daemon = True
            thread.start()
            
            return jsonify({
                'process_id': process_id,
                'status': 'processing'
            })
            
        except Exception as e:
            logger.error(f"File upload error: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    @diagnosis_bp.route('/api/status/<process_id>')
    def get_status(process_id):
        """Get processing status"""
        with processing_lock:
            if process_id not in processing_results:
                return jsonify({'error': 'Process not found'}), 404
            
            result = processing_results[process_id]
            if result['status'] == 'error':
                return jsonify({
                    'status': 'error',
                    'error': result['error']
                })
            elif result['status'] == 'completed':
                return jsonify({
                    'status': 'completed',
                    'preview_data': result['preview_data']
                })
            else:
                return jsonify({'status': 'processing'})
    
    @diagnosis_bp.route('/api/download/<process_id>/<file_type>')
    def download_file(process_id, file_type):
        """Download processed file"""
        with processing_lock:
            if process_id not in processing_results:
                return jsonify({'error': 'Process not found'}), 404
            
            result = processing_results[process_id]
            if result['status'] != 'completed':
                return jsonify({'error': 'Processing not completed'}), 400
            
            if file_type == 'txt':
                file_info = result['txt_file']
                mimetype = 'text/plain'
            elif file_type == 'excel':
                file_info = result['excel_file']
                mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            else:
                return jsonify({'error': 'Invalid file type'}), 400
            
            return send_file(
                file_info['content'],
                mimetype=mimetype,
                as_attachment=True,
                download_name=file_info['filename']
            )
    
    return diagnosis_bp
