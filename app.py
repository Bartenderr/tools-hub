#!/usr/bin/env python3
"""
Tools Hub - Main Application
Entry point for all tool applications.
"""

import os
import sys
import logging
from pathlib import Path

# Add the current directory to Python path for local imports
current_dir = Path(__file__).resolve().parent
if str(current_dir) not in sys.path:
    sys.path.append(str(current_dir))

from flask import Flask, render_template, jsonify

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_app():
    """Create and configure the main Flask application"""
    app = Flask(__name__)
    app.config['MAX_CONTENT_LENGTH'] = 3 * 1024 * 1024  # 3MB limit
    app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-key-change-in-production')

    # Import blueprints here to avoid circular imports
    from apps.workflow_app.workflow_app import create_workflow_app
    from apps.splitter_app.splitter_app import create_splitter_app
    from apps.diagnosis_processor.diagnosis_app import create_diagnosis_app
    from apps.excel_consolidator.excel_consolidator_app import create_excel_consolidator_app

    # Register blueprints
    app.register_blueprint(create_workflow_app(), url_prefix='/standard-workflow')
    app.register_blueprint(create_splitter_app(), url_prefix='/split-excel')
    app.register_blueprint(create_diagnosis_app(), url_prefix='/process-diagnosis')
    app.register_blueprint(create_excel_consolidator_app(), url_prefix='/excel-consolidator')

    # Main route
    @app.route('/')
    def index():
        """Serve the tools hub landing page"""
        return render_template('tools_hub_landing.html')

    @app.route('/api/health')
    def health_check():
        """Health check endpoint for Docker"""
        return jsonify({'status': 'healthy', 'timestamp': os.uname().nodename})

    @app.errorhandler(413)
    def request_entity_too_large(error):
        return jsonify({'error': 'File too large. Maximum size is 3MB per file.'}), 413

    @app.errorhandler(500)
    def internal_server_error(error):
        logger.error(f"Internal server error: {str(error)}")
        return jsonify({'error': 'Internal server error'}), 500

    return app

if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=8030, debug=False)