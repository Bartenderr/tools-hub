#!/bin/bash

# Claims Processing Tool Setup Script
# This script helps set up the application for development or production

set -e  # Exit on any error

echo "🚀 Claims Processing Tool Setup"
echo "================================"

# Function to generate secure secret key
generate_secret_key() {
    python3 -c "import secrets; print(secrets.token_hex(32))"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check for required tools
echo "📋 Checking prerequisites..."

if ! command_exists python3; then
    echo "❌ Python 3 is required but not installed."
    exit 1
fi

if ! command_exists docker; then
    echo "⚠️  Docker not found. Docker deployment will not be available."
    DOCKER_AVAILABLE=false
else
    DOCKER_AVAILABLE=true
fi

if ! command_exists docker-compose; then
    echo "⚠️  Docker Compose not found. Using docker compose instead."
    DOCKER_COMPOSE_CMD="docker compose"
else
    DOCKER_COMPOSE_CMD="docker-compose"
fi

echo "✅ Prerequisites checked"

# Get setup type from user
echo ""
echo "🔧 Setup Options:"
echo "1) Development setup (local Python environment)"
echo "2) Production setup (Docker)"
echo "3) Quick test (Docker with defaults)"

read -p "Choose setup type (1-3): " setup_type

case $setup_type in
    1)
        echo ""
        echo "🐍 Setting up development environment..."
        
        # Check Python version
        python_version=$(python3 --version | cut -d' ' -f2)
        echo "Python version: $python_version"
        
        # Create virtual environment
        echo "Creating virtual environment..."
        python3 -m venv venv
        
        # Activate virtual environment
        echo "Activating virtual environment..."
        source venv/bin/activate
        
        # Install dependencies
        echo "Installing dependencies..."
        pip install --upgrade pip
        pip install -r requirements.txt
        
        # Create environment file
        if [ ! -f .env ]; then
            echo "Creating .env file..."
            cp .env.example .env
            secret_key=$(generate_secret_key)
            sed -i.bak "s/your-secret-key-change-this-in-production/$secret_key/" .env
            rm .env.bak 2>/dev/null || true
        fi
        
        echo ""
        echo "✅ Development setup complete!"
        echo ""
        echo "To start the application:"
        echo "  source venv/bin/activate"
        echo "  python workflow_app.py"
        echo ""
        echo "Then visit: http://localhost:5000"
        ;;
        
    2)
        if [ "$DOCKER_AVAILABLE" = false ]; then
            echo "❌ Docker is required for production setup but not available."
            exit 1
        fi
        
        echo ""
        echo "🐳 Setting up production environment..."
        
        # Create environment file
        if [ ! -f .env ]; then
            echo "Creating .env file..."
            cp .env.example .env
            secret_key=$(generate_secret_key)
            sed -i.bak "s/your-secret-key-change-this-in-production/$secret_key/" .env
            rm .env.bak 2>/dev/null || true
            echo "FLASK_ENV=production" >> .env
        fi
        
        # Create logs directory
        mkdir -p logs
        
        echo ""
        echo "🔒 Please review and update the .env file with production values:"
        echo "  - Verify SECRET_KEY is secure"
        echo "  - Set any additional environment variables"
        echo ""
        read -p "Press Enter when ready to continue..."
        
        # Build and start containers
        echo "Building Docker containers..."
        $DOCKER_COMPOSE_CMD build
        
        echo ""
        echo "✅ Production setup complete!"
        echo ""
        echo "To start the application:"
        echo "  $DOCKER_COMPOSE_CMD up -d"
        echo ""
        echo "To view logs:"
        echo "  $DOCKER_COMPOSE_CMD logs -f"
        echo ""
        echo "Then visit: http://localhost:5000"
        ;;
        
    3)
        if [ "$DOCKER_AVAILABLE" = false ]; then
            echo "❌ Docker is required for quick test but not available."
            exit 1
        fi
        
        echo ""
        echo "⚡ Quick test setup..."
        
        # Create minimal environment file
        if [ ! -f .env ]; then
            echo "Creating minimal .env file..."
            secret_key=$(generate_secret_key)
            cat > .env << EOF
FLASK_ENV=production
SECRET_KEY=$secret_key
EOF
        fi
        
        # Create logs directory
        mkdir -p logs
        
        # Build and start
        echo "Building and starting containers..."
        $DOCKER_COMPOSE_CMD up --build -d
        
        echo ""
        echo "✅ Quick test setup complete!"
        echo ""
        echo "Application is starting up..."
        echo "Visit: http://localhost:5000"
        echo ""
        echo "To stop:"
        echo "  $DOCKER_COMPOSE_CMD down"
        ;;
        
    *)
        echo "❌ Invalid selection"
        exit 1
        ;;
esac

echo ""
echo "🎉 Setup complete!"
echo ""
echo "📖 Additional resources:"
echo "  - README.md: Comprehensive documentation"
echo "  - API endpoints: /api/health for health checks"
echo "  - Logs: Check application logs for troubleshooting"
echo ""
echo "⚠️  Important reminders:"
echo "  - Do not upload files containing PII"
echo "  - Files are automatically deleted after 5 minutes"
echo "  - Maximum file size: 3MB per file"