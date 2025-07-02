#!/bin/bash
# MetaZenseCode Quick Test Script

echo "🚀 MetaZenseCode Quick Test"
echo "=========================="

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "❌ uv package manager not found. Please install uv first:"
    echo "   curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

echo "✅ uv package manager found"

# Install dependencies
echo "📦 Installing dependencies..."
uv sync

if [ $? -ne 0 ]; then
    echo "❌ Failed to install dependencies"
    exit 1
fi

echo "✅ Dependencies installed successfully"

# Run installation test
echo "🧪 Running installation test..."
uv run python test_installation.py

if [ $? -eq 0 ]; then
    echo ""
    echo "🎉 Ready to analyze SSIS projects!"
    echo ""
    echo "Try these commands:"
    echo "  uv run python -m metazcode --help"
    echo "  uv run python -m metazcode ingest --path examples/sample_ssis_project"
    echo "  uv run python -m metazcode analyze --path examples/sample_ssis_project"
else
    echo "❌ Installation test failed"
    exit 1
fi