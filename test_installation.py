#!/usr/bin/env python3
"""
Simple test script to validate MetaZenseCode installation
"""

def test_imports():
    """Test that all core modules can be imported"""
    print("🧪 Testing MetaZenseCode imports...")
    
    try:
        import metazcode
        print("✅ Core metazcode module imported successfully")
        
        from metazcode.cli import commands
        print("✅ CLI commands module imported successfully") 
        
        from metazcode.sdk.ingestion.ssis import ssis_parser
        print("✅ SSIS parser module imported successfully")
        
        from metazcode.sdk.graph import graph_client_interface
        print("✅ Graph client interface imported successfully")
        
        from metazcode.sdk.models import canonical_types
        print("✅ Canonical types module imported successfully")
        
        from metazcode.sdk.analysis import cross_package_analyzer
        print("✅ Cross-package analyzer imported successfully")
        
        return True
        
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False

def test_structure():
    """Test that required files exist"""
    import os
    
    print("\n🏗️  Testing directory structure...")
    
    required_files = [
        "metazcode/__init__.py",
        "metazcode/__main__.py", 
        "metazcode/cli/commands.py",
        "metazcode/sdk/ingestion/ssis/ssis_parser.py",
        "metazcode/sdk/models/canonical_types.py",
        "examples/sample_ssis_project/Package-SSIS/ETL_firsttime.dtsx",
        "pyproject.toml",
        "README.md"
    ]
    
    missing_files = []
    for file_path in required_files:
        if os.path.exists(file_path):
            print(f"✅ {file_path} exists")
        else:
            print(f"❌ {file_path} missing") 
            missing_files.append(file_path)
    
    return len(missing_files) == 0

def main():
    print("🚀 MetaZenseCode Installation Test")
    print("=" * 50)
    
    structure_ok = test_structure()
    imports_ok = test_imports()
    
    print("\n📊 Test Results:")
    print("=" * 50)
    
    if structure_ok and imports_ok:
        print("🎉 All tests passed! MetaZenseCode is ready to use.")
        print("\n🔧 Quick start commands:")
        print("  uv run python -m metazcode --help")
        print("  uv run python -m metazcode ingest --path examples/sample_ssis_project")
        return 0
    else:
        print("⚠️  Some tests failed. Please check the installation.")
        return 1

if __name__ == "__main__":
    exit(main())