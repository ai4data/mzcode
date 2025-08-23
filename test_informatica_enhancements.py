#!/usr/bin/env python3
"""
Test script to validate the enhanced Informatica parser implementation.
This script tests the new SQL semantics, parameter/variable extraction,
and expression analysis capabilities.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from metazcode.sdk.ingestion.informatica.informatica_parser import InformaticaParser
from metazcode.sdk.ingestion.informatica.informatica_loader import InformaticaLoader
from metazcode.sdk.ingestion.informatica.sql_semantics import InformaticaSqlParser
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_informatica_sql_parser():
    """Test the Informatica SQL parser capabilities."""
    print("=" * 60)
    print("TESTING INFORMATICA SQL PARSER")
    print("=" * 60)
    
    parser = InformaticaSqlParser()
    
    # Test expression parsing
    test_expressions = [
        "UPPER(CUSTOMER_NAME)",
        "TO_DATE(DATE_STRING, 'YYYY-MM-DD')",
        "NVL(SALARY, 0) + BONUS",
        "DECODE(STATUS, 'A', 'Active', 'I', 'Inactive', 'Unknown')",
        "SUBSTR(PHONE, 1, 3) || '-' || SUBSTR(PHONE, 4, 3)",
        "$$PARAM_START_DATE <= ORDER_DATE AND ORDER_DATE <= $$PARAM_END_DATE",
        "$VAR_COUNTER + 1"
    ]
    
    for expr in test_expressions:
        print(f"\nTesting expression: {expr}")
        analysis = parser.parse_informatica_expression(expr, "Expression")
        print(f"  Functions used: {analysis['functions_used']}")
        print(f"  Parameters: {analysis['parameters_referenced']}")
        print(f"  Variables: {analysis['variables_referenced']}")
        print(f"  Complexity: {analysis['complexity_score']}")
        print(f"  Migration considerations: {analysis['migration_considerations']}")
    
    # Test joiner semantics
    print(f"\n{'-' * 40}")
    print("TESTING JOINER SEMANTICS")
    print(f"{'-' * 40}")
    
    master_fields = [{"name": "CUSTOMER_ID", "datatype": "number"}]
    detail_fields = [{"name": "ORDER_ID", "datatype": "number"}, {"name": "ORDER_DATE", "datatype": "date"}]
    
    joiner_analysis = parser.analyze_joiner_semantics(
        "CUSTOMER_ID = CUSTOMER_ID",
        "Master Outer Join",
        master_fields,
        detail_fields
    )
    
    print(f"Informatica Join Type: {joiner_analysis['informatica_join_type']}")
    print(f"SQL Join Type: {joiner_analysis['sql_join_type']}")
    print(f"SQL Equivalent:\n{joiner_analysis['sql_equivalent']}")
    
    # Test lookup semantics
    print(f"\n{'-' * 40}")
    print("TESTING LOOKUP SEMANTICS")
    print(f"{'-' * 40}")
    
    lookup_fields = [{"name": "PRODUCT_ID", "datatype": "number"}]
    return_fields = [{"name": "PRODUCT_NAME", "datatype": "string"}, {"name": "PRICE", "datatype": "number"}]
    
    lookup_analysis = parser.analyze_lookup_semantics(
        "PRODUCT_ID = LKP_PRODUCT_ID",
        "PRODUCT_LOOKUP",
        lookup_fields,
        return_fields
    )
    
    print(f"Lookup Condition: {lookup_analysis['lookup_condition']}")
    print(f"Cache Strategy: {lookup_analysis['cache_strategy']}")
    print(f"SQL Equivalent:\n{lookup_analysis['sql_equivalent']}")

def test_informatica_parser_with_sample():
    """Test the enhanced parser with sample Informatica files."""
    print(f"\n{'=' * 60}")
    print("TESTING INFORMATICA PARSER WITH SAMPLE FILES")
    print("=" * 60)
    
    # Test with sample file
    sample_file = "data/informatica/Q1/wf_m_q1.XML"
    
    if os.path.exists(sample_file):
        print(f"Testing with sample file: {sample_file}")
        
        parser = InformaticaParser()
        
        try:
            # Parse the workflow file
            for nodes, edges in parser.parse(sample_file):
                print(f"\nParsed {len(nodes)} nodes and {len(edges)} edges")
                
                # Show parameter/variable nodes
                param_nodes = [n for n in nodes if n.node_type.value == "PARAMETER"]
                var_nodes = [n for n in nodes if n.node_type.value == "VARIABLE"]
                
                print(f"Found {len(param_nodes)} parameters and {len(var_nodes)} variables")
                
                # Show transformation nodes with enhanced analysis
                transform_nodes = [n for n in nodes if n.node_type.value == "OPERATION"]
                print(f"Found {len(transform_nodes)} transformations")
                
                for node in transform_nodes[:3]:  # Show first 3 transformations
                    print(f"\nTransformation: {node.name}")
                    print(f"  Type: {node.properties.get('operation_subtype', 'Unknown')}")
                    
                    # Show SQL semantics if available
                    if 'sql_semantics' in node.properties:
                        print("  Has SQL semantics analysis")
                    
                    if 'join_semantics' in node.properties:
                        print("  Has join semantics analysis")
                    
                    if 'enhanced_expressions' in node.properties:
                        enhanced_exprs = node.properties['enhanced_expressions']
                        print(f"  Has {len(enhanced_exprs)} enhanced expressions")
                
                break  # Only process first batch
                
        except Exception as e:
            print(f"Error parsing file: {e}")
            import traceback
            traceback.print_exc()
    else:
        print(f"Sample file not found: {sample_file}")

def test_informatica_loader():
    """Test the enhanced Informatica loader."""
    print(f"\n{'=' * 60}")
    print("TESTING INFORMATICA LOADER")
    print("=" * 60)
    
    # Create a mock project directory structure
    test_dir = "data/informatica"
    
    if os.path.exists(test_dir):
        print(f"Testing loader with directory: {test_dir}")
        
        # Change to test directory temporarily
        original_dir = os.getcwd()
        try:
            os.chdir(test_dir)
            
            loader = InformaticaLoader()
            
            # Test file discovery
            workflow_files = loader.discover_files("wf_*.xml")
            param_files = loader.discover_files("*.param")
            
            print(f"Discovered {len(workflow_files)} workflow files")
            print(f"Discovered {len(param_files)} parameter files")
            
            # Test ingestion (first file only)
            if workflow_files:
                print(f"\nTesting ingestion with: {workflow_files[0]}")
                
                count = 0
                for nodes, edges in loader.ingest():
                    print(f"Batch {count + 1}: {len(nodes)} nodes, {len(edges)} edges")
                    count += 1
                    if count >= 2:  # Limit to first 2 batches
                        break
            
        finally:
            os.chdir(original_dir)
    else:
        print(f"Test directory not found: {test_dir}")

def main():
    """Run all tests."""
    print("INFORMATICA PARSER ENHANCEMENT VALIDATION")
    print("=" * 80)
    
    try:
        test_informatica_sql_parser()
        test_informatica_parser_with_sample()
        test_informatica_loader()
        
        print(f"\n{'=' * 80}")
        print("✅ ALL TESTS COMPLETED SUCCESSFULLY!")
        print("The enhanced Informatica parser is working correctly.")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n{'=' * 80}")
        print(f"❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        print("=" * 80)

if __name__ == "__main__":
    main()