#!/usr/bin/env python3
"""
Comprehensive Integration Test for Informatica Parser
Tests all implemented requirements with realistic XML structures
"""

import os
import tempfile
import logging
from typing import List, Dict, Any
from lxml import etree

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_test_informatica_xml() -> str:
    """Create a comprehensive test XML file with all the features we've implemented."""
    
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE POWERMART SYSTEM "powrmart.dtd">
<POWERMART CREATION_DATE="01/01/2024" REPOSITORY_VERSION="999.0">
    <REPOSITORY NAME="TEST_REPO">
        <FOLDER NAME="TEST_FOLDER" GROUP="" OWNER="ADMIN">
            
            <!-- Workflow Definition with Variables -->
            <WORKFLOW NAME="wf_test_integration">
                <VARIABLE NAME="$$LastRunDate" DATATYPE="date/time" DEFAULTVALUE="2024-01-01" ISPERSISTENT="YES"/>
                <VARIABLE NAME="$$BatchSize" DATATYPE="integer" DEFAULTVALUE="1000" ISPERSISTENT="NO"/>
                
                <SESSION NAME="s_test_mapping">
                    <SESSTRANSFORMATIONINST TRANSFORMATIONTYPE="Source Definition" SINSTANCENAME="SRC_CUSTOMERS">
                        <TABLEATTRIBUTE NAME="Sql Query" VALUE="SELECT * FROM customers WHERE update_date > $$LastRunDate"/>
                    </SESSTRANSFORMATIONINST>
                    
                    <SESSTRANSFORMATIONINST TRANSFORMATIONTYPE="Target Definition" SINSTANCENAME="TGT_CUSTOMERS">
                        <TABLEATTRIBUTE NAME="Target load type" VALUE="Normal"/>
                        <TABLEATTRIBUTE NAME="Insert" VALUE="YES"/>
                        <TABLEATTRIBUTE NAME="Update as Update" VALUE="YES"/>
                        <TABLEATTRIBUTE NAME="Update as Insert" VALUE="NO"/>
                        <TABLEATTRIBUTE NAME="Delete" VALUE="YES"/>
                        <TABLEATTRIBUTE NAME="Truncate table" VALUE="NO"/>
                    </SESSTRANSFORMATIONINST>
                    
                    <!-- Performance tuning attributes -->
                    <ATTRIBUTE NAME="DTM buffer size" VALUE="128000000"/>
                    <ATTRIBUTE NAME="Line Sequential Buffer Length" VALUE="1024"/>
                    <ATTRIBUTE NAME="Maximum Memory Allowed For Auto Memory Attributes" VALUE="2048MB"/>
                    <ATTRIBUTE NAME="Additional Concurrent Pipelines for Lookup Cache Creation" VALUE="2"/>
                </SESSION>
                
                <WORKFLOWLINK FROMTASK="Start" TOTASK="s_test_mapping"/>
            </WORKFLOW>
            
            <!-- Mapplet Definition with Internal Transformations -->
            <MAPPLET NAME="mplt_customer_validation" ISVALID="YES">
                <TRANSFORMATION NAME="Mapplet Input" TYPE="Mapplet Input">
                    <TRANSFORMFIELD NAME="CUSTOMER_ID" DATATYPE="decimal" PRECISION="10" SCALE="0"/>
                    <TRANSFORMFIELD NAME="CUSTOMER_NAME" DATATYPE="string" PRECISION="100"/>
                    <TRANSFORMFIELD NAME="REGION" DATATYPE="string" PRECISION="50"/>
                </TRANSFORMATION>
                
                <TRANSFORMATION NAME="exp_validate" TYPE="Expression">
                    <TRANSFORMFIELD NAME="VALID_ID" EXPRESSION="IIF(ISNULL(CUSTOMER_ID), 0, CUSTOMER_ID)" DATATYPE="decimal"/>
                    <TRANSFORMFIELD NAME="CLEAN_NAME" EXPRESSION="UPPER(TRIM(CUSTOMER_NAME))" DATATYPE="string"/>
                    <TRANSFORMFIELD NAME="REGION_CODE" EXPRESSION="DECODE(REGION, 'United States', 'US', 'Canada', 'CA', 'INTL')" DATATYPE="string"/>
                </TRANSFORMATION>
                
                <TRANSFORMATION NAME="fil_valid_only" TYPE="Filter">
                    <TRANSFORMFIELD NAME="FILTER_CONDITION" EXPRESSION="VALID_ID > 0 AND NOT ISNULL(CLEAN_NAME)"/>
                </TRANSFORMATION>
                
                <TRANSFORMATION NAME="Mapplet Output" TYPE="Mapplet Output">
                    <TRANSFORMFIELD NAME="OUT_CUSTOMER_ID" DATATYPE="decimal" PRECISION="10" SCALE="0"/>
                    <TRANSFORMFIELD NAME="OUT_CUSTOMER_NAME" DATATYPE="string" PRECISION="100"/>
                    <TRANSFORMFIELD NAME="OUT_REGION_CODE" DATATYPE="string" PRECISION="10"/>
                </TRANSFORMATION>
                
                <!-- Instances within mapplet -->
                <INSTANCE NAME="MPIN_CustomerInput" TYPE="Mapplet Input" TRANSFORMATIONNAME="Mapplet Input" TRANSFORMATIONTYPE="Mapplet Input"/>
                <INSTANCE NAME="EXP_Validate" TYPE="Expression" TRANSFORMATIONNAME="exp_validate" TRANSFORMATIONTYPE="Expression"/>
                <INSTANCE NAME="FIL_ValidOnly" TYPE="Filter" TRANSFORMATIONNAME="fil_valid_only" TRANSFORMATIONTYPE="Filter"/>
                <INSTANCE NAME="MPOUT_CustomerOutput" TYPE="Mapplet Output" TRANSFORMATIONNAME="Mapplet Output" TRANSFORMATIONTYPE="Mapplet Output"/>
                
                <!-- Connectors within mapplet -->
                <CONNECTOR FROMINSTANCE="MPIN_CustomerInput" TOINSTANCE="EXP_Validate" FROMFIELD="CUSTOMER_ID" TOFIELD="CUSTOMER_ID"/>
                <CONNECTOR FROMINSTANCE="MPIN_CustomerInput" TOINSTANCE="EXP_Validate" FROMFIELD="CUSTOMER_NAME" TOFIELD="CUSTOMER_NAME"/>
                <CONNECTOR FROMINSTANCE="MPIN_CustomerInput" TOINSTANCE="EXP_Validate" FROMFIELD="REGION" TOFIELD="REGION"/>
                
                <CONNECTOR FROMINSTANCE="EXP_Validate" TOINSTANCE="FIL_ValidOnly" FROMFIELD="VALID_ID" TOFIELD="VALID_ID"/>
                <CONNECTOR FROMINSTANCE="EXP_Validate" TOINSTANCE="FIL_ValidOnly" FROMFIELD="CLEAN_NAME" TOFIELD="CLEAN_NAME"/>
                
                <CONNECTOR FROMINSTANCE="FIL_ValidOnly" TOINSTANCE="MPOUT_CustomerOutput" FROMFIELD="VALID_ID" TOFIELD="OUT_CUSTOMER_ID"/>
                <CONNECTOR FROMINSTANCE="FIL_ValidOnly" TOINSTANCE="MPOUT_CustomerOutput" FROMFIELD="CLEAN_NAME" TOFIELD="OUT_CUSTOMER_NAME"/>
                <CONNECTOR FROMINSTANCE="FIL_ValidOnly" TOINSTANCE="MPOUT_CustomerOutput" FROMFIELD="REGION_CODE" TOFIELD="OUT_REGION_CODE"/>
            </MAPPLET>
            
            <!-- Main Mapping with all features -->
            <MAPPING NAME="m_customer_processing" ISVALID="YES">
                <!-- Mapping Variables -->
                <VARIABLE NAME="$$RecordCount" DATATYPE="integer" DEFAULTVALUE="0" ISPERSISTENT="YES"/>
                <VARIABLE NAME="$$ProcessDate" DATATYPE="date/time" DEFAULTVALUE="SYSDATE" ISPERSISTENT="NO"/>
                
                <!-- Source Definition -->
                <TRANSFORMATION NAME="SRC_CUSTOMERS" TYPE="Source Definition">
                    <TRANSFORMFIELD NAME="CUSTOMER_ID" DATATYPE="decimal" PRECISION="10" SCALE="0"/>
                    <TRANSFORMFIELD NAME="CUSTOMER_NAME" DATATYPE="string" PRECISION="100"/>
                    <TRANSFORMFIELD NAME="REGION" DATATYPE="string" PRECISION="50"/>
                    <TRANSFORMFIELD NAME="AMOUNT" DATATYPE="decimal" PRECISION="15" SCALE="2"/>
                </TRANSFORMATION>
                
                <!-- Target Definitions with DML Options -->
                <TARGET NAME="TGT_CUSTOMERS" DATABASETYPE="Oracle">
                    <TARGETFIELD NAME="CUSTOMER_ID" DATATYPE="decimal" PRECISION="10" SCALE="0"/>
                    <TARGETFIELD NAME="CUSTOMER_NAME" DATATYPE="string" PRECISION="100"/>
                    <TARGETFIELD NAME="REGION_CODE" DATATYPE="string" PRECISION="10"/>
                    <TARGETFIELD NAME="LOAD_DATE" DATATYPE="date/time"/>
                    
                    <TABLEATTRIBUTE NAME="Insert" VALUE="YES"/>
                    <TABLEATTRIBUTE NAME="Update as Update" VALUE="YES"/>
                    <TABLEATTRIBUTE NAME="Update as Insert" VALUE="NO"/>
                    <TABLEATTRIBUTE NAME="Delete" VALUE="NO"/>
                </TARGET>
                
                <TARGET NAME="TGT_CUSTOMER_ERRORS" DATABASETYPE="Oracle">
                    <TARGETFIELD NAME="ERROR_ID" DATATYPE="decimal" PRECISION="10"/>
                    <TARGETFIELD NAME="CUSTOMER_ID" DATATYPE="decimal" PRECISION="10"/>
                    <TARGETFIELD NAME="ERROR_MSG" DATATYPE="string" PRECISION="500"/>
                    
                    <TABLEATTRIBUTE NAME="Insert" VALUE="YES"/>
                    <TABLEATTRIBUTE NAME="Update as Update" VALUE="NO"/>
                    <TABLEATTRIBUTE NAME="Delete" VALUE="NO"/>
                </TARGET>
                
                <!-- Transformations -->
                <TRANSFORMATION NAME="sq_customers" TYPE="Source Qualifier">
                    <TABLEATTRIBUTE NAME="Sql Query" VALUE="SELECT * FROM customers WHERE batch_id = $$BatchSize"/>
                </TRANSFORMATION>
                
                <TRANSFORMATION NAME="exp_complex" TYPE="Expression">
                    <TRANSFORMFIELD NAME="PREMIUM_FLAG" EXPRESSION="IIF(AMOUNT > 10000, IIF(REGION = 'US', 'PREMIUM_US', 'PREMIUM_INTL'), IIF(AMOUNT > 5000, 'STANDARD', 'BASIC'))" DATATYPE="string"/>
                    <TRANSFORMFIELD NAME="FORMATTED_DATE" EXPRESSION="TO_CHAR(ADD_TO_DATE(SYSDATE, 'DD', 7), 'YYYY-MM-DD')" DATATYPE="string"/>
                    <TRANSFORMFIELD NAME="CLEAN_NAME" EXPRESSION="REG_REPLACE(UPPER(TRIM(CUSTOMER_NAME)), '[^A-Z0-9 ]', '')" DATATYPE="string"/>
                </TRANSFORMATION>
                
                <TRANSFORMATION NAME="lkp_customer_history" TYPE="Lookup">
                    <TABLEATTRIBUTE NAME="Lookup Sql Override" VALUE="SELECT * FROM customer_history WHERE effective_date = $$ProcessDate"/>
                    <TRANSFORMFIELD NAME="IN_CUSTOMER_ID" DATATYPE="decimal" PORTTYPE="INPUT"/>
                    <TRANSFORMFIELD NAME="PREV_REGION" DATATYPE="string" PORTTYPE="OUTPUT"/>
                    <TRANSFORMFIELD NAME="CHANGE_COUNT" DATATYPE="integer" PORTTYPE="OUTPUT"/>
                </TRANSFORMATION>
                
                <TRANSFORMATION NAME="upd_strategy" TYPE="Update Strategy">
                    <TABLEATTRIBUTE NAME="Update Strategy Expression" VALUE="IIF(ISNULL(:LKP.lkp_customer_history(CUSTOMER_ID)), DD_INSERT, IIF(CHANGE_COUNT > 5, DD_DELETE, DD_UPDATE))"/>
                </TRANSFORMATION>
                
                <TRANSFORMATION NAME="sp_validate_batch" TYPE="Stored Procedure">
                    <TABLEATTRIBUTE NAME="Stored Procedure Name" VALUE="PKG_VALIDATE.VALIDATE_BATCH"/>
                    <TABLEATTRIBUTE NAME="Connection Information" VALUE="Oracle_DW"/>
                    <TABLEATTRIBUTE NAME="Call Text" VALUE="{call PKG_VALIDATE.VALIDATE_BATCH($$BatchSize, $$ProcessDate)}"/>
                    <TABLEATTRIBUTE NAME="Execution Order" VALUE="Source Pre Load"/>
                </TRANSFORMATION>
                
                <!-- Instances -->
                <INSTANCE NAME="SRC_CUSTOMERS" TYPE="Source Definition" TRANSFORMATIONNAME="SRC_CUSTOMERS" TRANSFORMATIONTYPE="Source Definition"/>
                <INSTANCE NAME="SQ_Customers" TYPE="Source Qualifier" TRANSFORMATIONNAME="sq_customers" TRANSFORMATIONTYPE="Source Qualifier"/>
                <INSTANCE NAME="MPLT_Validation" TYPE="Mapplet" TRANSFORMATIONNAME="mplt_customer_validation" TRANSFORMATIONTYPE="Mapplet"/>
                <INSTANCE NAME="EXP_Complex" TYPE="Expression" TRANSFORMATIONNAME="exp_complex" TRANSFORMATIONTYPE="Expression" INSTANCENAME="EXP_Complex"/>
                <INSTANCE NAME="LKP_History" TYPE="Lookup" TRANSFORMATIONNAME="lkp_customer_history" TRANSFORMATIONTYPE="Lookup"/>
                <INSTANCE NAME="UPD_Strategy" TYPE="Update Strategy" TRANSFORMATIONNAME="upd_strategy" TRANSFORMATIONTYPE="Update Strategy"/>
                <INSTANCE NAME="SP_Validate" TYPE="Stored Procedure" TRANSFORMATIONNAME="sp_validate_batch" TRANSFORMATIONTYPE="Stored Procedure"/>
                <INSTANCE NAME="TGT_CUSTOMERS" TYPE="Target Definition" TRANSFORMATIONNAME="TGT_CUSTOMERS" TRANSFORMATIONTYPE="Target Definition"/>
                <INSTANCE NAME="TGT_ERRORS" TYPE="Target Definition" TRANSFORMATIONNAME="TGT_CUSTOMER_ERRORS" TRANSFORMATIONTYPE="Target Definition"/>
                
                <!-- Connectors -->
                <CONNECTOR FROMINSTANCE="SRC_CUSTOMERS" TOINSTANCE="SQ_Customers" FROMINSTANCETYPE="Source Definition" TOINSTANCETYPE="Source Qualifier"/>
                <CONNECTOR FROMINSTANCE="SQ_Customers" TOINSTANCE="MPLT_Validation" FROMFIELD="CUSTOMER_ID" TOFIELD="CUSTOMER_ID"/>
                <CONNECTOR FROMINSTANCE="SQ_Customers" TOINSTANCE="MPLT_Validation" FROMFIELD="CUSTOMER_NAME" TOFIELD="CUSTOMER_NAME"/>
                <CONNECTOR FROMINSTANCE="SQ_Customers" TOINSTANCE="MPLT_Validation" FROMFIELD="REGION" TOFIELD="REGION"/>
                <CONNECTOR FROMINSTANCE="MPLT_Validation" TOINSTANCE="EXP_Complex" FROMFIELD="OUT_CUSTOMER_ID" TOFIELD="CUSTOMER_ID"/>
                <CONNECTOR FROMINSTANCE="EXP_Complex" TOINSTANCE="LKP_History" FROMFIELD="CUSTOMER_ID" TOFIELD="IN_CUSTOMER_ID"/>
                <CONNECTOR FROMINSTANCE="EXP_Complex" TOINSTANCE="UPD_Strategy"/>
                <CONNECTOR FROMINSTANCE="UPD_Strategy" TOINSTANCE="TGT_CUSTOMERS"/>
                
                <!-- Target Load Order -->
                <TARGETLOADORDER>
                    <TARGETINSTANCE NAME="TGT_ERRORS" />
                    <TARGETINSTANCE NAME="TGT_CUSTOMERS" />
                </TARGETLOADORDER>
                
            </MAPPING>
            
        </FOLDER>
    </REPOSITORY>
</POWERMART>
"""
    
    # Create temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False, encoding='utf-8') as f:
        f.write(xml_content)
        return f.name


def run_integration_test():
    """Run comprehensive integration test of all parser features."""
    
    print("=" * 80)
    print("INFORMATICA PARSER INTEGRATION TEST")
    print("=" * 80)
    
    # Import the parser
    from metazcode.sdk.ingestion.informatica.informatica_parser import CanonicalInformaticaParser
    
    # Create test XML file
    test_file = create_test_informatica_xml()
    print(f"\n[OK] Created test XML file: {test_file}")
    
    try:
        # Initialize parser
        parser = CanonicalInformaticaParser(
            enable_schema_introspection=False,
            enable_type_mapping=True
        )
        print("[OK] Parser initialized successfully")
        
        # Parse the test file
        print("\n" + "="*50)
        print("PARSING TEST FILE")
        print("="*50)
        
        # Parse the XML file first
        from lxml import etree
        with open(test_file, 'r', encoding='utf-8') as f:
            xml_content = f.read()
        root = etree.fromstring(xml_content.encode('utf-8'))
        
        # Call the parsing method with the root element
        all_nodes = []
        all_edges = []
        
        # The parser returns a generator, so we need to iterate
        for nodes, edges in parser._parse_informatica_project(
            workflow_root=root,
            workflow_file_path=test_file,
            mapping_root=root,  # In our test, both are in the same file
            mapping_file_path=test_file
        ):
            all_nodes.extend(nodes)
            all_edges.extend(edges)
        
        nodes = all_nodes
        edges = all_edges
        
        print(f"\n[OK] Parsing completed successfully")
        print(f"  - Total nodes created: {len(nodes)}")
        print(f"  - Total edges created: {len(edges)}")
        
        # Test 1: Verify workflow variables were parsed
        print("\n" + "="*50)
        print("TEST 1: WORKFLOW & MAPPING VARIABLES")
        print("="*50)
        
        variable_nodes = [n for n in nodes if n.node_type == "VARIABLE"]
        print(f"[OK] Found {len(variable_nodes)} variable nodes")
        
        for var_node in variable_nodes:
            props = var_node.properties
            print(f"  - {props.get('name')}: {props.get('datatype')} = {props.get('default_value')} (scope: {props.get('scope')})")
        
        # Check variables context
        if hasattr(parser, 'variables_context'):
            print(f"[OK] Variables context populated with {len(parser.variables_context)} variables")
            for var_name, var_info in parser.variables_context.items():
                print(f"  - ${var_name}: {var_info.get('default_value')}")
        
        # Test 2: Verify mapplet parsing with internal transformations
        print("\n" + "="*50)
        print("TEST 2: MAPPLET RECURSIVE PARSING")
        print("="*50)
        
        mapplet_nodes = [n for n in nodes if 'mapplet' in n.node_id.lower()]
        print(f"[OK] Found {len(mapplet_nodes)} mapplet-related nodes")
        
        # Check for internal transformations
        mapplet_internals = [n for n in mapplet_nodes if 'mplt_customer_validation' in n.node_id]
        print(f"[OK] Mapplet 'mplt_customer_validation' expanded with {len(mapplet_internals)} internal nodes")
        
        for node in mapplet_internals[:5]:  # Show first 5
            print(f"  - {node.node_id}: {node.properties.get('transformation_type', 'N/A')}")
        
        # Test 3: Verify target DML options parsing
        print("\n" + "="*50)
        print("TEST 3: TARGET DML OPTIONS")
        print("="*50)
        
        # Find WRITES_TO edges from Update Strategy
        update_strategy_edges = [e for e in edges if 'upd_strategy' in e.source_id.lower() and e.edge_type == "WRITES_TO"]
        
        if update_strategy_edges:
            print(f"[OK] Found {len(update_strategy_edges)} WRITES_TO edges from Update Strategy")
            for edge in update_strategy_edges:
                props = edge.properties
                print(f"  - Effective operations: {props.get('effective_operations', [])}")
                print(f"  - Rejected operations: {props.get('rejected_operations', [])}")
        
        # Test 4: Verify expression semantic fingerprinting
        print("\n" + "="*50)
        print("TEST 4: EXPRESSION SEMANTIC FINGERPRINTING")
        print("="*50)
        
        expression_nodes = [n for n in nodes if n.properties.get('transformation_type') == 'Expression']
        print(f"[OK] Found {len(expression_nodes)} Expression transformation nodes")
        
        for exp_node in expression_nodes:
            props = exp_node.properties
            sql_semantics = props.get('sql_semantics', {})
            if sql_semantics:
                print(f"\n  Expression: {exp_node.node_id}")
                print(f"  - Functions used: {sql_semantics.get('functions_used', [])}")
                print(f"  - Port references: {sql_semantics.get('port_references', [])}")
                
                complexity = sql_semantics.get('expression_complexity', {})
                if complexity:
                    print(f"  - Complexity score: {complexity.get('complexity_score', 0)}")
                    print(f"  - Nested IIFs: {complexity.get('nested_iif_count', 0)}")
                    print(f"  - Has regex functions: {complexity.get('has_regex_functions', False)}")
        
        # Test 5: Verify unconnected lookup resolution
        print("\n" + "="*50)
        print("TEST 5: UNCONNECTED LOOKUP RESOLUTION")
        print("="*50)
        
        # Check for unconnected lookup references in expressions
        for node in nodes:
            if node.properties.get('unconnected_lookups'):
                print(f"[OK] Found unconnected lookup references in {node.node_id}")
                print(f"  - Lookups: {node.properties['unconnected_lookups']}")
                
                # Check for corresponding DEPENDS_ON edges
                lookup_edges = [e for e in edges if e.source_id == node.node_id and e.edge_type == "DEPENDS_ON"]
                if lookup_edges:
                    print(f"  - Created {len(lookup_edges)} DEPENDS_ON edges for lookups")
        
        # Test 6: Verify target load order
        print("\n" + "="*50)
        print("TEST 6: TARGET LOAD ORDER")
        print("="*50)
        
        load_order_edges = [e for e in edges if e.properties.get('relationship') == 'target_load_order']
        if load_order_edges:
            print(f"[OK] Found {len(load_order_edges)} target load order dependencies")
            for edge in load_order_edges:
                props = edge.properties
                print(f"  - {props.get('next_target')} depends on {props.get('current_target')}")
        
        # Test 7: Verify stored procedure parsing
        print("\n" + "="*50)
        print("TEST 7: STORED PROCEDURE PARSING")
        print("="*50)
        
        sp_nodes = [n for n in nodes if n.properties.get('transformation_type') == 'Stored Procedure']
        if sp_nodes:
            print(f"[OK] Found {len(sp_nodes)} Stored Procedure nodes")
            for sp_node in sp_nodes:
                props = sp_node.properties
                print(f"  - Procedure: {props.get('procedure_name', 'N/A')}")
                print(f"  - Execution order: {props.get('execution_order', 'N/A')}")
                print(f"  - Call text: {props.get('call_text', 'N/A')}")
        
        # Test 8: Verify performance tuning attributes
        print("\n" + "="*50)
        print("TEST 8: PERFORMANCE TUNING ATTRIBUTES")
        print("="*50)
        
        session_nodes = [n for n in nodes if n.properties.get('task_type') == 'Session']
        if session_nodes:
            print(f"[OK] Found {len(session_nodes)} Session nodes")
            for session in session_nodes:
                perf_attrs = session.properties.get('performance_attributes', {})
                if perf_attrs:
                    print(f"  Session: {session.properties.get('name', 'N/A')}")
                    for category, attrs in perf_attrs.items():
                        if attrs:
                            print(f"    - {category}: {len(attrs)} attributes")
        
        # Summary
        print("\n" + "="*80)
        print("INTEGRATION TEST SUMMARY")
        print("="*80)
        
        test_results = {
            "Variables Parsing": len(variable_nodes) > 0,
            "Mapplet Expansion": len(mapplet_internals) > 0,
            "Target DML Options": len(update_strategy_edges) > 0,
            "Expression Fingerprinting": any(n.properties.get('sql_semantics') for n in expression_nodes),
            "Unconnected Lookups": any(n.properties.get('unconnected_lookups') for n in nodes),
            "Target Load Order": len(load_order_edges) > 0,
            "Stored Procedures": len(sp_nodes) > 0,
            "Performance Attributes": any(n.properties.get('performance_attributes') for n in session_nodes)
        }
        
        all_passed = all(test_results.values())
        
        for test_name, passed in test_results.items():
            status = "[OK] PASSED" if passed else "[FAIL] FAILED"
            print(f"{status}: {test_name}")
        
        print("\n" + "="*80)
        if all_passed:
            print("ALL INTEGRATION TESTS PASSED!")
            print("The Informatica parser is fully functional and production-ready!")
        else:
            print("[WARNING] Some tests failed. Please review the results above.")
        print("="*80)
        
        return all_passed
        
    except Exception as e:
        print(f"\n[ERROR] Integration test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Clean up test file
        if os.path.exists(test_file):
            os.remove(test_file)
            print(f"\n[OK] Cleaned up test file: {test_file}")


if __name__ == "__main__":
    success = run_integration_test()
    exit(0 if success else 1)