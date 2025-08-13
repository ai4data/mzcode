#!/usr/bin/env python3
"""Investigate operations in Memgraph to see what SQL was extracted"""

import json
from metazcode.sdk.graph.client_memgraph import MemgraphClient
from metazcode.sdk.models.config import DatabaseConfig

def main():
    # Initialize Memgraph client with config
    config = DatabaseConfig(
        backend="memgraph",
        memgraph_host="localhost",
        memgraph_port=7687,
        memgraph_username="admin",
        memgraph_password="admin"
    )
    client = MemgraphClient(config)
    
    try:
        print("✅ Connected to Memgraph")
        
        # Get all operations from Product.dtsx
        print("\n🔍 Searching for operations in Product.dtsx...")
        query1 = """
        MATCH (n:Node) 
        WHERE n.node_type = 'operation' 
        AND (toLower(n.file_path) CONTAINS 'product.dtsx' OR toLower(n.name) CONTAINS 'product')
        RETURN n.id, n.name, n.properties
        """
        
        results1 = client._execute_query(query1)
        if results1:
            print(f"Found {len(results1)} operations in Product.dtsx:")
            for result in results1:
                print(f"\n  🔧 Operation: {result[1]} (ID: {result[0]})")
                props = result[2]
                print(f"     Properties keys: {list(props.keys()) if isinstance(props, dict) else 'Not a dict'}")
                
                # Check for SQL content
                if isinstance(props, dict):
                    for key, value in props.items():
                        if 'sql' in key.lower() and value and 'categories' in str(value).lower():
                            print(f"     ✅ Found Categories in {key}:")
                            print(f"        {value[:200]}...")
                        elif 'sql' in key.lower() and value:
                            print(f"     📝 SQL in {key} (length: {len(str(value))})")
                            if len(str(value)) < 500:  # Show short SQL
                                print(f"        {value}")
                            else:
                                print(f"        {str(value)[:200]}...")
        else:
            print("❌ No operations found in Product.dtsx")
            
        # Get ALL operations and their SQL content
        print("\n\n🔍 Examining ALL operations for SQL content...")
        query2 = """
        MATCH (n:Node) 
        WHERE n.node_type = 'operation'
        RETURN n.id, n.name, n.properties
        LIMIT 10
        """
        
        results2 = client._execute_query(query2)
        if results2:
            print(f"Found {len(results2)} operations (showing first 10):")
            for result in results2:
                print(f"\n  🔧 Operation: {result[1]} (ID: {result[0]})")
                props = result[2]
                
                if isinstance(props, dict):
                    # Look for any SQL-related properties
                    sql_props = [k for k in props.keys() if 'sql' in k.lower() or 'query' in k.lower() or 'command' in k.lower()]
                    if sql_props:
                        print(f"     SQL-related properties: {sql_props}")
                        for prop in sql_props:
                            value = props[prop]
                            if value and 'categories' in str(value).lower():
                                print(f"     🎯 FOUND Categories in {prop}!")
                                print(f"        {value}")
                            elif value:
                                print(f"     📝 {prop}: {str(value)[:100]}...")
                else:
                    print(f"     Properties: {props}")
            
        # Also check for any mention of "ole db source" operations
        print("\n\n🔍 Searching for OLE DB Source operations...")
        query3 = """
        MATCH (n:Node) 
        WHERE n.node_type = 'operation' 
        AND toLower(n.name) CONTAINS 'ole db source'
        RETURN n.id, n.name, n.properties
        """
        
        results3 = client._execute_query(query3)
        if results3:
            print(f"Found {len(results3)} OLE DB Source operations:")
            for result in results3:
                print(f"\n  📥 OLE DB Source: {result[1]} (ID: {result[0]})")
                props = result[2]
                if isinstance(props, dict):
                    for key, value in props.items():
                        if value and 'categories' in str(value).lower():
                            print(f"     🎯 FOUND Categories in {key}!")
                            print(f"        {value}")
                        elif 'sql' in key.lower() or 'query' in key.lower():
                            print(f"     📝 {key}: {str(value)[:150]}...")
        else:
            print("❌ No OLE DB Source operations found")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()
        print("\n🔌 Disconnected from Memgraph")

if __name__ == "__main__":
    main()