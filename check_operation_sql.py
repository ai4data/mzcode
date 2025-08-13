#!/usr/bin/env python3
"""Check what SQL content is actually stored in the operation nodes"""

from metazcode.sdk.graph.client_memgraph import MemgraphClient
from metazcode.sdk.models.config import DatabaseConfig
import json

def main():
    config = DatabaseConfig(
        backend="memgraph",
        memgraph_host="localhost",
        memgraph_port=7687,
        memgraph_username="admin",
        memgraph_password="admin"
    )
    client = MemgraphClient(config)
    
    try:
        print("🔍 Checking SQL content in operation nodes...")
        
        # Get operations that might contain SQL from Product.dtsx
        query = """
        MATCH (n:Node) 
        WHERE n.node_type = 'operation' 
        AND (toLower(n.name) CONTAINS 'product' OR toLower(toString(n.properties)) CONTAINS 'product')
        RETURN n.id, n.name, n.properties
        """
        
        results = client._execute_query(query)
        print(f"Found {len(results)} operations related to Product")
        
        for result in results:
            node_id, name, properties = result
            print(f"\n🔧 Operation: {name}")
            print(f"   ID: {node_id}")
            
            if isinstance(properties, dict):
                # Look for any SQL-related content
                sql_keys = [k for k in properties.keys() if 'sql' in k.lower()]
                if sql_keys:
                    print(f"   SQL-related keys: {sql_keys}")
                    for key in sql_keys:
                        value = properties[key]
                        if value and len(str(value)) > 50:
                            print(f"   {key}: {str(value)[:200]}...")
                        else:
                            print(f"   {key}: {value}")
                
                # Check column lineage for SQL traces
                if 'column_lineage' in properties:
                    print(f"   ✅ Has column lineage data")
                    lineage = properties['column_lineage']
                    if isinstance(lineage, list) and len(lineage) > 0:
                        print(f"   Components in lineage: {len(lineage)}")
                        for component in lineage:
                            if isinstance(component, dict):
                                comp_name = component.get('component_name', 'Unknown')
                                print(f"     - {comp_name}")
                                
                                # Check if there are any SQL references in component
                                comp_str = str(component)
                                if 'categories' in comp_str.lower():
                                    print(f"       🎯 Found 'Categories' reference in {comp_name}!")
                
                # Check for any other properties mentioning Categories
                prop_str = str(properties)
                if 'categories' in prop_str.lower():
                    print(f"   🎯 Contains 'Categories' reference somewhere in properties!")
                    
            else:
                print(f"   Properties: {properties}")
                
        print(f"\n" + "="*60)
        print("🔍 Now checking what table nodes DO exist...")
        
        table_query = """
        MATCH (n:Node) 
        WHERE n.node_type = 'table'
        RETURN n.id, n.name, n.properties
        ORDER BY n.name
        """
        
        table_results = client._execute_query(table_query)
        print(f"Found {len(table_results)} table nodes:")
        
        for result in table_results:
            node_id, name, properties = result
            print(f"  📋 Table: {name} (ID: {node_id})")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()

if __name__ == "__main__":
    main()