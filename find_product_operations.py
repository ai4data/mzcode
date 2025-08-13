#!/usr/bin/env python3
"""Find Product.dtsx specific operations and SQL content"""

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
        print("🔍 Finding Product.dtsx operations...")
        
        # Get operations specifically from Product.dtsx
        query = """
        MATCH (n:Node) 
        WHERE n.node_type = 'operation' 
        AND (n.id CONTAINS 'Product.dtsx' OR toLower(toString(n.properties)) CONTAINS 'product.dtsx')
        RETURN n.id, n.name, n.properties
        """
        
        results = client._execute_query(query)
        print(f"Found {len(results)} operations from Product.dtsx")
        
        for result in results:
            node_id, name, properties = result
            print(f"\n🔧 Operation: {name}")
            print(f"   ID: {node_id}")
            
            if isinstance(properties, dict):
                # Look specifically for SQL-related content
                if 'column_lineage' in properties:
                    lineage = properties['column_lineage']
                    if isinstance(lineage, list):
                        print(f"   📋 Found {len(lineage)} components in column lineage")
                        
                        for component in lineage:
                            if isinstance(component, dict):
                                comp_name = component.get('component_name', 'Unknown')
                                print(f"     - Component: {comp_name}")
                                
                                # Check if this is the OLE DB Source component
                                if 'ole db source' in comp_name.lower() and 'product' in comp_name.lower():
                                    print(f"       🎯 This is the Product OLE DB Source!")
                                    
                                    # Check input/output columns for Categories references
                                    if 'output_columns' in component:
                                        output_cols = component['output_columns']
                                        category_cols = [col for col in output_cols if 'category' in col.get('column_name', '').lower()]
                                        if category_cols:
                                            print(f"       📊 Found Category columns:")
                                            for col in category_cols:
                                                print(f"         - {col.get('column_name', 'Unknown')}")
                
                # Check for any stored SQL content
                sql_keys = [k for k in properties.keys() if 'sql' in k.lower()]
                if sql_keys:
                    print(f"   📝 SQL-related properties: {sql_keys}")
                    
        print(f"\n" + "="*60)
        print("🔍 Let's check what table nodes exist and their connections...")
        
        # Get all table nodes
        table_query = """
        MATCH (n:Node) 
        WHERE n.node_type = 'table'
        RETURN n.id, n.name, n.properties
        ORDER BY n.name
        """
        
        table_results = client._execute_query(table_query)
        print(f"📋 Found {len(table_results)} table nodes:")
        
        for result in table_results:
            node_id, name, properties = result
            print(f"  - {name} (ID: {node_id})")
            
        print(f"\n" + "="*60)
        print("🔍 Let's check edges to see what tables operations are connected to...")
        
        # Get edges from Product operations to tables
        edge_query = """
        MATCH (op:Node)-[r]->(table:Node)
        WHERE op.node_type = 'operation' 
        AND op.id CONTAINS 'Product.dtsx'
        AND table.node_type = 'table'
        RETURN op.name, type(r), table.name
        """
        
        edge_results = client._execute_query(edge_query)
        print(f"🔗 Found {len(edge_results)} edges from Product operations to tables:")
        
        for result in edge_results:
            op_name, edge_type, table_name = result
            print(f"  {op_name} --{edge_type}--> {table_name}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()

if __name__ == "__main__":
    main()