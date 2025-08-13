#!/usr/bin/env python3
"""Query Memgraph to check for Categories table references"""

import os
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
        # Client automatically connects on initialization
        print("✅ Connected to Memgraph")
        
        # Query 1: Check for any node containing "Categories" in name
        print("\n🔍 Searching for nodes with 'Categories' in name...")
        query1 = """
        MATCH (n:Node) 
        WHERE toLower(n.name) CONTAINS 'categories' 
        RETURN n.id, n.name, n.node_type, n.properties
        """
        
        results1 = client._execute_query(query1)
        if results1:
            print(f"Found {len(results1)} nodes containing 'Categories' in name:")
            for result in results1:
                print(f"  - ID: {result[0]}, Name: {result[1]}, Type: {result[2]}")
        else:
            print("❌ No nodes found with 'Categories' in name")
            
        # Query 2: Check for any node containing "Categories" anywhere in properties
        print("\n🔍 Searching for nodes mentioning 'Categories' in properties...")
        query2 = """
        MATCH (n:Node) 
        WHERE toLower(toString(n.properties)) CONTAINS 'categories'
        RETURN n.id, n.name, n.node_type, n.properties
        LIMIT 10
        """
        
        results2 = client._execute_query(query2)
        if results2:
            print(f"Found {len(results2)} nodes mentioning 'Categories' in properties:")
            for result in results2:
                print(f"  - ID: {result[0]}, Name: {result[1]}, Type: {result[2]}")
                print(f"    Properties: {result[3]}")
        else:
            print("❌ No nodes found mentioning 'Categories' in properties")
            
        # Query 3: Check all data asset nodes
        print("\n🔍 Listing all DATA_ASSET nodes...")
        query3 = """
        MATCH (n:Node) 
        WHERE n.node_type = 'data_asset'
        RETURN n.id, n.name, n.properties
        """
        
        results3 = client._execute_query(query3)
        if results3:
            print(f"Found {len(results3)} DATA_ASSET nodes:")
            for result in results3:
                print(f"  - ID: {result[0]}, Name: {result[1]}")
        else:
            print("❌ No DATA_ASSET nodes found")
            
        # Query 4: Check for SQL commands containing Categories
        print("\n🔍 Searching for SQL commands mentioning 'Categories'...")
        query4 = """
        MATCH (n:Node) 
        WHERE n.node_type = 'operation' 
        AND toLower(toString(n.properties)) CONTAINS 'categories'
        RETURN n.id, n.name, n.properties
        """
        
        results4 = client._execute_query(query4)
        if results4:
            print(f"Found {len(results4)} operations mentioning 'Categories':")
            for result in results4:
                print(f"  - ID: {result[0]}, Name: {result[1]}")
                props = result[2]
                if 'sql_command' in str(props):
                    print(f"    SQL Command found in properties")
        else:
            print("❌ No operations found mentioning 'Categories'")
            
        # Query 5: Get all nodes to understand what was actually extracted
        print("\n📊 Summary of all nodes by type...")
        query5 = """
        MATCH (n:Node) 
        RETURN n.node_type, count(*) as count
        ORDER BY count DESC
        """
        
        results5 = client._execute_query(query5)
        if results5:
            print("Node type distribution:")
            for result in results5:
                print(f"  - {result[0]}: {result[1]} nodes")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        client.close()
        print("\n🔌 Disconnected from Memgraph")

if __name__ == "__main__":
    main()