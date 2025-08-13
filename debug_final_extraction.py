#!/usr/bin/env python3
"""Debug the final table extraction with actual Product.dtsx SQL"""

import sys
sys.path.append('/mnt/c/Users/Hicham/OneDrive/python/projects/mzcode')

from metazcode.sdk.ingestion.ssis.ssis_parser import CanonicalSsisParser

def test_direct_extraction():
    """Test the extraction function directly"""
    
    # Create parser instance
    parser = CanonicalSsisParser()
    
    # The exact SQL from Product.dtsx
    product_sql = """SELECT        p.ProductID, p.ProductName, p.SupplierID, p.CategoryID, p.QuantityPerUnit, p.UnitPrice, p.UnitsInStock, p.UnitsOnOrder, p.ReorderLevel, p.Discontinued, c.CategoryID AS Expr1, c.CategoryName, c.Description, c.Picture
FROM            Products AS p INNER JOIN
                         Categories AS c ON p.CategoryID = c.CategoryID"""
    
    print("🔍 Testing direct table extraction from Product.dtsx SQL")
    print("=" * 70)
    print(f"SQL: {' '.join(product_sql.split())}")
    print()
    
    # Test the extraction function
    extracted_tables = parser._extract_tables_from_sql(product_sql)
    
    print(f"Extracted tables: {extracted_tables}")
    print()
    
    # Expected result
    expected = ["Categories", "Products"]
    success = set(extracted_tables) == set(expected)
    
    print(f"Expected: {expected}")
    print(f"Success: {'✅ YES' if success else '❌ NO'}")
    
    if not success:
        print(f"Missing: {set(expected) - set(extracted_tables)}")
        print(f"Extra: {set(extracted_tables) - set(expected)}")
    
    return success

if __name__ == "__main__":
    test_direct_extraction()