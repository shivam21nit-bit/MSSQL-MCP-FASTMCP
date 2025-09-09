# test_logic.py
# This script is for directly testing the functions in app.py without
# running the web server. This helps isolate issues with the core logic
# from issues with the network or server itself.

import logging
from app import (
    load_database_schema_cache,
    get_table_schema,
    get_object_definition,
    get_job_status,
    get_column_population_logic,
    get_column_data
)

# --- Basic Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_tests():
    """Runs a series of tests against the core application logic."""
    print("--- Starting Direct Logic Test ---")

    # Step 1: Load the schema cache, just like the server does on startup.
    print("\n[TEST 1] Loading database schema cache...")
    try:
        load_database_schema_cache()
        print("✅ Cache loaded successfully.")
    except Exception as e:
        print(f"❌ FAILED to load cache: {e}")
        return # Stop if cache fails to load

    # Step 2: Test fetching a table schema.
    # --- IMPORTANT: Change 'employees' to a real table name in your database ---
    print("\n[TEST 2] Fetching schema for table 'employees'...")
    try:
        schema_result = get_table_schema("what is the schema for employees")
        print("✅ SUCCESS! Result:")
        print(schema_result)
    except Exception as e:
        print(f"❌ FAILED to get table schema: {e}")

    # Step 3: Test fetching data from a table.
    # --- IMPORTANT: Change the table, columns, and value to match your data ---
    print("\n[TEST 3] Fetching data: 'data of FirstName from employees where EmployeeID is 1'...")
    try:
        data_result = get_column_data("data of FirstName from employees where EmployeeID is 1")
        print("✅ SUCCESS! Result:")
        print(data_result)
    except Exception as e:
        print(f"❌ FAILED to get data: {e}")

    # Step 4: Test fetching column population logic.
    # --- IMPORTANT: Change 'EmployeeID' to a real column name ---
    print("\n[TEST 4] Fetching population logic for column 'EmployeeID'...")
    try:
        logic_result = get_column_population_logic("how is column EmployeeID populated")
        print("✅ SUCCESS! Result:")
        print(logic_result)
    except Exception as e:
        print(f"❌ FAILED to get population logic: {e}")


    print("\n--- Test complete. Check the output above for results. ---")

if __name__ == '__main__':
    run_tests()
