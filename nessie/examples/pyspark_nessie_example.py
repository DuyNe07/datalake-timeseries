"""Example PySpark script demonstrating Nessie integration"""
import os
import sys

# Add Nessie modules to path if not already there
nessie_modules_path = os.getenv("NESSIE_MODULES_PATH", "/nessie/modules")
if os.path.exists(nessie_modules_path) and nessie_modules_path not in sys.path:
    sys.path.insert(0, nessie_modules_path)

# Import the modules - with error handling to demonstrate all possible import paths
try:
    # Import as standard module if installed
    from nessie.modules.commit import quick_commit
    from nessie.modules.rollback import perform_rollback
    from nessie.modules.show_commit_graph import show_graph
except ImportError:
    try:
        # Try direct import
        from commit import quick_commit
        from rollback import perform_rollback
        from show_commit_graph import show_graph
    except ImportError:
        print("Could not import Nessie modules - check your Python path")
        sys.exit(1)

# Example function showing how to use Nessie in a PySpark job
def run_etl_with_nessie():
    """Example ETL function with Nessie integration"""
    print("Starting ETL process with Nessie versioning...")
    
    # You would typically put your PySpark code here
    # from pyspark.sql import SparkSession
    # spark = SparkSession.builder.appName("NessieExample").getOrCreate()
    
    try:
        # Your ETL logic would go here
        print("Simulating ETL process...")
        
        # Commit changes after successful ETL
        print("ETL completed successfully, creating commit...")
        commit_hash = quick_commit(message="ETL job completed successfully")
        print(f"Changes committed with hash: {commit_hash}")
        
        # Show the commit graph
        print("\nCurrent commit graph:")
        show_graph(max_commits=5, simple=True)
        
    except Exception as e:
        print(f"ETL error: {str(e)}")
        print("Rolling back to previous commit...")
        
        # This is just a demonstration - you'd need logic to determine
        # the actual commit to roll back to
        # perform_rollback(commit_hash="previous_commit_hash")
        
        return 1
        
    return 0

# Main function to execute the example
if __name__ == "__main__":
    sys.exit(run_etl_with_nessie())
