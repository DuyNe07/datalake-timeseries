"""Nessie commit operations."""
import requests
import json
import os
import sys

# Handle imports for both direct execution and module import
try:
    # When imported as a module
    from .api_client import NessieAPIClient
except ImportError:
    # When run directly
    module_dir = os.path.abspath(os.path.dirname(__file__))
    if module_dir not in sys.path:
        sys.path.insert(0, module_dir)
    from api_client import NessieAPIClient

class CommitClient(NessieAPIClient):
    def create_commit(self, branch="main", message="Data update", operations=None):
        """Create a new commit on a branch
        
        Args:
            branch: Branch to commit to
            message: Commit message
            operations: List of operations to include in commit
            
        Returns:
            Commit hash if successful
        """
        if operations is None:
            operations = []
            
        headers = {"Content-Type": "application/json"}
        payload = {
            "operations": operations,
            "message": message
        }
        
        response = requests.post(
            f"{self.base_url}/trees/{branch}/commit",
            headers=headers,
            data=json.dumps(payload)
        )
        
        if response.status_code in [200, 201]:
            return response.json().get("hash")
        else:
            raise Exception(f"Error creating commit: {response.text}")
    
    def create_branch(self, new_branch, source_branch="main"):
        """Create a new branch from source branch
        
        Args:
            new_branch: Name of branch to create
            source_branch: Source branch to branch from
            
        Returns:
            True if successful
        """
        headers = {"Content-Type": "application/json"}
        payload = {
            "sourceRefName": source_branch,
            "type": "BRANCH"
        }
        
        response = requests.post(
            f"{self.base_url}/trees/{new_branch}",
            headers=headers,
            data=json.dumps(payload)
        )
        
        if response.status_code in [200, 201]:
            return True
        else:
            raise Exception(f"Error creating branch: {response.text}")

def quick_commit(branch="main", message="Data update", operations=None):
    """Utility function to quickly create a commit"""
    client = CommitClient()
    return client.create_commit(branch, message, operations)
