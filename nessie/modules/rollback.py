"""Nessie rollback operations."""
import requests
import json
import os
import sys

# Handle imports for both direct execution and module import
try:
    # When imported as a module
    from .api_client import NessieAPIClient
    from .show_commit_graph import CommitGraphClient
except ImportError:
    # When run directly
    module_dir = os.path.abspath(os.path.dirname(__file__))
    if module_dir not in sys.path:
        sys.path.insert(0, module_dir)
    from api_client import NessieAPIClient
    from show_commit_graph import CommitGraphClient

class RollbackClient(NessieAPIClient):
    def rollback_to_commit(self, branch="main", commit_hash=None):
        """Roll back a branch to a specific commit
        
        Args:
            branch: Branch to rollback
            commit_hash: Target commit hash to rollback to
            
        Returns:
            True if successful
        """
        if not commit_hash:
            raise ValueError("commit_hash must be provided")
            
        headers = {"Content-Type": "application/json"}
        payload = {
            "hash": commit_hash
        }
        
        response = requests.put(
            f"{self.base_url}/trees/{branch}",
            headers=headers,
            data=json.dumps(payload)
        )
        
        if response.status_code == 200:
            return True
        else:
            raise Exception(f"Error rolling back: {response.text}")
    
    def get_rollback_options(self, branch="main", limit=10):
        """Get available commits for rollback
        
        Args:
            branch: Branch name
            limit: Maximum number of commits to return
            
        Returns:
            List of commits available for rollback
        """
        graph_client = CommitGraphClient(base_url=self.base_url)
        return graph_client.get_branch_commits(branch, max_records=limit)

def perform_rollback(branch="main", commit_hash=None):
    """Utility function to perform a rollback
    
    Args:
        branch: Branch to rollback
        commit_hash: Target commit hash
        
    Returns:
        True if successful
    """
    client = RollbackClient()
    return client.rollback_to_commit(branch, commit_hash)
