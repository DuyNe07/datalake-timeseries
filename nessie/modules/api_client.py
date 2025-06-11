"""Base Nessie API client."""
import requests

class NessieAPIClient:
    def __init__(self, base_url="http://nessie:19120/api/v1"):
        self.base_url = base_url
    
    def get_branches(self):
        """Get all branches from Nessie"""
        response = requests.get(f"{self.base_url}/trees")
        if response.status_code == 200:
            return response.json()
        else:
            return None
