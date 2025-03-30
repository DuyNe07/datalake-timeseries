"""Nessie commit graph visualization."""
import sys
import requests
from datetime import datetime
import argparse
import os
import json

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

class CommitGraphClient(NessieAPIClient):
    def get_all_branches(self):
        """Get all branches from Nessie"""
        response = requests.get(f"{self.base_url}/trees")
        if response.status_code == 200:
            data = response.json()
            print(f"API response structure for branches: {type(data)}")
            if isinstance(data, dict) and 'references' in data:
                print(f"Found {len(data['references'])} branches in 'references' key")
                return data['references']
            elif isinstance(data, list):
                print(f"Found {len(data)} branches in list response")
                return data
            else:
                print(f"Unexpected API response format for branches: {data}")
                print("Using default branch structure")
                return [{"name": "main"}]
        else:
            raise Exception(f"Error fetching branches: {response.status_code} - {response.text}")
    
    def get_branch_commits(self, branch="main", max_records=30):
        """Get commit history for a specific branch
        
        Args:
            branch: Branch name to get history for
            max_records: Maximum number of commits to return
            
        Returns:
            List of commit objects
        """
        # Try different API endpoints based on Nessie version
        endpoints = [
            f"{self.base_url}/trees/{branch}/log",  # Standard endpoint
            f"{self.base_url}/trees/{branch}/history",  # Alternative endpoint
            f"{self.base_url}/trees/branch/{branch}/log",  # Another alternative
            f"{self.base_url}/trees/branch/{branch}/history"  # Yet another alternative
        ]
        
        for endpoint in endpoints:
            print(f"Trying to fetch commits from: {endpoint}")
            try:
                response = requests.get(
                    endpoint,
                    params={"max_records": max_records}
                )
                
                if response.status_code == 200:
                    data = response.json()
                    print(f"API response structure for commits: {type(data)}")
                    
                    # Handle different response formats
                    if isinstance(data, list):
                        print(f"Found {len(data)} commits in list")
                        return data
                    elif isinstance(data, dict) and 'entries' in data:
                        print(f"Found {len(data['entries'])} commits in 'entries' key")
                        return data['entries']
                    elif isinstance(data, dict) and 'commits' in data:
                        print(f"Found {len(data['commits'])} commits in 'commits' key")
                        return data['commits']
                    else:
                        print(f"Unexpected format but got 200 response: {data}")
                        # If empty, return empty list
                        return []
                else:
                    print(f"Endpoint {endpoint} returned status {response.status_code}")
            except Exception as e:
                print(f"Error trying {endpoint}: {str(e)}")
                continue
        
        # If we reach here, none of the endpoints worked
        print("WARN: Could not fetch commits from any endpoint. Using branch info instead.")
        
        # Check if branch has a hash property and create dummy commit
        try:
            # Try to get branch directly
            branch_response = requests.get(f"{self.base_url}/trees/{branch}")
            if branch_response.status_code == 200:
                branch_data = branch_response.json()
                if isinstance(branch_data, dict) and 'hash' in branch_data:
                    # Create a dummy commit using branch hash
                    return [{
                        "hash": branch_data["hash"],
                        "message": "Current HEAD",
                        "author": "Unknown",
                        "timestamp": int(datetime.now().timestamp() * 1000)
                    }]
        except Exception as e:
            print(f"Error getting branch info: {str(e)}")
            
        # Last resort: check if any branches have a hash property
        branches = self.get_all_branches()
        for b in branches:
            if isinstance(b, dict) and b.get("name") == branch and 'hash' in b:
                # Create a dummy commit using branch hash
                return [{
                    "hash": b["hash"],
                    "message": "Current HEAD",
                    "author": "Unknown",
                    "timestamp": int(datetime.now().timestamp() * 1000)
                }]
        
        print("ERROR: Could not retrieve any commit information")
        return []
    
    def build_commit_graph_data(self, max_commits=30):
        """Build data structure for commit graph
        
        Args:
            max_commits: Maximum number of commits to include
            
        Returns:
            Dictionary with commits and branch information
        """
        # Get all branches
        branches = self.get_all_branches()
        
        # Initialize graph data
        graph_data = {
            "branches": {},
            "commits": {},
            "branch_heads": {}
        }
        
        # Process each branch
        for branch in branches:
            # Extract branch name
            if isinstance(branch, dict) and "name" in branch:
                branch_name = branch["name"]
                # Store hash if branch object has it
                if "hash" in branch:
                    graph_data["branch_heads"][branch_name] = branch["hash"]
            elif isinstance(branch, str):
                branch_name = branch
            else:
                print(f"WARN: Skipping branch with unexpected format: {branch}")
                continue
                
            print(f"Processing branch: {branch_name}")
            
            # Get commits for this branch
            commits = self.get_branch_commits(branch_name, max_commits)
            
            # Process commits for this branch
            graph_data["branches"][branch_name] = []
            for commit in commits:
                # Extract commit hash
                if isinstance(commit, dict) and "hash" in commit:
                    commit_hash = commit["hash"]
                elif isinstance(commit, str):
                    commit_hash = commit
                else:
                    print(f"WARN: Skipping commit with unexpected format: {commit}")
                    continue
                    
                graph_data["branches"][branch_name].append(commit_hash)
                
                # Set branch head if not already set
                if branch_name not in graph_data["branch_heads"] and len(graph_data["branches"][branch_name]) == 1:
                    graph_data["branch_heads"][branch_name] = commit_hash
                
                # Store commit details if not already stored
                if commit_hash not in graph_data["commits"]:
                    if isinstance(commit, dict):
                        graph_data["commits"][commit_hash] = {
                            "message": commit.get("message", "No message"),
                            "author": commit.get("author", "Unknown"),
                            "timestamp": commit.get("timestamp", 0),
                            "parents": commit.get("parentHash", [])
                        }
                    else:
                        # Minimal data if commit is just a hash
                        graph_data["commits"][commit_hash] = {
                            "message": "No message available",
                            "author": "Unknown",
                            "timestamp": 0,
                            "parents": []
                        }
        
        # Print summary
        print(f"Built graph data for {len(graph_data['branches'])} branches and {len(graph_data['commits'])} commits")
        return graph_data

def format_time(timestamp):
    """Format timestamp for display"""
    try:
        dt = datetime.fromtimestamp(timestamp / 1000)
        return dt.strftime("%Y-%m-%d %H:%M")
    except:
        return "Unknown"

def display_commit_graph(graph_data, show_details=True):
    """Display ASCII representation of commit graph
    
    Args:
        graph_data: Graph data from build_commit_graph_data
        show_details: Whether to show commit details
    """
    # Create a timeline of all commits
    all_commits = list(graph_data["commits"].keys())
    all_commits.sort(key=lambda c: graph_data["commits"][c]["timestamp"], reverse=True)
    
    # Branch display information
    branch_names = list(graph_data["branches"].keys())
    branch_colors = ["*", "+", "o", "x", "#", "@"]  # Symbols for different branches
    
    # Display header
    print("\n" + "=" * 80)
    print("NESSIE COMMIT GRAPH")
    print("=" * 80)
    
    # Show branch legend
    print("\nBranches:")
    for i, branch in enumerate(branch_names):
        symbol = branch_colors[i % len(branch_colors)]
        head = graph_data["branch_heads"].get(branch, "")
        print(f"  {symbol} {branch:<20} HEAD: {head[:8]}")
    
    print("\n" + "-" * 80)
    
    # Display the commit graph
    for commit_hash in all_commits:
        commit = graph_data["commits"][commit_hash]
        
        # Find which branches contain this commit
        commit_branches = []
        for i, branch in enumerate(branch_names):
            if commit_hash in graph_data["branches"][branch]:
                commit_branches.append((branch, branch_colors[i % len(branch_colors)]))
        
        # Prepare branch indicators
        branch_indicators = " ".join([symbol for _, symbol in commit_branches])
        
        # Display commit with its branches
        print(f"{commit_hash[:8]} [{branch_indicators}] {format_time(commit['timestamp'])}")
        
        if show_details:
            print(f"  Author: {commit['author']}")
            print(f"  Message: {commit['message']}")
            if commit.get('parents'):
                print(f"  Parents: {', '.join([p[:8] for p in commit['parents']])}")
            print()
    
    print("-" * 80)

def show_graph(base_url="http://nessie:19120/api/v1", max_commits=20, simple=False, debug=False):
    """Function to easily display commit graph from any script
    
    Args:
        base_url: Nessie API URL
        max_commits: Maximum number of commits to display
        simple: Whether to show simplified view
        debug: Enable debugging output
        
    Returns:
        0 on success, 1 on error
    """
    try:
        if debug:
            print(f"Connecting to Nessie API at: {base_url}")
            
        client = CommitGraphClient(base_url=base_url)
        
        if debug:
            print("Building commit graph data...")
            
        graph_data = client.build_commit_graph_data(max_commits=max_commits)
        
        if debug:
            print("Graph data built successfully")
            
        if graph_data["commits"]:
            display_commit_graph(graph_data, show_details=not simple)
            return 0
        else:
            print("No commits found to display")
            return 1
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1

def main():
    """Main function to run when executed directly"""
    parser = argparse.ArgumentParser(description="Visualize Nessie commit graph")
    parser.add_argument("--url", default="http://nessie:19120/api/v1", 
                        help="Nessie API URL")
    parser.add_argument("--max-commits", type=int, default=20,
                        help="Maximum number of commits to display")
    parser.add_argument("--simple", action="store_true",
                        help="Show simplified view without details")
    parser.add_argument("--debug", action="store_true",
                        help="Print debug information")
    
    args = parser.parse_args()
    
    if args.debug:
        print(f"Debug mode enabled")
        print(f"Using Nessie API URL: {args.url}")
    
    return show_graph(
        base_url=args.url, 
        max_commits=args.max_commits, 
        simple=args.simple,
        debug=args.debug
    )

if __name__ == "__main__":
    sys.exit(main())
