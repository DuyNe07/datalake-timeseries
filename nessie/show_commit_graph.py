import requests
import json
import datetime
import logging
import sys
import argparse
from typing import List, Dict, Any

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("NessieCommitGraph")

# Cấu hình Nessie
NESSIE_API = "http://nessie:19120/api/v1"

def get_commit_log(limit: int = 50) -> List[Dict[str, Any]]:
    """Lấy lịch sử commit từ Nessie"""
    url = f"{NESSIE_API}/trees/tree/main/log?limit={limit}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Failed to get commit log: {response.status_code} - {response.text}")
            return []
    except Exception as e:
        logger.error(f"Error getting commit log: {e}")
        return []

def get_branches() -> List[Dict[str, Any]]:
    """Lấy danh sách các branch từ Nessie"""
    url = f"{NESSIE_API}/trees"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Failed to get branches: {response.status_code} - {response.text}")
            return []
    except Exception as e:
        logger.error(f"Error getting branches: {e}")
        return []

def get_commit_details(commit_hash: str) -> Dict[str, Any]:
    """Lấy chi tiết về một commit cụ thể"""
    url = f"{NESSIE_API}/trees/tree/main/log/{commit_hash}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Failed to get commit details: {response.status_code} - {response.text}")
            return {}
    except Exception as e:
        logger.error(f"Error getting commit details: {e}")
        return {}

def get_commit_diff(hash1: str, hash2: str = None) -> Dict[str, Any]:
    """Lấy sự khác biệt giữa hai commit"""
    if not hash2:
        # Nếu chỉ có một hash, so sánh với parent
        hash2 = f"{hash1}~1"
    
    url = f"{NESSIE_API}/trees/diff/main/{hash1}...main/{hash2}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Failed to get commit diff: {response.status_code} - {response.text}")
            return {}
    except Exception as e:
        logger.error(f"Error getting commit diff: {e}")
        return {}

def display_commit_log(commits: List[Dict[str, Any]], show_details: bool = False):
    """Hiển thị commit log dưới dạng đồ thị"""
    if not commits:
        print("No commits found")
        return
    
    print("\n" + "="*80)
    print("NESSIE COMMIT GRAPH")
    print("="*80)
    
    for i, commit in enumerate(commits):
        commit_hash = commit.get("commitMeta", {}).get("hash", "Unknown")
        message = commit.get("commitMeta", {}).get("message", "No message")
        author = commit.get("commitMeta", {}).get("author", "Unknown")
        commit_time = commit.get("commitMeta", {}).get("commitTime", 0)
        
        # Convert commit_time từ milliseconds thành datetime
        commit_date = datetime.datetime.fromtimestamp(commit_time/1000).strftime('%Y-%m-%d %H:%M:%S')
        
        # Tạo biểu đồ đơn giản
        graph_char = "│" if i < len(commits) - 1 else " "
        commit_char = "●"
        
        print(f"{graph_char}")
        print(f"{commit_char} Commit: {commit_hash}")
        print(f"│ Author: {author}")
        print(f"│ Date: {commit_date}")
        print(f"│ Message: {message}")
        
        if show_details:
            details = get_commit_details(commit_hash)
            print(f"│ Operations: {len(details.get('operations', []))}")
            
            # Hiển thị chi tiết các thay đổi nếu có
            for op in details.get("operations", [])[:5]:  # Giới hạn hiển thị 5 operations
                print(f"│   - {op.get('type', 'Unknown')} on {op.get('key', {}).get('elements', ['Unknown'])[0]}")
            
            if len(details.get("operations", [])) > 5:
                print(f"│   ... and {len(details.get('operations', [])) - 5} more operations")
            
            if i > 0:
                prev_commit = commits[i-1].get("commitMeta", {}).get("hash", "Unknown")
                diff = get_commit_diff(commit_hash, prev_commit)
                print(f"│ Changes: {len(diff.get('entries', []))}")
        
        print(f"│")
    
    print("="*80)

def display_branches(branches: List[Dict[str, Any]]):
    """Hiển thị danh sách các branch"""
    if not branches:
        print("No branches found")
        return
    
    print("\n" + "="*80)
    print("NESSIE BRANCHES")
    print("="*80)
    
    for branch in branches:
        name = branch.get("name", "Unknown")
        branch_type = branch.get("type", "Unknown")
        hash_val = branch.get("hash", "Unknown")
        
        print(f"Branch: {name}")
        print(f"Type: {branch_type}")
        print(f"Current Hash: {hash_val}")
        print("-"*80)
    
    print("="*80)

def main():
    parser = argparse.ArgumentParser(description="Display Nessie commit graph and branch information")
    parser.add_argument("-l", "--limit", type=int, default=20, help="Limit number of commits to display")
    parser.add_argument("-d", "--details", action="store_true", help="Show detailed commit information")
    parser.add_argument("-b", "--branches", action="store_true", help="Show branches information")
    
    args = parser.parse_args()
    
    if args.branches:
        branches = get_branches()
        display_branches(branches)
    
    commits = get_commit_log(args.limit)
    display_commit_log(commits, args.details)

if __name__ == "__main__":
    main()