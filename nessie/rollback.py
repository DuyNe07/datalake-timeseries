import requests
import json
import os
import logging
import sys

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("NessieRollback")

# Cấu hình Nessie
NESSIE_API = "http://nessie:19120/api/v1"
COMMIT_FILE = os.path.join(os.path.dirname(__file__), "last_commit.json")

def load_last_commit():
    """Đọc commit cuối cùng đã lưu"""
    try:
        if not os.path.exists(COMMIT_FILE):
            logger.error(f"Commit file not found: {COMMIT_FILE}")
            return None
            
        with open(COMMIT_FILE, "r") as f:
            commit_info = json.load(f)
        return commit_info.get("commit_hash")
    except Exception as e:
        logger.error(f"Error loading last commit: {e}")
        return None

def rollback_to_commit(commit_hash=None):
    """Rollback về commit cụ thể hoặc commit đã lưu cuối cùng"""
    if not commit_hash:
        commit_hash = load_last_commit()
        
    if not commit_hash:
        logger.error("No commit hash provided or found in saved file")
        return False
        
    url = f"{NESSIE_API}/trees/tree/main"
    headers = {"Content-Type": "application/json"}
    payload = {
        "type": "BRANCH",
        "name": "main",
        "hash": commit_hash
    }
    
    try:
        response = requests.put(url, headers=headers, json=payload)
        if response.status_code == 200:
            logger.info(f"Successfully rolled back to commit: {commit_hash}")
            return True
        else:
            logger.error(f"Failed to rollback: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error during rollback: {e}")
        return False

if __name__ == "__main__":
    # Cho phép chỉ định commit hash từ command line
    commit_hash = sys.argv[1] if len(sys.argv) > 1 else None
    rollback_to_commit(commit_hash)