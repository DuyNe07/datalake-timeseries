import requests
import json
import os
import datetime
import logging

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("NessieCommit")

# Cấu hình Nessie
NESSIE_API = "http://nessie:19120/api/v1"
COMMIT_FILE = os.path.join(os.path.dirname(__file__), "last_commit.json")

def get_current_head():
    """Lấy commit hiện tại của branch main"""
    url = f"{NESSIE_API}/trees/tree/main"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return data.get("hash")
        else:
            logger.error(f"Failed to get head: {response.text}")
            return None
    except Exception as e:
        logger.error(f"Error getting current head: {e}")
        return None

def save_commit_info(message="Auto saved commit"):
    """Lưu thông tin commit hiện tại và trả về commit hash"""
    commit_hash = get_current_head()
    if not commit_hash:
        logger.error("Could not get current commit hash")
        return None
    
    commit_info = {
        "commit_hash": commit_hash,
        "timestamp": str(datetime.datetime.now()),
        "message": message,
        "author": "duy-ne"  # Lấy từ thông tin người dùng
    }
    
    try:
        with open(COMMIT_FILE, "w") as f:
            json.dump(commit_info, f, indent=2)
        logger.info(f"Saved commit info: {commit_hash}")
        return commit_hash
    except Exception as e:
        logger.error(f"Error saving commit info: {e}")
        return None

def create_commit(message):
    """Tạo commit mới với message"""
    url = f"{NESSIE_API}/trees/tree/main/commit"
    headers = {"Content-Type": "application/json"}
    payload = {
        "message": message, 
        "operations": [],
        "author": "duy-ne"  # Thêm tên người dùng vào commit
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            logger.info(f"Created commit: {message}")
            # Không lưu commit ở đây để tránh override
            commit_hash = get_current_head()
            return commit_hash
        else:
            logger.error(f"Failed to create commit: {response.text}")
            return None
    except Exception as e:
        logger.error(f"Error creating commit: {e}")
        return None

if __name__ == "__main__":
    import datetime
    message = f"Test commit {datetime.datetime.now()}"
    create_commit(message)