#!/bin/bash
# File: nessie/bin/nessie-tools.sh

# Hàm hiển thị usage
show_usage() {
  echo "Nessie Tools - Các công cụ quản lý Nessie"
  echo ""
  echo "Usage:"
  echo "  $0 verify             Kiểm tra môi trường Nessie"
  echo "  $0 commit MESSAGE     Tạo commit mới với message"
  echo "  $0 save-commit        Lưu thông tin commit hiện tại"
  echo "  $0 rollback [HASH]    Rollback về commit trước hoặc commit có hash cụ thể"
  echo "  $0 graph              Hiển thị đồ thị commit"
  echo ""
}

# Kiểm tra môi trường Python
check_python() {
  if ! command -v python &> /dev/null; then
    echo "Python không được cài đặt hoặc không có trong PATH"
    exit 1
  fi
}

# Đường dẫn tới thư mục nessie
NESSIE_DIR="/nessie"

# Đảm bảo PYTHONPATH chứa thư mục nessie
if [[ ":$PYTHONPATH:" != *":/nessie:"* ]]; then
  export PYTHONPATH="/nessie:$PYTHONPATH"
  echo "Added /nessie to PYTHONPATH"
fi

# Xử lý tham số
if [ $# -lt 1 ]; then
  show_usage
  exit 1
fi

check_python

case "$1" in
  verify)
    python -m nessie.modules.verify_nessie
    ;;
  commit)
    if [ -z "$2" ]; then
      echo "ERROR: Thiếu message cho commit"
      exit 1
    fi
    python -c "from nessie.modules.current_commit import create_commit; print(create_commit('$2'))"
    ;;
  save-commit)
    python -c "from nessie.modules.current_commit import save_commit_info; print(save_commit_info('Manual save'))"
    ;;
  rollback)
    HASH="$2"
    if [ -z "$HASH" ]; then
      python -c "from nessie.modules.rollback import rollback_to_commit; print('Rollback successful' if rollback_to_commit() else 'Rollback failed')"
    else
      python -c "from nessie.modules.rollback import rollback_to_commit; print('Rollback successful' if rollback_to_commit('$HASH') else 'Rollback failed')"
    fi
    ;;
  graph)
    python -m nessie.modules.show_commit_graph
    ;;
  *)
    echo "Unknown command: $1"
    show_usage
    exit 1
    ;;
esac