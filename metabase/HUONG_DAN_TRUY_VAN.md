# Hướng dẫn truy vấn dữ liệu từ Metabase

## Truy vấn dữ liệu thông qua Trino

### Ví dụ 1: Truy vấn dữ liệu từ bảng indices_predict

```sql
SELECT 
  russell2000, dow_jones, msci_world, nasdaq100, s_p500, gold
FROM
  gold.indices_predict
LIMIT 100
```

### Ví dụ 2: Tính giá trị trung bình của các chỉ số tài chính

```sql
SELECT
  AVG(russell2000) AS avg_russell2000,
  AVG(dow_jones) AS avg_dow_jones,
  AVG(msci_world) AS avg_msci_world,
  AVG(nasdaq100) AS avg_nasdaq100,
  AVG(s_p500) AS avg_sp500,
  AVG(gold) AS avg_gold
FROM
  gold.indices_predict
```

## Sử dụng Metabase để kết nối với Cube.js

### 1. Lấy danh sách các dimensions từ Cube.js

Bạn có thể sử dụng REST API để lấy thông tin về các dimensions và measures từ Cube.js:

```bash
curl http://localhost:3500/api/meta | jq .
```

### 2. Tạo câu truy vấn sử dụng các dimensions từ Cube.js trong Metabase

Sau khi biết các dimensions có trong model Cube.js, bạn có thể tạo câu truy vấn SQL trong Metabase:

```sql
/* Dimensions từ Cube.js model: russell2000, dow_jones, msci_world, nasdaq100, s_p500, gold */
SELECT
  date_trunc('month', date_column) AS month,
  AVG(russell2000) AS avg_russell_2000,
  AVG(s_p500) AS avg_sp_500
FROM
  gold.indices_predict
GROUP BY 1
ORDER BY 1 DESC
```

### 3. Tạo biểu đồ so sánh các chỉ số

Sau khi thực hiện các câu truy vấn, bạn có thể:

1. Chọn loại biểu đồ phù hợp (Line, Bar, Scatter, etc.)
2. Đặt X-axis là dimension thời gian (nếu có)
3. Đặt Y-axis là các measures (như giá trị trung bình của các chỉ số)
4. Lưu biểu đồ vào dashboard

## Cách tạo dashboard hiệu quả

1. Tạo New Dashboard trong Metabase
2. Thêm các câu truy vấn (questions) đã lưu vào dashboard
3. Sắp xếp và thay đổi kích thước các biểu đồ
4. Thêm các filter để lọc dữ liệu theo yêu cầu

## Tips:

- Sử dụng SQL Snippets trong Metabase để tái sử dụng các câu truy vấn phức tạp
- Tạo các parameterized queries để người dùng có thể tương tác với dashboard
- Tham khảo cấu trúc model trong Cube.js để biết các relations giữa các bảng dữ liệu
- Xem trước kết quả truy vấn trước khi lưu để đảm bảo hiệu suất tốt
