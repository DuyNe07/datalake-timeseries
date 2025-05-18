# Hướng dẫn kết nối Metabase với Cube

## Tổng quan

Metabase và Cube.js là hai công cụ phân tích và hiển thị dữ liệu mạnh mẽ trong hệ thống của bạn. Cấu trúc kết nối giữa chúng được thiết lập như sau:

1. **Metabase**: UI cho việc tạo dashboard và phân tích dữ liệu
2. **Cube.js**: Semantic layer định nghĩa các model dữ liệu
3. **Bridge Service**: Service trung gian giúp Metabase và Cube.js giao tiếp
4. **Trino**: Query engine truy vấn dữ liệu từ MinIO

## Hướng dẫn cấu hình

### Cách 1: Kết nối Metabase trực tiếp với Trino (Đã cấu hình tự động)

Metabase đã được tự động cấu hình để kết nối với Trino, cho phép truy cập trực tiếp đến dữ liệu trong schema `gold` của catalog `datalake`.

### Cách 2: Kết nối Metabase với Cube.js thông qua Bridge Service

Để tận dụng các model dữ liệu đã định nghĩa trong Cube.js, bạn cần làm các bước sau:

1. Đăng nhập vào Metabase tại http://localhost:3030
   - Email: admin@example.com
   - Mật khẩu: metabase123

2. Vào trang Admin > Databases > Add database

3. Chọn "URL" làm engine (hoặc "Web Service" nếu có)

4. Điền các thông tin kết nối:
   - Tên: Cube.js Models
   - URL: http://cube-metabase-bridge:3500
   - Không cần API key cho bridge service

5. Sau khi kết nối, bạn có thể tạo các câu truy vấn sử dụng API endpoint sau:
   - `/api/meta` - Lấy metadata của tất cả các cube
   - `/api/cubes/{cube_name}` - Lấy thông tin về một cube cụ thể
   - `/api/query` - Gửi truy vấn tới Cube.js

## Sử dụng API từ Bridge Service

### Lấy danh sách tất cả cube và dimensions

```
GET http://localhost:3500/api/meta
```

### Lấy thông tin về một cube cụ thể (ví dụ: indices_predict)

```
GET http://localhost:3500/api/cubes/indices_predict
```

### Thực hiện truy vấn

```
POST http://localhost:3500/api/query
Content-Type: application/json

{
  "measures": ["indices_predict.count"],
  "dimensions": ["indices_predict.gold"],
  "timeDimensions": []
}
```

## Tạo Dashboard trong Metabase

1. Tạo một New Question trong Metabase
2. Chọn "Native Query" để viết SQL trực tiếp với Trino
3. Sử dụng cú pháp SQL để truy vấn các bảng trong schema `gold`
4. Tham khảo các dimensions từ Cube.js model thông qua API bridge

## Khắc phục sự cố

Nếu bạn gặp vấn đề với kết nối:

1. Kiểm tra logs của các service:
   ```
   docker logs metabase
   docker logs cube
   docker logs cube-metabase-bridge
   ```

2. Đảm bảo tất cả các service đều đang chạy:
   ```
   docker ps | grep -E 'metabase|cube'
   ```

3. Kiểm tra kết nối API của Cube.js:
   ```
   curl http://localhost:4000/cubejs-api/v1/meta
   ```

4. Kiểm tra bridge service:
   ```
   curl http://localhost:3500/
   ```
