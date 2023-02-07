# Requirement:
- Có Docker, Docker Compose, Docker Desktop
- Tải MongoDB Compass về để xem data trong MongoDB

# Chạy Demo (local):
- Chạy các thành phần với Docker: `docker compose up`

1. Demo crawl data và lưu vào MongoDB:
- Chạy Docker và đợi cho các container chạy hết
- Chạy data_ingestion.py
- Chạy crawler:
    - `cd dataCrawler`
    - `scrapy crawl <league/club/player>`
- (Xem kết quả) Vào MongoDB Compass kết nối với URI: `mongodb://localhost:27017`

2. Load dữ liệu từ MongoDB, xử lý bằng Spark, lưu vào Postgres (Phần này đang test, về sau sẽ không dùng notebook mà chạy python trực tiếp):
- Mở Pyspark Notebook:
    - Vào Docker Desktop, mục `Containers / Apps`
    - Chọn `pyspark-notebook`
    - Tìm dòng này và mở bằng trình duyệt: 
    ![pyspark-notebook](https://user-images.githubusercontent.com/57038442/212036829-f074d9b1-40cf-483b-9cd8-ecc369c432ff.png)
- Khi có Pyspark Notebook (Jupyter), chạy các notebook ở folder `spark`.

# Nhiệm vụ (Phần này đang tìm hiểu để làm):
- Lưu data vào Postgres:
    - Mỗi data từ MongoDB sẽ gồm `id`, `info` và `stats`
    - Lưu `info` của từng thành phần `league`/`club`/`player` vào mỗi bảng (VD: tên bảng `info.player`, `info.club`,...). Khóa chính là `id`.
    - Tách các bảng con bên trong stats ra, lưu vào bảng (VD: data shooting của tất cả player sẽ có tên `player_stats.shooting`,...). Khóa chính sẽ là `id` và `Season`.
    - Làm một vài phần tiền xử lý data.
    - Mẫu hàm lấy từ MongoDB và lưu vào Postgres có trong `test.ipynb`
    
- Visualize data (Nghiên cứu thêm)
