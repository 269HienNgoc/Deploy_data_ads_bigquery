# Kế hoạch: Facebook Ads → BigQuery → Looker Studio (Go Service)

---

## 1. Context (Bối cảnh)

Người dùng cần xây dựng **1 service bằng Go** để:
- Gọi Facebook Marketing API → lấy dữ liệu tài khoản quảng cáo, chiến dịch, insights
- Load dữ liệu vào **Google BigQuery** (layer `ads_raw` + `ads_mart`)
- Trực quan hóa bằng **Looker Studio**

**Thách thức chính:**
- Dữ liệu có thể **> 3 triệu records** → cần thiết kế batching, concurrency
- Facebook giới hạn backfill **37 tháng** → cần chunk theo tháng
- 2 chế độ chạy: **daily** (ngày hiện tại) và **backfill** (lịch sử)
- 4 thời điểm chạy: 1h AM, 8h AM, 14h PM, 17h PM

---

## 2. Quyết định đã xác nhận

| Câu hỏi | Quyết định |
|---|---|
| Backfill | Chạy **1 lần duy nhất** khi deploy — không cần chạy lại tự động |
| Error handling | **Retry toàn bộ job** — đơn giản, đáng tin cậy |
| BigQuery method | **Streaming Insert** — gửi trực tiếp 500 rows/rpc |
| Deploy target | **Local machine** — chạy thủ công bằng CLI |

---

## 3. Cấu trúc thư mục (MVC, mở rộng cho nhiều platform)

```
deploy_data_bigquery/
├── cmd/
│   └── fetcher/
│       └── main.go              # Entry point, đăng ký cron job
├── internal/
│   ├── config/
│   │   └── config.go            # Đọc .env bằng viper
│   ├── models/
│   │   ├── account.go           # Account model
│   │   ├── campaign.go          # Campaign model
│   │   └── insight.go           # Insight model (mart)
│   ├── repository/
│   │   ├── bigquery/
│   │   │   ├── raw_repo.go      # Insert vào ads_raw
│   │   │   └── mart_repo.go     # Insert/update vào ads_mart
│   │   └── interfaces.go        # Repository interfaces (ISP)
│   ├── service/
│   │   ├── facebook/
│   │   │   ├── client.go        # Facebook Graph API client
│   │   │   ├── fetcher.go       # Logic gọi API (batch, retry)
│   │   │   ├── transformer.go   # Transform response → model
│   │   │   └── rate_limiter.go  # Facebook 200 calls/giờ rate limit
│   │   ├── pipeline.go          # Orchestrator: fetch → transform → load
│   │   └── scheduler.go         # Cron job logic
│   ├── worker/
│   │   └── worker_pool.go       # Goroutine pool cho concurrency
│   └── logger/
│       └── logger.go            # Zap logger setup (Structured logging)
├── configs/
│   └── .env.example             # Template cấu hình
├── scripts/
│   └── sql/
│       ├── create_raw_tables.sql
│       └── create_mart_table.sql
├── go.mod
├── go.sum
└── prompt_req.md
```

---

## 4. Database Schema (BigQuery)

### Dataset: `ads_raw`

```sql
-- raw_flatform_account
CREATE TABLE ads_raw.raw_flatform_account (
  id             STRING      NOT NULL,
  name           STRING,
  account_status INT64,
  currency       STRING,
  timezone_name  STRING,
  created_time   TIMESTAMP,
  updated_time   TIMESTAMP,
  spend_cap      FLOAT64,
  amount_spent   FLOAT64,
  flatform       STRING,          -- 'facebook', 'google', 'tiktok'
  fetched_at     TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP()
);
```

```sql
-- raw_flatform_campaign
CREATE TABLE ads_raw.raw_flatform_campaign (
  id                 STRING      NOT NULL,
  account_id         STRING,
  name               STRING,
  objective          STRING,
  status             STRING,
  configured_status  STRING,
  effective_status   STRING,
  buying_type        STRING,
  daily_budget       FLOAT64,
  lifetime_budget    FLOAT64,
  start_time         TIMESTAMP,
  stop_time          TIMESTAMP,
  created_time       TIMESTAMP,
  updated_time       TIMESTAMP,
  flatform           STRING,
  fetched_at         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP()
);
```

### Dataset: `ads_mart`

```sql
-- mart_platform_insights
CREATE TABLE ads_mart.mart_platform_insights (
  date           DATE        NOT NULL,
  flatform       STRING,
  account_id     STRING,
  account_name   STRING,
  campaign_id    STRING,
  campaign_name  STRING,
  status         STRING,
  daily_budget   FLOAT64,
  spend          FLOAT64,
  fetched_at     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(date)
CLUSTER BY flatform, account_id;
```

---

## 4. Thiết kế xử lý 3 triệu records

### 4.1 Chiến lược Batching & Concurrency

```
Main Goroutine
    ├── Worker Pool (10 goroutines)
    │     ├── Worker 1:  Gọi Facebook API (chunk 1)  → Transform  → BigQuery Streaming
    │     ├── Worker 2:  Gọi Facebook API (chunk 2)  → Transform  → BigQuery Streaming
    │     └── Worker N: ...
    └── Batch Collector: Gộp 500 rows → 1 RPC call BigQuery
```

| Chiến lược | Giá trị |
|---|---|
| Facebook Batch API | Mỗi request gọi tối đa **50 campaigns** cùng lúc |
| Goroutine Worker Pool | **10-20 concurrent workers** |
| BigQuery Streaming Insert | Gửi **500 rows/rpc** |
| Backfill chunk | Mỗi lần chạy backfill: **1 tháng / account** |
| Retry | Exponential backoff: 1s → 2s → 4s → 8s (max 3 lần) |
| Facebook Rate Limit | Sliding window 200 calls/giờ — nếu chạm limit → sleep đến khi slot freed |

### 4.2 Chế độ chạy

| Mode | Trigger | Logic |
|---|---|---|
| `daily` | Cron 1h, 8h, 14h, 17h | Lấy `date_preset=today,yesterday` |
| `backfill` | Cron 1 lần (chỉ định tháng) | Lặp qua **từng tháng**, mỗi tháng gọi API với `time_range` |

### 4.3 Backfill chi tiết

```
FOR each (account_id, month) IN backfill_months:
    time_range = {since: first_day_of_month, until: last_day_of_month}
    Gọi /{account_id}/insights?time_range=...
    FOR each page IN paginated_response:
        Transform → Batch 500 rows → BigQuery Insert
    Sleep 1 giây giữa các account (tránh rate limit)
```

---

## 5. Áp dụng SOLID Principles

| Nguyên tắc | Cách áp dụng |
|---|---|
| **S**ingle Responsibility | Mỗi file chỉ làm 1 việc: `client.go` gọi API, `transformer.go` chuyển đổi, `raw_repo.go` insert |
| **O**pen/Closed | Thêm platform mới (Google, TikTok) → chỉ cần thêm `service/google/` hoặc `service/tiktok`, không sửa code cũ |
| **L**iskov Substitution | `FacebookFetcher` và `GoogleFetcher` implement cùng `AdsFetcher` interface → interchangeable |
| **I**nterface Segregation | Nhiều interface nhỏ: `AccountRepository`, `CampaignRepository`, `InsightRepository` |
| **D**ependency Inversion | `Pipeline` phụ thuộc interface `AdsFetcher`, `DataRepository` — không phụ thuộc implementation cụ thể |

### Interface Design

```go
// Repository interfaces
type AccountRepository interface { InsertAccounts(ctx context.Context, accounts []Account) error }
type CampaignRepository interface { InsertCampaigns(ctx context.Context, campaigns []Campaign) error }
type InsightRepository interface { InsertInsights(ctx context.Context, insights []Insight) error }

// Fetcher interface (Platform mới chỉ cần implement interface này)
type AdsFetcher interface {
    FetchAccounts(ctx context.Context) ([]Account, error)
    FetchCampaigns(ctx context.Context, accountID string) ([]Campaign, error)
    FetchInsights(ctx context.Context, accountID string, timeRange TimeRange) ([]Insight, error)
}
```

---

## 6. Cấu hình (.env)

```env
# Facebook
FACEBOOK_ACCESS_TOKEN=EAACEdEose0c...

# BigQuery
GOOGLE_APPLICATION_CREDENTIALS=./configs/service_account.json
BQ_PROJECT_ID=my-project
BQ_DATASET_RAW=ads_raw
BQ_DATASET_MART=ads_mart

# App
APP_MODE=daily           # daily | backfill
RUN_BACKFILL_SINCE=2023-01-01
MAX_WORKERS=10
BATCH_SIZE=500
CRON_SCHEDULE=0 1,8,14,17 * * *   # 1h, 8h, 14h, 17h

# Retry
MAX_RETRIES=3
RETRY_BASE_DELAY=1s
```

---

## 7. Triển khai từng bước

### Bước 1: Khởi tạo project
```bash
mkdir -p deploy_data_bigquery && cd deploy_data_bigquery
go mod init deploy_data_bigquery
go get github.com/spf13/viper
go get cloud.google.com/go/bigquery
go get github.com/robfig/cron/v3
go get github.com/google/uuid
go get go.uber.org/zap
```

### Bước 2: Tạo config + models
- `internal/config/config.go` — đọc .env
- `internal/models/account.go`, `campaign.go`, `insight.go`

### Bước 3: Xây dựng Facebook API Client
- `internal/service/facebook/client.go` — HTTP client + batch request
- `internal/service/facebook/fetcher.go` — gọi 3 endpoint
- `internal/service/facebook/transformer.go` — map response → model

### Bước 4: Xây dựng BigQuery Repository
- `internal/repository/bigquery/raw_repo.go` — insert raw data
- `internal/repository/bigquery/mart_repo.go` — upsert mart data

### Bước 5: Xây dựng Pipeline & Worker
- `internal/worker/worker_pool.go` — goroutine pool
- `internal/service/pipeline.go` — orchestrate toàn bộ flow

### Bước 6: Scheduler
- `internal/service/scheduler.go` — đăng ký cron jobs
- `cmd/fetcher/main.go` — entry point

### Bước 7: SQL setup
- Tạo dataset + tables trong BigQuery

### Bước 8: Tests
- Unit test cho transformer, mock repository

---

## 8. Mở rộng thêm platform (Google, TikTok)

Khi thêm platform mới, chỉ cần:

```
internal/
  └── service/
        ├── facebook/      ← đã có
        ├── google/        ← THÊM MỚI
        │     ├── client.go
        │     ├── fetcher.go
        │     └── transformer.go
        └── tiktok/        ← THÊM MỚI (nếu cần)
```

Code cũ ở `pipeline.go`, `scheduler.go`, `cmd/` **KHÔNG cần sửa**.

---

## 9. Verification

| Test | Cách kiểm tra |
|---|---|
| Đọc .env đúng | Chạy `go run cmd/fetcher/main.go` — không crash |
| Gọi Facebook API thật | Log ra response JSON, kiểm tra số lượng campaigns |
| Insert BigQuery | Query `SELECT COUNT(*) FROM ads_raw.raw_flatform_campaign` |
| Batch 500 rows | Log số lần gọi RPC, so sánh với tổng records |
| Backfill 37 tháng | Chạy mode=backfill, verify dữ liệu đủ 37 tháng |
| Scheduler chạy đúng giờ | Quan sát log tại 1h, 8h, 14h, 17h |
| Concurrency | So sánh thời gian chạy: 1 worker vs 10 workers |

---

## 10. Góp ý bổ sung

1. **Rate limit Facebook**: Cần tracking `X-Business-Use-Case-Usage` header. Nếu gặp rate limit → sleep thêm 5-10 phút.
2. **Deduplication**: Dùng `MERGE` statement trong BigQuery (thay vì INSERT) trên `mart_platform_insights` theo `(date, campaign_id)` để tránh trùng khi chạy lại.
3. **Structured Logging với Zap**:
   - Thư viện: `go.uber.org/zap`
   - 2 output modes:
     - `production`: JSON format → gửi log lên ELK / Cloud Logging
     - `development`: Console colorized → dễ đọc khi dev
   - Mỗi log entry gồm: `timestamp`, `level`, `caller`, `message`, `request_id`, `fields`
   - Ví dụ fields: `account_id`, `campaign_count`, `rows_inserted`, `duration_ms`, `error`
   - Cấu hình trong `.env`: `LOG_LEVEL=debug|info|warn|error`, `LOG_ENV=development|production`
   - File: `internal/logger/logger.go` — khởi tạo và export singleton `Logger`
4. **Facebook API Rate Limit (200 calls/giờ)**:
   - **Cơ chế Sliding Window**: Dùng `time.Ticker` + queue lưu timestamp mỗi lần gọi API
   - **Trigger**: Khi số calls trong 1 giờ qua **>= 180** → bắt đầu kiểm tra trước mỗi request
   - **Logic chờ**: Khi sắp chạm 200 → tính `time.Since(oldest_call_timestamp)`:
     - Nếu < 1 giờ → **sleep đến khi đủ 1 giờ**, sau đó clear queue và tiếp tục
     - Nếu >= 1 giờ → clear queue cũ, tiếp tục bình thường
   - **Ưu tiên Batch API**: Gom nhiều request vào 1 batch call (tối đa 50/request) → giảm số API calls
   - **File**: `internal/service/facebook/rate_limiter.go`
   - **Log khi rate limit**: Ghi rõ `RATE_LIMIT: waiting X seconds, next available slot at HH:MM:SS`
5. **Alerting**: Nếu chạy trên Cloud Run / VM, cần thêm alert khi số rows inserted = 0 (dấu hiệu API key hết hạn).
6. **OAuth token refresh**: Token Facebook có thể hết hạn → implement token refresh flow.
7. **IAM/Quyền BigQuery**: Service account cần quyền `bigquery.dataEditor` trên dataset `ads_raw` và `ads_mart`.
