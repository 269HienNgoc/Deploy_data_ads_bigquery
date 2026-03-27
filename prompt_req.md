# Looker Studio Dashboard Ads Facebook, Google .. (Có thể thêm nhiều nền tảng khác)

## Luồng dữ liệu ban đầu
Lấy data từ API -> Lưu vào bigquery google -> Trực quan lên Looker Studio

1. Xử lý phần facebook trước.
- Xử lý Grab API của facebook:
- Endpoint:
  + /v25.0/me/adaccounts : Lấy thông tin danh sách tài khoản quảng cáo.
  + /v25.0/{{ID Ads Account}}/insights: lấy thông tin chiến dịch đang chạy quảng cáo của 1 tài khoản.
  + /v25.0/{{ ID campaign }}: Thông tin của 1 chiến dịch.
- Key Field:
  + /v25.0/{{ ID campaign }}:
    - access_token: token truy cập
    - fields: id,name,objective,status,configured_status,effective_status,buying_type,daily_budget,lifetime_budget,start_time,stop_time,created_time,updated_time.
  + /v25.0/me/adaccounts:
    - access_token: token truy cập
    - fields: id,name,account_status,currency,timezone_name,created_time,updated_time,spend_cap,amount_spent
  + /v25.0/{{ID Ads Account}}/insights
    - level: 
    - date_preset : enum: (today, yesterday, this_month, last_month, this_quarter, maximum, data_maximum, last_3d, last_7d, last_14d, last_28d, last_30d, last_90d, last_week_mon_sun, last_week_sun_sat, last_quarter, last_year, this_week_mon_today, this_week_sun_today, this_year)
    - time_range: {'since': '2026-03-10' , 'until': '2026-03-20' } => Khoảng thời gian.
    - access_token: token truy cập..
    - fields: account_id,account_name,campaign_id,campaign_name,date_start,date_stop,spend
    - effective_status: enum(ACTIVE, PAUSED, DELETED, PENDING_REVIEW, DISAPPROVED, PREAPPROVED, PENDING_BILLING_INFO, CAMPAIGN_PAUSED, ARCHIVED, ADSET_PAUSED, IN_PROCESS, WITH_ISSUES)
## Database schema:
- Dataset:
  + ads_raw
    raw_flatform_account:
      
    raw_flatform_campaign:

  + ads_mart
    mart_platform_insights:
      date            : date
      flatform        : string 'Tên nền tản vd: facebook, google, tiktok...'
      account_name    : string (Tên tài khoản)
      campaign_name   : string (Tên chiến dịch)
      status          : string (Trạng thái chiến dịch)
      daily_budget    : float (Chi phí / ngày)
      spend           : float (Chi phí chi tiêu / ngày)

## Yêu cầu:
### Kỹ thuật và công nghệ:
- Sử dụng Golang, có thể áp dụng khả năng đa luồng của Golang để tối ưu nếu được. 
- Tuân Thủ nguyên tắc SOLID.
  S - Single Responsibility Principle (Nguyên tắc Trách nhiệm duy nhất): Một lớp (class) chỉ nên có một lý do duy nhất để thay đổi, tức là chỉ nên thực hiện một chức năng cụ thể.
  O - Open/Closed Principle (Nguyên tắc Đóng/Mở): Thực thể phần mềm (class, module, function) nên mở rộng để thêm tính năng mới, nhưng đóng để sửa đổi mã nguồn hiện có.
  L - Liskov Substitution Principle (Nguyên tắc Thay thế Liskov): Các đối tượng của lớp con có thể thay thế đối tượng của lớp cha mà không làm thay đổi tính đúng đắn của chương trình.
  I - Interface Segregation Principle (Nguyên tắc Phân tách giao diện): Thay vì sử dụng một interface lớn, đa năng, nên chia thành các interface nhỏ, cụ thể để các lớp không bị buộc phải phụ thuộc vào những interface chúng không dùng.
  D - Dependency Inversion Principle (Nguyên tắc Đảo ngược phụ thuộc): Các module cấp cao không nên phụ thuộc vào các module cấp thấp; cả hai nên phụ thuộc vào các lớp trừu tượng (interface/abstract class). 
- Cấu trúc thư mục Theo Mô Hình MVC đơn giản dễ mở rộng thêm những service khác mà không ảnh hưởng tới nhau.
- Có Crond Job để lên lịch chạy.
- Có 2 kiểu chạy.
  1. Chạy lần đầu lấy dữ liệu chiến dịch mới nhất theo ngày hiện tại.
  2. Chạy lấy những dữ liệu cũ. tối đa 37 tháng theo rate limit của facebook.
  3. Thời gian hẹn giờ chạy: 1h AM, 8h Am, 14h PM, 17h PM.
- Tất cả cấu hình gộp hết vào 1 file .env.
### Lượng dữ liệu.
Có thể hơn 3tr record vào Bigquyery. Hãy thiết kế 1 cơ chế giúp tối ưu vấn đề này

