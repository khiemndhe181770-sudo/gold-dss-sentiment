# Gold DSS – Sentiment & Risk Data Pipeline

This repository automatically collects Google Trends data related to gold and market risk,
and stores raw sentiment data for a Decision Support System (DSS).

⚠️ This project is for academic demonstration only.

Cau truc
🔹 scripts/fetch_trends.py

Thu thập dữ liệu tâm lý & rủi ro thị trường từ Google Trends
→ tạo bảng RAW cho DSS

🔹 scripts/push_to_sheet.py

Đẩy dữ liệu raw lên Google Sheets
→ đảm bảo truy vết, minh bạch, audit được

🔹 .github/workflows/trends_pipeline.yml

Pipeline tự động:

chạy theo chu kỳ

không phụ thuộc con người

chứng minh DSS hoạt động độc lập

