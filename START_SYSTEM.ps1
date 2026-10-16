[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$Host.UI.RawUI.WindowTitle = "HỆ THỐNG GỢI Ý PHIM LAKEHOUSE - NHÓM 2"
Write-Host "==============================================================================" -ForegroundColor Green
Write-Host "      HỆ THỐNG GỢI Ý PHIM TRỰC TUYẾN DỰA TRÊN KIẾN TRÚC LAKEHOUSE" -ForegroundColor Green
Write-Host "                    (ĐỒ ÁN TỐT NGHIỆP BIG DATA - NHÓM 2)" -ForegroundColor Green
Write-Host "==============================================================================" -ForegroundColor Green
Write-Host ""
Write-Host "[*] Đang kích hoạt toàn bộ cụm dịch vụ qua WSL (Ubuntu-26.04)..." -ForegroundColor Yellow
Write-Host "    + Hadoop HDFS (NameNode + DataNode 1)" -ForegroundColor Gray
Write-Host "    + Hadoop DataNode 2 (Cluster 2 Live Nodes)" -ForegroundColor Gray
Write-Host "    + Apache Kafka KRaft Broker (Port 9092)" -ForegroundColor Gray
Write-Host "    + Web Application FastAPI Recommender (Port 8000)" -ForegroundColor Gray
Write-Host ""

wsl -d Ubuntu-26.04 -u dell /home/dell/start_all.sh

Write-Host ""
Write-Host "[*] Đang tự động mở giao diện Web trên trình duyệt..." -ForegroundColor Cyan
Start-Sleep -Seconds 2
Start-Process "http://localhost:8000"

Write-Host ""
Write-Host "==============================================================================" -ForegroundColor Green
Write-Host "  ✅ HỆ THỐNG ĐÃ SẴN SÀNG 100%!" -ForegroundColor Green
Write-Host "  - Giao diện Web:     http://localhost:8000" -ForegroundColor White
Write-Host "  - Hadoop NameNode:   http://localhost:9870" -ForegroundColor White
Write-Host "==============================================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Nhấn phím Enter để đóng cửa sổ này (hệ thống vẫn tiếp tục chạy ngầm)..." -ForegroundColor Yellow
Read-Host
