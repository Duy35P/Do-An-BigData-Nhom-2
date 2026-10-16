[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$Host.UI.RawUI.WindowTitle = "DỪNG HỆ THỐNG BIG DATA LAKEHOUSE - NHÓM 2"
Write-Host "==============================================================================" -ForegroundColor Red
Write-Host "       DỪNG TOÀN BỘ CỤM HỆ THỐNG BIG DATA LAKEHOUSE (NHÓM 2)" -ForegroundColor Red
Write-Host "==============================================================================" -ForegroundColor Red
Write-Host ""
Write-Host "[*] Đang dừng toàn bộ dịch vụ (Hadoop, Kafka, Web Server)..." -ForegroundColor Yellow

wsl -d Ubuntu-26.04 -u dell /home/dell/stop_all.sh

Write-Host ""
Write-Host "==============================================================================" -ForegroundColor Green
Write-Host "  ✅ TOÀN BỘ HỆ THỐNG ĐÃ ĐƯỢC TẮT AN TOÀN!" -ForegroundColor Green
Write-Host "==============================================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Nhấn phím Enter để đóng cửa sổ..." -ForegroundColor Yellow
Read-Host
