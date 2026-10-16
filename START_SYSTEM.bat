@echo off
title KHOI DONG HE THONG BIG DATA LAKEHOUSE - NHOM 2
color 0A
cls
echo ==============================================================================
echo       HE THONG GOI Y PHIM TRUC TUYEN - BIG DATA LAKEHOUSE (NHOM 2)
echo ==============================================================================
echo.
echo [*] Dang kich hoat toan bo cum he thong qua WSL...
echo.

wsl -d Ubuntu-26.04 -u dell -- bash /home/dell/start_all.sh

echo.
echo [*] Dang mo trinh duyet Web...
timeout /t 2 >nul
start http://localhost:8000
echo.
echo ==============================================================================
echo   HE THONG DA SAN SANG!
echo   - Web App: http://localhost:8000
echo   - Hadoop:  http://localhost:9870
echo ==============================================================================
echo.
pause
