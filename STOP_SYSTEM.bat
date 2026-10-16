@echo off
title DUNG HE THONG BIG DATA LAKEHOUSE - NHOM 2
color 0C
cls
echo ==============================================================================
echo       DUNG TOAN BO HE THONG BIG DATA LAKEHOUSE (NHOM 2)
echo ==============================================================================
echo.
echo [*] Dang dung cac dich vu Hadoop, Kafka, Web Server...
echo.

wsl -d Ubuntu-26.04 -u dell -- bash /home/dell/stop_all.sh

echo.
echo ==============================================================================
echo   HE THONG DA DUOC DUNG AN TOAN!
echo ==============================================================================
echo.
pause
