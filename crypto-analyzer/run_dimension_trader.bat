@echo off
cd /d D:\test3\crypto-analyzer
".venv\Scripts\python.exe" -u dimension_trader.py > logs\dimension_trader.log 2> logs\dimension_trader.err.log
