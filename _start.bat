@echo off
rem NeirOS-M — запуск программы с правильной кодировкой консоли
chcp 65001 >nul
cd /d "%~dp0"
python -X utf8 main.py
pause
