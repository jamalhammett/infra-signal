@echo off
where py >nul 2>&1
if %ERRORLEVEL%==0 (py "%~dp0installer.py") else (python "%~dp0installer.py")
pause
