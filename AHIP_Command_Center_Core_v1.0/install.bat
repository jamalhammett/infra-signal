@echo off
setlocal
title AHIP Command Center Core Installer

where py >nul 2>&1
if %ERRORLEVEL%==0 (
    py "%~dp0installer.py"
    goto END
)

where python >nul 2>&1
if %ERRORLEVEL%==0 (
    python "%~dp0installer.py"
    goto END
)

echo [ERROR] Python was not found.

:END
echo.
echo Press any key to close...
pause >nul
endlocal
