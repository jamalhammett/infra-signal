@echo off
setlocal
title AHIP DevKit v2 Installer

echo ============================================================
echo   Allen Hammett Intelligence Platform
echo   AHIP DevKit v2
echo ============================================================
echo.

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
echo Install Python and ensure it is available in PATH.
echo.

:END
echo.
echo ============================================================
echo Press any key to close...
pause >nul
endlocal
