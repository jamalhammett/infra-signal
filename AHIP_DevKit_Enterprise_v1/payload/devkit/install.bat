@echo off
setlocal
title AHIP DevKit Enterprise Installer

echo ============================================================
echo   Allen Hammett Intelligence Platform
echo   AHIP DevKit Enterprise Installer
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
echo Install Python and make sure it is available in PATH.

:END
echo.
echo ============================================================
echo Press any key to close...
pause >nul
endlocal
