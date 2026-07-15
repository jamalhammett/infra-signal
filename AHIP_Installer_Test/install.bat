@echo off
title AHIP Package Installer

echo ============================================
echo   Allen Hammett Intelligence Platform
echo   Package Installer
echo ============================================
echo.

REM Locate Python
where py >nul 2>&1
if %ERRORLEVEL%==0 (
    py "%~dp0install.py"
    goto END
)

where python >nul 2>&1
if %ERRORLEVEL%==0 (
    python "%~dp0install.py"
    goto END
)

echo.
echo ERROR: Python was not found on this computer.
echo Install Python and ensure it is added to PATH.
echo.

:END
echo.
echo ============================================
echo Installation finished.
echo Press any key to close...
pause >nul
