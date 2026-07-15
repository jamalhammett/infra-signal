from pathlib import Path

class InstallerBuilder:
    VERSION="1.0.0"

    def build(self, output:Path):
        content = """@echo off
where py >nul 2>&1
if %ERRORLEVEL%==0 (
    py installer.py
) else (
    python installer.py
)
pause
"""
        output.write_text(content, encoding="utf-8")
        return output
