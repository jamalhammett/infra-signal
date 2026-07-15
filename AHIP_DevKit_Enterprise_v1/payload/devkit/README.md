# AHIP DevKit Enterprise v1

This package installs the reusable AHIP DevKit into the `infra-signal`
repository.

## One-click use

1. Extract this ZIP.
2. Double-click `install.bat`.
3. The installer searches saved configuration, the known AHIP path
   `Y:\R&D\Infra-signal`, parent directories, and common drive locations.
4. If it still cannot locate the repository, it asks for the full path once.
5. After a successful install, the location is remembered.

## Capabilities

- Repository discovery and remembered configuration
- Manifest-driven file deployment
- SHA-256 duplicate detection
- Automatic backup before replacement
- Python syntax validation
- Automatic rollback after failure
- JSON installation logging
- One-click Windows launcher
