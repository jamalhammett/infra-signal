from __future__ import annotations

import ast
import hashlib
import json
import os
import shutil
import string
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


DEVKIT_VERSION = "1.0.0"
REPO_MARKERS = ("app.py", "engines", "README.md")
KNOWN_REPOSITORY_CANDIDATES = (
    Path(r"Y:\R&D\Infra-signal"),
    Path(r"Y:\R&D\infra-signal"),
    Path.home() / "Documents" / "GitHub" / "infra-signal",
    Path.home() / "GitHub" / "infra-signal",
    Path.home() / "source" / "repos" / "infra-signal",
)


@dataclass(slots=True)
class InstallRecord:
    source: str
    destination: str
    action: str
    status: str
    checksum: str | None = None
    message: str | None = None


class InstallerError(RuntimeError):
    pass


class AHIPEnterpriseInstaller:
    def __init__(self, package_dir: Path) -> None:
        self.package_dir = package_dir.resolve()
        self.config_path = Path.home() / ".ahip-devkit.json"
        self.manifest = self._load_manifest()
        self.repo_root = self._discover_repository()
        self.timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.backup_root = self.repo_root / "backups" / f"install_{self.timestamp}"
        self.log_root = self.repo_root / "logs"
        self.records: list[InstallRecord] = []
        self.created_files: list[Path] = []
        self.replaced_files: list[tuple[Path, Path]] = []

    def run(self) -> int:
        started = time.perf_counter()
        self._print_header()

        try:
            self._validate_manifest()
            self._validate_payload()
            self._prepare_directories()
            self._install_files()
            self._validate_installed_python()
            self._save_repository_location()
            elapsed = time.perf_counter() - started
            self._write_log(success=True, elapsed=elapsed)
            self._print_summary(success=True, elapsed=elapsed)
            return 0
        except Exception as exc:
            self._rollback()
            elapsed = time.perf_counter() - started
            self._write_log(success=False, elapsed=elapsed, error=str(exc))
            self._print_summary(success=False, elapsed=elapsed, error=str(exc))
            return 1

    def _load_manifest(self) -> dict[str, Any]:
        path = self.package_dir / "manifest.json"
        if not path.exists():
            raise InstallerError("manifest.json is missing.")
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise InstallerError(f"manifest.json is invalid: {exc}") from exc

    @staticmethod
    def _is_repo(path: Path) -> bool:
        return (
            path.exists()
            and path.is_dir()
            and (path / "app.py").is_file()
            and (path / "engines").is_dir()
            and (path / "README.md").is_file()
        )

    def _discover_repository(self) -> Path:
        candidates: list[Path] = []

        # 1. Saved configuration from a prior successful install.
        if self.config_path.exists():
            try:
                config = json.loads(self.config_path.read_text(encoding="utf-8"))
                saved = config.get("repository")
                if saved:
                    candidates.append(Path(saved))
            except (OSError, json.JSONDecodeError):
                pass

        # 2. Explicit environment override.
        env_root = os.environ.get("AHIP_REPO_ROOT")
        if env_root:
            candidates.append(Path(env_root))

        # 3. Known location for this deployment.
        candidates.extend(KNOWN_REPOSITORY_CANDIDATES)

        # 4. Current working directory and package ancestry.
        candidates.extend(self._ancestor_candidates(Path.cwd()))
        candidates.extend(self._ancestor_candidates(self.package_dir))

        # 5. Common root-level repository locations on mounted drives.
        candidates.extend(self._common_drive_candidates())

        seen: set[str] = set()
        for candidate in candidates:
            normalized = str(candidate).lower()
            if normalized in seen:
                continue
            seen.add(normalized)
            if self._is_repo(candidate):
                print(f"[FOUND] AHIP repository: {candidate}")
                return candidate.resolve()

        return self._prompt_for_repository()

    @staticmethod
    def _ancestor_candidates(start: Path) -> list[Path]:
        results: list[Path] = []
        current = start.resolve()
        while True:
            results.append(current)
            if current == current.parent:
                break
            current = current.parent
        return results

    @staticmethod
    def _common_drive_candidates() -> list[Path]:
        names = (
            "infra-signal",
            "Infra-signal",
            "Infra-Signal",
            r"R&D\Infra-signal",
            r"R&D\infra-signal",
            r"GitHub\infra-signal",
            r"Projects\infra-signal",
        )
        candidates: list[Path] = []
        for letter in string.ascii_uppercase:
            root = Path(f"{letter}:\\")
            if not root.exists():
                continue
            for name in names:
                candidates.append(root / name)
        return candidates

    def _prompt_for_repository(self) -> Path:
        print()
        print("[NOTICE] AHIP repository was not found automatically.")
        print("Enter the full path to the infra-signal repository.")
        print(r"Example: Y:\R&D\Infra-signal")
        print()

        for _ in range(3):
            raw = input("Repository path: ").strip().strip('"')
            candidate = Path(raw)
            if self._is_repo(candidate):
                return candidate.resolve()
            print("[ERROR] That folder does not contain app.py, engines, and README.md.")

        raise InstallerError("AHIP repository could not be located.")

    def _validate_manifest(self) -> None:
        required = {"package_name", "package_version", "files"}
        missing = required.difference(self.manifest)
        if missing:
            raise InstallerError(
                "Manifest is missing: " + ", ".join(sorted(missing))
            )

        files = self.manifest["files"]
        if not isinstance(files, list) or not files:
            raise InstallerError("Manifest must contain at least one file.")

        destinations: set[str] = set()
        for index, item in enumerate(files, start=1):
            if not isinstance(item, dict):
                raise InstallerError(f"File entry {index} must be an object.")
            if "source" not in item or "destination" not in item:
                raise InstallerError(
                    f"File entry {index} requires source and destination."
                )
            destination = str(item["destination"])
            if destination in destinations:
                raise InstallerError(f"Duplicate destination: {destination}")
            destinations.add(destination)

    def _validate_payload(self) -> None:
        for item in self.manifest["files"]:
            source = self.package_dir / item["source"]
            if not source.is_file():
                raise InstallerError(f"Missing payload file: {item['source']}")

    def _prepare_directories(self) -> None:
        self.backup_root.mkdir(parents=True, exist_ok=True)
        self.log_root.mkdir(parents=True, exist_ok=True)

    def _install_files(self) -> None:
        print()
        print("Installing package files...")

        for item in self.manifest["files"]:
            source = (self.package_dir / item["source"]).resolve()
            destination = (self.repo_root / item["destination"]).resolve()

            if destination != self.repo_root and self.repo_root not in destination.parents:
                raise InstallerError(
                    f"Unsafe destination outside repository: {item['destination']}"
                )

            destination.parent.mkdir(parents=True, exist_ok=True)
            source_hash = self._sha256(source)

            if destination.exists():
                destination_hash = self._sha256(destination)
                if source_hash == destination_hash:
                    self.records.append(
                        InstallRecord(
                            source=item["source"],
                            destination=item["destination"],
                            action="skipped",
                            status="unchanged",
                            checksum=source_hash,
                        )
                    )
                    print(f"[SKIP] {item['destination']} (unchanged)")
                    continue

                backup = self.backup_root / item["destination"]
                backup.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(destination, backup)
                self.replaced_files.append((destination, backup))
                action = "updated"
            else:
                self.created_files.append(destination)
                action = "installed"

            shutil.copy2(source, destination)
            self.records.append(
                InstallRecord(
                    source=item["source"],
                    destination=item["destination"],
                    action=action,
                    status="success",
                    checksum=source_hash,
                )
            )
            print(f"[OK]   {item['destination']} ({action})")

    def _validate_installed_python(self) -> None:
        print()
        print("Validating Python syntax...")

        for item in self.manifest["files"]:
            destination = self.repo_root / item["destination"]
            if destination.suffix.lower() != ".py":
                continue
            try:
                ast.parse(destination.read_text(encoding="utf-8"))
            except SyntaxError as exc:
                raise InstallerError(
                    f"Syntax validation failed for {item['destination']} "
                    f"at line {exc.lineno}: {exc.msg}"
                ) from exc
            print(f"[PASS] {item['destination']}")

    def _rollback(self) -> None:
        if not self.created_files and not self.replaced_files:
            return

        print()
        print("[ROLLBACK] Restoring repository...")

        for created in reversed(self.created_files):
            if created.exists():
                created.unlink()

        for destination, backup in reversed(self.replaced_files):
            if backup.exists():
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(backup, destination)

        print("[ROLLBACK] Complete.")

    def _save_repository_location(self) -> None:
        payload = {
            "repository": str(self.repo_root),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "devkit_version": DEVKIT_VERSION,
        }
        self.config_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _write_log(
        self,
        *,
        success: bool,
        elapsed: float,
        error: str | None = None,
    ) -> None:
        self.log_root.mkdir(parents=True, exist_ok=True)
        log_path = self.log_root / f"install_{self.timestamp}.json"
        payload = {
            "devkit_version": DEVKIT_VERSION,
            "package_name": self.manifest.get("package_name"),
            "package_version": self.manifest.get("package_version"),
            "repository": str(self.repo_root),
            "timestamp": self.timestamp,
            "success": success,
            "elapsed_seconds": round(elapsed, 3),
            "backup_directory": str(self.backup_root),
            "error": error,
            "files": [asdict(record) for record in self.records],
        }
        log_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _print_header(self) -> None:
        print("=" * 62)
        print("Allen Hammett Intelligence Platform")
        print(f"AHIP DevKit Enterprise v{DEVKIT_VERSION}")
        print("=" * 62)
        print(f"Repository : {self.repo_root}")
        print(f"Package    : {self.manifest.get('package_name')}")
        print(f"Version    : {self.manifest.get('package_version')}")
        print("-" * 62)

    def _print_summary(
        self,
        *,
        success: bool,
        elapsed: float,
        error: str | None = None,
    ) -> None:
        installed = sum(record.action == "installed" for record in self.records)
        updated = sum(record.action == "updated" for record in self.records)
        skipped = sum(record.action == "skipped" for record in self.records)

        print()
        print("=" * 62)
        print("INSTALLATION SUCCESSFUL" if success else "INSTALLATION FAILED")
        print("=" * 62)
        print(f"Installed : {installed}")
        print(f"Updated   : {updated}")
        print(f"Skipped   : {skipped}")
        print(f"Errors    : {0 if success else 1}")
        print(f"Elapsed   : {elapsed:.2f} seconds")
        if success:
            print(f"Backup    : {self.backup_root}")
            print(f"Logs      : {self.log_root}")
        if error:
            print(f"Error     : {error}")

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()


def main() -> int:
    try:
        installer = AHIPEnterpriseInstaller(Path(__file__).parent)
        return installer.run()
    except Exception as exc:
        print()
        print(f"[FATAL] {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
