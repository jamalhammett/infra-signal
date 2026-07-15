from __future__ import annotations

import ast
import hashlib
import json
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


DEVKIT_VERSION = "2.0.0"


@dataclass
class InstallRecord:
    source: str
    destination: str
    action: str
    status: str
    checksum: str | None = None
    message: str | None = None


class AHIPInstaller:
    def __init__(self, package_dir: Path) -> None:
        self.package_dir = package_dir.resolve()
        self.repo_root = self._find_repo_root()
        self.manifest_path = self.package_dir / "manifest.json"
        self.manifest = self._load_manifest()
        self.timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.backup_root = self.repo_root / "backups" / f"install_{self.timestamp}"
        self.log_dir = self.repo_root / "logs"
        self.records: list[InstallRecord] = []
        self.created_files: list[Path] = []
        self.replaced_files: list[tuple[Path, Path]] = []

    def run(self) -> int:
        start = time.perf_counter()
        self._print_header()

        try:
            self._validate_manifest()
            self._validate_payload()
            self._prepare_directories()
            self._install_files()
            self._validate_installed_python()
            self._write_log(success=True, elapsed=time.perf_counter() - start)
            self._print_summary(success=True, elapsed=time.perf_counter() - start)
            return 0
        except Exception as exc:
            self._rollback()
            self._write_log(
                success=False,
                elapsed=time.perf_counter() - start,
                error=str(exc),
            )
            self._print_summary(
                success=False,
                elapsed=time.perf_counter() - start,
                error=str(exc),
            )
            return 1

    def _find_repo_root(self) -> Path:
        candidates = [Path.cwd().resolve(), self.package_dir]
        searched: set[Path] = set()

        for candidate in candidates:
            current = candidate
            while current not in searched:
                searched.add(current)
                if (
                    (current / "app.py").exists()
                    and (current / "engines").exists()
                    and (current / "README.md").exists()
                ):
                    return current
                if current == current.parent:
                    break
                current = current.parent

        raise RuntimeError(
            "AHIP repository root was not found. "
            "Run this package from inside the infra-signal repository."
        )

    def _load_manifest(self) -> dict[str, Any]:
        if not self.manifest_path.exists():
            raise RuntimeError("manifest.json is missing from the package.")
        try:
            return json.loads(self.manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"manifest.json is invalid: {exc}") from exc

    def _validate_manifest(self) -> None:
        required = {"package_name", "package_version", "files"}
        missing = required.difference(self.manifest)
        if missing:
            raise RuntimeError(
                "Manifest is missing required fields: " + ", ".join(sorted(missing))
            )

        files = self.manifest["files"]
        if not isinstance(files, list) or not files:
            raise RuntimeError("Manifest must contain at least one file entry.")

        destinations: set[str] = set()
        for index, item in enumerate(files, start=1):
            if not isinstance(item, dict):
                raise RuntimeError(f"Manifest file entry {index} must be an object.")
            if "source" not in item or "destination" not in item:
                raise RuntimeError(
                    f"Manifest file entry {index} requires source and destination."
                )
            destination = str(item["destination"])
            if destination in destinations:
                raise RuntimeError(f"Duplicate destination detected: {destination}")
            destinations.add(destination)

    def _validate_payload(self) -> None:
        for item in self.manifest["files"]:
            source = self.package_dir / item["source"]
            if not source.exists() or not source.is_file():
                raise RuntimeError(f"Payload file is missing: {item['source']}")

    def _prepare_directories(self) -> None:
        self.backup_root.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def _install_files(self) -> None:
        print("Installing package files...")
        for item in self.manifest["files"]:
            source = (self.package_dir / item["source"]).resolve()
            destination = (self.repo_root / item["destination"]).resolve()

            if self.repo_root not in destination.parents:
                raise RuntimeError(
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
                            action="skip",
                            status="unchanged",
                            checksum=source_hash,
                        )
                    )
                    print(f"[SKIP] {item['destination']} (unchanged)")
                    continue

                backup_path = self.backup_root / item["destination"]
                backup_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(destination, backup_path)
                self.replaced_files.append((destination, backup_path))
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
                raise RuntimeError(
                    f"Python validation failed for {item['destination']}: "
                    f"line {exc.lineno}: {exc.msg}"
                ) from exc
            print(f"[PASS] {item['destination']}")

    def _rollback(self) -> None:
        print()
        print("Installation failed. Rolling back changes...")

        for created in reversed(self.created_files):
            if created.exists():
                created.unlink()

        for destination, backup in reversed(self.replaced_files):
            if backup.exists():
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(backup, destination)

        print("Rollback complete.")

    def _write_log(
        self,
        *,
        success: bool,
        elapsed: float,
        error: str | None = None,
    ) -> None:
        log_path = self.log_dir / f"install_{self.timestamp}.json"
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
            "files": [record.__dict__ for record in self.records],
        }
        log_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _print_header(self) -> None:
        print("=" * 60)
        print("Allen Hammett Intelligence Platform")
        print(f"AHIP DevKit v{DEVKIT_VERSION}")
        print("=" * 60)
        print(f"Repository : {self.repo_root}")
        print(f"Package    : {self.manifest.get('package_name', 'Unknown')}")
        print(f"Version    : {self.manifest.get('package_version', 'Unknown')}")
        print("-" * 60)

    def _print_summary(
        self,
        *,
        success: bool,
        elapsed: float,
        error: str | None = None,
    ) -> None:
        installed = sum(record.action == "installed" for record in self.records)
        updated = sum(record.action == "updated" for record in self.records)
        skipped = sum(record.action == "skip" for record in self.records)

        print()
        print("=" * 60)
        print("INSTALLATION SUCCESSFUL" if success else "INSTALLATION FAILED")
        print("=" * 60)
        print(f"Installed : {installed}")
        print(f"Updated   : {updated}")
        print(f"Skipped   : {skipped}")
        print(f"Elapsed   : {elapsed:.2f} seconds")
        if success:
            print(f"Backup    : {self.backup_root}")
            print(f"Log       : {self.log_dir}")
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
    installer = AHIPInstaller(Path(__file__).parent)
    return installer.run()


if __name__ == "__main__":
    raise SystemExit(main())
