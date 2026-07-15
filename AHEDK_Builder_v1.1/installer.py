from __future__ import annotations

import ast
import hashlib
import json
import os
import shutil
import string
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


PACKAGE_DIR = Path(__file__).parent.resolve()
CONFIG_PATH = Path.home() / ".ahip-devkit.json"
DEVKIT_VERSION = "1.1.0"

KNOWN_REPOSITORIES = (
    Path(r"Y:\R&D\Infra-signal"),
    Path(r"\\192.168.0.147\1. AllenHammettInc\R&D\Infra-signal"),
)


@dataclass(slots=True)
class InstallRecord:
    source: str
    destination: str
    action: str
    checksum: str


def is_repository(path: Path) -> bool:
    return (
        path.exists()
        and path.is_dir()
        and (path / "app.py").is_file()
        and (path / "engines").is_dir()
        and (path / "README.md").is_file()
    )


def ancestors(path: Path) -> list[Path]:
    results: list[Path] = []
    current = path.resolve()
    while True:
        results.append(current)
        if current == current.parent:
            break
        current = current.parent
    return results


def discover_repository() -> Path:
    candidates: list[Path] = []

    if CONFIG_PATH.exists():
        try:
            config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
            saved = config.get("repository")
            if saved:
                candidates.append(Path(saved))
        except Exception:
            pass

    env_path = os.environ.get("AHIP_REPO_ROOT")
    if env_path:
        candidates.append(Path(env_path))

    candidates.extend(KNOWN_REPOSITORIES)
    candidates.extend(ancestors(Path.cwd()))
    candidates.extend(ancestors(PACKAGE_DIR))

    common_names = (
        Path("R&D") / "Infra-signal",
        Path("R&D") / "infra-signal",
        Path("GitHub") / "infra-signal",
        Path("Projects") / "infra-signal",
        Path("infra-signal"),
    )

    for letter in string.ascii_uppercase:
        drive = Path(f"{letter}:\\")
        if drive.exists():
            for relative in common_names:
                candidates.append(drive / relative)

    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate).lower()
        if key in seen:
            continue
        seen.add(key)
        if is_repository(candidate):
            return candidate.resolve()

    print("AHIP repository was not found automatically.")
    print(r"Example: Y:\R&D\Infra-signal")
    for _ in range(3):
        raw = input("Repository path: ").strip().strip('"')
        candidate = Path(raw)
        if is_repository(candidate):
            return candidate.resolve()
        print("Invalid repository path.")

    raise RuntimeError("Unable to locate the AHIP repository.")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_python(path: Path) -> None:
    ast.parse(path.read_text(encoding="utf-8"))


def main() -> int:
    manifest_path = PACKAGE_DIR / "manifest.json"
    manifest: dict[str, Any] = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )
    repository = discover_repository()

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    backup_root = repository / "backups" / f"ahedk_builder_{timestamp}"
    log_root = repository / "logs"
    backup_root.mkdir(parents=True, exist_ok=True)
    log_root.mkdir(parents=True, exist_ok=True)

    created: list[Path] = []
    replaced: list[tuple[Path, Path]] = []
    records: list[InstallRecord] = []

    print("=" * 62)
    print("AHEDK Builder v1.1 Installer")
    print("=" * 62)
    print("Repository:", repository)
    print()

    try:
        for item in manifest["files"]:
            source = PACKAGE_DIR / item["source"]
            destination = repository / item["destination"]

            if not source.is_file():
                raise RuntimeError(f"Missing payload file: {item['source']}")

            destination.parent.mkdir(parents=True, exist_ok=True)
            source_hash = sha256(source)

            if destination.exists():
                if sha256(destination) == source_hash:
                    print("[SKIP]", item["destination"])
                    records.append(
                        InstallRecord(
                            source=item["source"],
                            destination=item["destination"],
                            action="skipped",
                            checksum=source_hash,
                        )
                    )
                    continue

                backup = backup_root / item["destination"]
                backup.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(destination, backup)
                replaced.append((destination, backup))
                action = "updated"
            else:
                created.append(destination)
                action = "installed"

            shutil.copy2(source, destination)

            if destination.suffix.lower() == ".py":
                validate_python(destination)

            print("[OK]", item["destination"], f"({action})")
            records.append(
                InstallRecord(
                    source=item["source"],
                    destination=item["destination"],
                    action=action,
                    checksum=source_hash,
                )
            )

        CONFIG_PATH.write_text(
            json.dumps(
                {
                    "repository": str(repository),
                    "updated_at": datetime.now().isoformat(timespec="seconds"),
                    "devkit_version": DEVKIT_VERSION,
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        log_payload = {
            "package_name": manifest["package_name"],
            "package_version": manifest["package_version"],
            "repository": str(repository),
            "success": True,
            "records": [asdict(record) for record in records],
        }
        (log_root / f"ahedk_builder_{timestamp}.json").write_text(
            json.dumps(log_payload, indent=2),
            encoding="utf-8",
        )

        print()
        print("Installation successful.")
        return 0

    except Exception as exc:
        for path in reversed(created):
            if path.exists():
                path.unlink()

        for destination, backup in reversed(replaced):
            if backup.exists():
                shutil.copy2(backup, destination)

        print()
        print("Installation failed. Changes rolled back.")
        print("Error:", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
