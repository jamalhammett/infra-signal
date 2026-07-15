from pathlib import Path
import json
import shutil

config_path = Path.home() / ".ahip-devkit.json"
if not config_path.exists():
    raise SystemExit("AHEDK configuration not found. Install AHEDK Builder first.")

repository = Path(
    json.loads(config_path.read_text(encoding="utf-8"))["repository"]
)

package_dir = Path(__file__).parent
manifest = json.loads(
    (package_dir / "manifest.json").read_text(encoding="utf-8")
)

for item in manifest["files"]:
    source = package_dir / item["source"]
    destination = repository / item["destination"]
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
    print("[OK]", destination)

print("AHIP Command Center Core installed.")
