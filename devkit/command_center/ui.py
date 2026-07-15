from pathlib import Path
import json

class CommandCenterUI:
    VERSION="1.0.0"

    def __init__(self, repo:Path):
        self.repo=repo

    def dashboard(self):
        reg=self.repo/"devkit"/"registry"/"registry.json"
        if reg.exists():
            data=json.loads(reg.read_text(encoding="utf-8"))
        else:
            data={"components":[]}

        print("="*60)
        print("AHIP COMMAND CENTER")
        print("="*60)
        print(f"Installed Components: {len(data['components'])}")
        print()

        for c in data["components"]:
            print(f"[{c['category']}] {c['name']} v{c['version']}")
