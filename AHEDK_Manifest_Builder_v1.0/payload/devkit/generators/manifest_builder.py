import json
from pathlib import Path

class ManifestBuilder:
    VERSION="1.0.0"

    def build(self, package_name:str, version:str, files:list, output:Path):
        manifest={
            "package_name":package_name,
            "package_version":version,
            "files":files
        }
        output.write_text(json.dumps(manifest,indent=2),encoding="utf-8")
        return output
