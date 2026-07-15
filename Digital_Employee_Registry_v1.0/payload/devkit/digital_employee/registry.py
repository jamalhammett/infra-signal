import json
from pathlib import Path

class DigitalEmployeeRegistry:
    VERSION="1.0.0"

    def __init__(self, repo:Path):
        self.file=repo/"devkit"/"digital_employee"/"employees.json"
        self.file.parent.mkdir(parents=True,exist_ok=True)
        if not self.file.exists():
            self.file.write_text('{"employees":[]}',encoding="utf-8")

    def _load(self):
        return json.loads(self.file.read_text(encoding="utf-8"))

    def register(self, profile:dict):
        data=self._load()
        data["employees"]=[e for e in data["employees"] if e["name"]!=profile["name"]]
        data["employees"].append(profile)
        self.file.write_text(json.dumps(data,indent=2),encoding="utf-8")
        return profile

    def list(self):
        return self._load()["employees"]
