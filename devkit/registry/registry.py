from __future__ import annotations
from pathlib import Path
import json
from datetime import datetime

class Registry:
    VERSION="1.0.0"

    def __init__(self, repo:Path):
        self.repo=repo
        self.file=repo/"devkit"/"registry"/"registry.json"
        self.file.parent.mkdir(parents=True,exist_ok=True)
        if not self.file.exists():
            self.file.write_text(json.dumps({"components":[]},indent=2),encoding="utf-8")

    def load(self):
        return json.loads(self.file.read_text(encoding="utf-8"))

    def register(self,name,category,version):
        data=self.load()
        comps=[c for c in data["components"] if c["name"]!=name]
        comps.append({
            "name":name,
            "category":category,
            "version":version,
            "installed":datetime.now().isoformat(timespec="seconds")
        })
        data["components"]=sorted(comps,key=lambda x:(x["category"],x["name"]))
        self.file.write_text(json.dumps(data,indent=2),encoding="utf-8")
        return data

    def list(self):
        return self.load()["components"]
