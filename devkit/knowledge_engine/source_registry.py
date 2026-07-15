from dataclasses import dataclass, asdict
from pathlib import Path
import json

@dataclass
class KnowledgeSource:
    name:str
    category:str
    url:str
    license:str
    confidence:float
    enabled:bool=True

class SourceRegistry:
    VERSION="1.0.0"

    def __init__(self, root:Path):
        self.file=root/"devkit"/"knowledge_engine"/"sources.json"
        self.file.parent.mkdir(parents=True,exist_ok=True)
        if not self.file.exists():
            self.file.write_text('{"sources":[]}',encoding="utf-8")

    def list(self):
        return json.loads(self.file.read_text())["sources"]

    def register(self, source:KnowledgeSource):
        data=json.loads(self.file.read_text())
        data["sources"]=[s for s in data["sources"] if s["name"]!=source.name]
        data["sources"].append(asdict(source))
        self.file.write_text(json.dumps(data,indent=2),encoding="utf-8")
