from pathlib import Path
import json

class KnowledgeIngestionEngine:
    VERSION="1.0.0"

    def __init__(self, root:Path):
        self.root=root
        self.sources=root/"devkit"/"knowledge_engine"/"sources.json"
        self.staging=root/"devkit"/"knowledge_engine"/"staging"
        self.staging.mkdir(parents=True,exist_ok=True)

    def list_sources(self):
        return json.loads(self.sources.read_text(encoding="utf-8")).get("sources",[])

    def stage(self,name:str,content:dict):
        out=self.staging/f"{name}.json"
        out.write_text(json.dumps(content,indent=2),encoding="utf-8")
        return out
