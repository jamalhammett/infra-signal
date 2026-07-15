from dataclasses import dataclass, field
from typing import List

@dataclass
class DigitalEmployee:
    name:str
    title:str
    department:str
    version:str="1.0.0"
    status:str="Active"
    skills:List[str]=field(default_factory=list)
    kpis:List[str]=field(default_factory=list)

    def profile(self):
        return {
            "name":self.name,
            "title":self.title,
            "department":self.department,
            "version":self.version,
            "status":self.status,
            "skills":self.skills,
            "kpis":self.kpis
        }
