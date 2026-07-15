from .catalog import CYBER_COE
class CyberKnowledge:
    VERSION="1.0.0"
    def domains(self):
        return list(CYBER_COE.keys())
    def topics(self,domain):
        return CYBER_COE.get(domain,[])
