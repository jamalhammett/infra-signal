from devkit.coe.cyber.knowledge import CyberKnowledge

class CyberEnabledMixin:
    """Mixin that gives Digital Employees access to the Cyber CoE."""

    def __init__(self):
        self.cyber_knowledge = CyberKnowledge()

    def available_domains(self):
        return self.cyber_knowledge.domains()

    def knowledge_topics(self, domain:str):
        return self.cyber_knowledge.topics(domain)
