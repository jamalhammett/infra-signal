from .framework import DigitalEmployee
from .cyber_coe_integration import CyberEnabledMixin

class AICISO(CyberEnabledMixin, DigitalEmployee):
    VERSION="2.0.0"

    def __init__(self):
        DigitalEmployee.__init__(
            self,
            name="CISO",
            title="Chief Information Security Officer",
            department="Cybersecurity"
        )
        CyberEnabledMixin.__init__(self)

    def executive_brief(self):
        return {
            "employee": self.name,
            "domains": self.available_domains(),
            "message":"Cyber Center of Excellence connected."
        }
