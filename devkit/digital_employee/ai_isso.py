from .framework import DigitalEmployee

class AIISSO(DigitalEmployee):
    VERSION="1.0.0"

    def __init__(self):
        super().__init__(
            name="ISSO",
            title="Information System Security Officer",
            department="Cybersecurity Operations",
            skills=[
                "RMF Package Management",
                "JCAM",
                "XACTA",
                "POA&M Management",
                "Continuous Monitoring",
                "Assessment Coordination"
            ],
            kpis=[
                "Package Quality",
                "POA&M Timeliness",
                "Control Compliance",
                "Audit Readiness"
            ]
        )

    def compliance_brief(self):
        return {
            "employee": self.name,
            "message":"ISSO workspace ready. Review JCAM, XACTA, POA&Ms, findings, and assessment tasks."
        }
