from dataclasses import dataclass, field
from typing import List


@dataclass
class Opportunity:

    # -----------------------
    # Project
    # -----------------------

    project_name: str = ""
    owner: str = ""
    developer: str = ""
    market: str = ""

    # -----------------------
    # Contractors
    # -----------------------

    general_contractor: str = ""
    electrical_contractor: str = ""
    mechanical_contractor: str = ""
    civil_contractor: str = ""

    # -----------------------
    # Construction
    # -----------------------

    construction_phase: str = ""
    buying_window_days: int = 0

    trade_packages: List[str] = field(default_factory=list)

    # -----------------------
    # Relationships
    # -----------------------

    contacts: int = 0

    relationship_score: float = 0

    coverage_score: float = 0

    influence_score: float = 0

    missing_roles: List[str] = field(default_factory=list)

    # -----------------------
    # Distributor
    # -----------------------

    distributor: str = ""

    distributor_confidence: float = 0

    # -----------------------
    # Products
    # -----------------------

    product_families: List[str] = field(default_factory=list)

    estimated_revenue: float = 0

    demand_score: float = 0

    # -----------------------
    # Executive AI
    # -----------------------

    capture_score: float = 0

    executive_priority: str = ""

    ai_summary: str = ""

    next_action: str = ""
