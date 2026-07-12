"""
Allen Hammett Intelligence Platform
Data Center Construction Knowledge Model

This module defines the relationship between:

Project phase
→ trade packages
→ buying committee
→ likely distributors
→ product demand
→ sales actions

It contains no Streamlit code and no database logic.
"""

from __future__ import annotations

from typing import Any


DATA_CENTER_CONSTRUCTION_MODEL: dict[str, dict[str, Any]] = {
    "Land Acquisition": {
        "sequence": 10,
        "typical_duration_days": 120,
        "procurement_window": "Long-range positioning",
        "trade_packages": [
            "Land Due Diligence",
            "Surveying",
            "Environmental Review",
            "Geotechnical Investigation",
        ],
        "buying_roles": [
            "Real Estate Director",
            "Site Acquisition Director",
            "Development Executive",
            "Program Executive",
        ],
        "likely_distributors": [],
        "product_families": [
            "Laser Measurement",
            "Layout and Survey Support",
            "Site Inspection Equipment",
        ],
        "sales_action": (
            "Build owner and developer relationships before contractor selection."
        ),
        "revenue_intensity": "Low",
    },

    "Entitlements": {
        "sequence": 20,
        "typical_duration_days": 180,
        "procurement_window": "Specification and preconstruction positioning",
        "trade_packages": [
            "Rezoning",
            "Site Plan",
            "Special Exception",
            "Environmental Permitting",
            "Utility Coordination",
        ],
        "buying_roles": [
            "Development Executive",
            "Preconstruction Director",
            "Design Manager",
            "Utility Program Manager",
            "Procurement Director",
        ],
        "likely_distributors": [],
        "product_families": [
            "Layout Lasers",
            "Measurement Tools",
            "Digital Layout",
            "Jobsite Planning Equipment",
        ],
        "sales_action": (
            "Position DEWALT specifications with the owner, design team, "
            "and preconstruction leadership."
        ),
        "revenue_intensity": "Low",
    },

    "Early Site Development": {
        "sequence": 30,
        "typical_duration_days": 90,
        "procurement_window": "Civil and sitework purchasing begins",
        "trade_packages": [
            "Clearing",
            "Grubbing",
            "Earthwork",
            "Erosion Control",
            "Mass Grading",
            "Temporary Access Roads",
        ],
        "buying_roles": [
            "Civil Project Manager",
            "Site Superintendent",
            "Procurement Manager",
            "Safety Manager",
            "Equipment Manager",
            "Foreman",
        ],
        "likely_distributors": [
            "White Cap",
            "Fastenal",
            "Grainger",
        ],
        "product_families": [
            "Outdoor Power Equipment",
            "Generators",
            "Pumps",
            "Rotary Lasers",
            "Jobsite Storage",
            "Cutting and Grinding",
            "Safety Equipment",
        ],
        "sales_action": (
            "Engage the civil contractor and White Cap before mass grading "
            "and field mobilization accelerate."
        ),
        "revenue_intensity": "Moderate",
    },

    "Underground Utilities": {
        "sequence": 40,
        "typical_duration_days": 120,
        "procurement_window": "Utility, conduit, and underground installation",
        "trade_packages": [
            "Stormwater",
            "Sanitary Sewer",
            "Domestic Water",
            "Underground Electrical",
            "Telecommunications Conduit",
            "Site Lighting Conduit",
        ],
        "buying_roles": [
            "Utility Project Manager",
            "Electrical Project Manager",
            "Underground Superintendent",
            "Procurement Manager",
            "Foreman",
            "Tool and Equipment Manager",
        ],
        "likely_distributors": [
            "Graybar",
            "Wesco",
            "Border States",
            "White Cap",
        ],
        "product_families": [
            "Rotary Hammers",
            "Cut-Off Saws",
            "Concrete Drilling",
            "Cordless Threading",
            "Bandsaws",
            "Cable Preparation",
            "Jobsite Storage",
            "Portable Power",
        ],
        "sales_action": (
            "Coordinate the electrical and utility contractor pursuit with "
            "Graybar, Wesco, or Border States."
        ),
        "revenue_intensity": "High",
    },

    "Foundations": {
        "sequence": 50,
        "typical_duration_days": 120,
        "procurement_window": "Concrete, reinforcing, anchoring, and forming",
        "trade_packages": [
            "Excavation",
            "Deep Foundations",
            "Formwork",
            "Rebar",
            "Concrete Placement",
            "Embedded Anchors",
            "Slab-on-Grade",
        ],
        "buying_roles": [
            "Concrete Project Executive",
            "Concrete Superintendent",
            "Procurement Manager",
            "Safety Manager",
            "Foreman",
            "Tool Crib Manager",
        ],
        "likely_distributors": [
            "White Cap",
            "Fastenal",
            "Grainger",
        ],
        "product_families": [
            "SDS Plus Rotary Hammers",
            "SDS Max Rotary Hammers",
            "Concrete Anchors",
            "Diamond Blades",
            "Cut-Off Saws",
            "Dust Extraction",
            "Rebar Cutting",
            "Cordless Impacts",
            "Jobsite Storage",
        ],
        "sales_action": (
            "Launch a concrete and anchoring package with the concrete "
            "contractor and White Cap before major pours begin."
        ),
        "revenue_intensity": "Very High",
    },

    "Structural Frame": {
        "sequence": 60,
        "typical_duration_days": 150,
        "procurement_window": "Steel erection and structural fastening",
        "trade_packages": [
            "Structural Steel",
            "Metal Deck",
            "Joists",
            "Miscellaneous Metals",
            "Structural Fastening",
        ],
        "buying_roles": [
            "Structural Project Manager",
            "Steel Superintendent",
            "Procurement Manager",
            "Foreman",
            "Safety Manager",
        ],
        "likely_distributors": [
            "Fastenal",
            "White Cap",
            "Grainger",
        ],
        "product_families": [
            "High-Torque Impacts",
            "Grinders",
            "Metal Cutting Saws",
            "Magnetic Drills",
            "Cordless Fastening",
            "FLEXVOLT",
            "Fall Protection",
            "Jobsite Storage",
        ],
        "sales_action": (
            "Target the steel erector and Fastenal before structural "
            "mobilization and fastening demand peaks."
        ),
        "revenue_intensity": "High",
    },

    "Building Envelope": {
        "sequence": 70,
        "typical_duration_days": 150,
        "procurement_window": "Roofing, wall systems, and weather enclosure",
        "trade_packages": [
            "Roofing",
            "Insulated Metal Panels",
            "Exterior Wall Systems",
            "Waterproofing",
            "Doors and Openings",
        ],
        "buying_roles": [
            "Envelope Project Manager",
            "Roofing Superintendent",
            "Procurement Manager",
            "Safety Manager",
            "Foreman",
        ],
        "likely_distributors": [
            "White Cap",
            "Fastenal",
            "Grainger",
        ],
        "product_families": [
            "Cordless Fastening",
            "Cutting Tools",
            "Sealant Tools",
            "Roofing Tools",
            "Lasers",
            "Fall Protection",
            "Jobsite Storage",
        ],
        "sales_action": (
            "Engage envelope and roofing contractors before the building "
            "is scheduled to become weather-tight."
        ),
        "revenue_intensity": "Moderate",
    },

    "Electrical Rough-In": {
        "sequence": 80,
        "typical_duration_days": 240,
        "procurement_window": "Major electrical field deployment",
        "trade_packages": [
            "Electrical Rooms",
            "Cable Tray",
            "Conduit",
            "Grounding",
            "Busway",
            "Switchgear Installation",
            "Generator Connections",
            "UPS Installation",
            "Lighting",
        ],
        "buying_roles": [
            "Electrical Project Executive",
            "Electrical Project Manager",
            "Electrical Superintendent",
            "Procurement Director",
            "Warehouse Manager",
            "Tool Crib Manager",
            "Foreman",
            "Safety Manager",
        ],
        "likely_distributors": [
            "Graybar",
            "Wesco",
            "Border States",
            "Rexel",
        ],
        "product_families": [
            "FLEXVOLT",
            "POWERSTACK",
            "Cordless Knockout Tools",
            "Bandsaws",
            "Cable Cutters",
            "Cable Crimpers",
            "Threading Tools",
            "Rotary Hammers",
            "Lasers",
            "Lighting",
            "TOUGHSYSTEM Storage",
        ],
        "sales_action": (
            "Coordinate a joint electrical-contractor and distributor pursuit "
            "before cable tray, conduit, and equipment-room labor peaks."
        ),
        "revenue_intensity": "Extreme",
    },

    "Mechanical Rough-In": {
        "sequence": 90,
        "typical_duration_days": 240,
        "procurement_window": "HVAC, piping, and fabrication deployment",
        "trade_packages": [
            "Chilled Water Piping",
            "Mechanical Piping",
            "HVAC",
            "Ductwork",
            "Equipment Setting",
            "Pipe Supports",
            "Prefabrication",
        ],
        "buying_roles": [
            "Mechanical Project Executive",
            "Mechanical Project Manager",
            "Mechanical Superintendent",
            "Fabrication Manager",
            "Procurement Director",
            "Warehouse Manager",
            "Foreman",
        ],
        "likely_distributors": [
            "Ferguson",
            "Grainger",
            "Wesco",
            "Fastenal",
        ],
        "product_families": [
            "Pipe Cutting",
            "Bandsaws",
            "Threading Tools",
            "Press Tools",
            "Cordless Grinders",
            "Rotary Hammers",
            "Lasers",
            "Material Handling",
            "TOUGHSYSTEM Storage",
            "FLEXVOLT",
        ],
        "sales_action": (
            "Target the mechanical contractor, fabrication manager, and "
            "distribution partner before prefabrication and field installation peak."
        ),
        "revenue_intensity": "Extreme",
    },

    "Interior Buildout": {
        "sequence": 100,
        "typical_duration_days": 150,
        "procurement_window": "Interior systems, controls, and finish trades",
        "trade_packages": [
            "Interior Framing",
            "Ceilings",
            "Firestopping",
            "Security",
            "Low Voltage",
            "Controls",
            "Life Safety",
        ],
        "buying_roles": [
            "Interior Superintendent",
            "Low Voltage Project Manager",
            "Controls Project Manager",
            "Procurement Manager",
            "Foreman",
        ],
        "likely_distributors": [
            "Graybar",
            "Wesco",
            "Fastenal",
            "Grainger",
        ],
        "product_families": [
            "Cordless Drills",
            "Impacts",
            "Lasers",
            "Fastening",
            "Cable Tools",
            "Inspection Lighting",
            "Storage",
            "Firestop Installation Tools",
        ],
        "sales_action": (
            "Expand into low-voltage, controls, security, and interior trade packages."
        ),
        "revenue_intensity": "High",
    },

    "Commissioning": {
        "sequence": 110,
        "typical_duration_days": 120,
        "procurement_window": "Testing, verification, punch-list, and turnover",
        "trade_packages": [
            "Electrical Commissioning",
            "Mechanical Commissioning",
            "Controls Verification",
            "Integrated Systems Testing",
            "Punch List",
            "Owner Training",
        ],
        "buying_roles": [
            "Commissioning Manager",
            "Quality Manager",
            "Operations Director",
            "Facility Manager",
            "Electrical Superintendent",
            "Mechanical Superintendent",
        ],
        "likely_distributors": [
            "Grainger",
            "Fastenal",
            "Graybar",
            "Wesco",
        ],
        "product_families": [
            "Inspection Lighting",
            "Diagnostic Tools",
            "Hand Tools",
            "Cordless Drills",
            "Compact Impacts",
            "Portable Storage",
            "Maintenance Kits",
        ],
        "sales_action": (
            "Convert construction users into operations and maintenance accounts "
            "during turnover."
        ),
        "revenue_intensity": "Moderate",
    },

    "Operations": {
        "sequence": 120,
        "typical_duration_days": 3650,
        "procurement_window": "Recurring MRO and facility operations",
        "trade_packages": [
            "Preventive Maintenance",
            "Facility Repairs",
            "Electrical Maintenance",
            "Mechanical Maintenance",
            "Security Operations",
            "Expansion and Retrofit",
        ],
        "buying_roles": [
            "Facility Director",
            "Data Center Operations Manager",
            "Critical Facilities Manager",
            "Maintenance Manager",
            "MRO Procurement Manager",
            "Warehouse Manager",
        ],
        "likely_distributors": [
            "Grainger",
            "Fastenal",
            "Graybar",
            "Wesco",
            "HD Supply",
        ],
        "product_families": [
            "MRO Tool Kits",
            "POWERSTACK",
            "Compact Tools",
            "Inspection Lighting",
            "Hand Tools",
            "Storage",
            "Outdoor Power Equipment",
            "Service and Repair Programs",
        ],
        "sales_action": (
            "Establish a recurring MRO program and convert construction purchases "
            "into long-term facility-standard tool platforms."
        ),
        "revenue_intensity": "Recurring",
    },
}


PHASE_ALIASES: dict[str, list[str]] = {
    "Land Acquisition": [
        "land acquisition",
        "property purchase",
        "site acquisition",
        "due diligence",
    ],
    "Entitlements": [
        "rezoning",
        "entitlement",
        "site plan",
        "special exception",
        "planning review",
        "permit application",
    ],
    "Early Site Development": [
        "clearing",
        "grading",
        "earthwork",
        "sitework",
        "site work",
        "erosion control",
    ],
    "Underground Utilities": [
        "underground utility",
        "stormwater",
        "sanitary sewer",
        "water line",
        "underground electrical",
        "duct bank",
        "conduit",
    ],
    "Foundations": [
        "foundation",
        "footing",
        "concrete",
        "rebar",
        "slab",
        "formwork",
        "pile",
    ],
    "Structural Frame": [
        "structural steel",
        "steel erection",
        "metal deck",
        "structural frame",
        "joist",
    ],
    "Building Envelope": [
        "building envelope",
        "roofing",
        "exterior wall",
        "weather tight",
        "waterproofing",
        "metal panel",
    ],
    "Electrical Rough-In": [
        "electrical rough",
        "switchgear",
        "busway",
        "cable tray",
        "grounding",
        "electrical room",
        "generator",
        "ups",
        "substation",
    ],
    "Mechanical Rough-In": [
        "mechanical rough",
        "hvac",
        "chilled water",
        "mechanical piping",
        "ductwork",
        "fabrication",
    ],
    "Interior Buildout": [
        "interior buildout",
        "interior build out",
        "low voltage",
        "security",
        "controls",
        "fire alarm",
        "life safety",
        "firestopping",
    ],
    "Commissioning": [
        "commissioning",
        "integrated systems testing",
        "punch list",
        "turnover",
        "owner training",
    ],
    "Operations": [
        "operations",
        "operational",
        "facility maintenance",
        "mro",
        "preventive maintenance",
        "retrofit",
    ],
}


REVENUE_INTENSITY_SCORES: dict[str, int] = {
    "Low": 20,
    "Moderate": 45,
    "High": 65,
    "Very High": 80,
    "Extreme": 100,
    "Recurring": 85,
}


def get_phase_names() -> list[str]:
    """Return phases in construction-sequence order."""

    return sorted(
        DATA_CENTER_CONSTRUCTION_MODEL.keys(),
        key=lambda phase: DATA_CENTER_CONSTRUCTION_MODEL[phase]["sequence"],
    )


def get_phase_definition(phase: str) -> dict[str, Any]:
    """Return a phase definition or an empty dictionary."""

    return DATA_CENTER_CONSTRUCTION_MODEL.get(phase, {})


def get_trade_packages(phase: str) -> list[str]:
    """Return trade packages for a construction phase."""

    return list(
        DATA_CENTER_CONSTRUCTION_MODEL
        .get(phase, {})
        .get("trade_packages", [])
    )


def get_buying_roles(phase: str) -> list[str]:
    """Return target buying roles for a construction phase."""

    return list(
        DATA_CENTER_CONSTRUCTION_MODEL
        .get(phase, {})
        .get("buying_roles", [])
    )


def get_distributors(phase: str) -> list[str]:
    """Return likely distributors for a construction phase."""

    return list(
        DATA_CENTER_CONSTRUCTION_MODEL
        .get(phase, {})
        .get("likely_distributors", [])
    )


def get_product_families(phase: str) -> list[str]:
    """Return recommended product families for a construction phase."""

    return list(
        DATA_CENTER_CONSTRUCTION_MODEL
        .get(phase, {})
        .get("product_families", [])
    )


def get_sales_action(phase: str) -> str:
    """Return the recommended sales action for a phase."""

    return str(
        DATA_CENTER_CONSTRUCTION_MODEL
        .get(phase, {})
        .get("sales_action", "Monitor project development.")
    )


def get_revenue_intensity_score(phase: str) -> int:
    """Convert the phase revenue intensity into a numeric score."""

    intensity = (
        DATA_CENTER_CONSTRUCTION_MODEL
        .get(phase, {})
        .get("revenue_intensity", "Low")
    )

    return REVENUE_INTENSITY_SCORES.get(str(intensity), 0)


def infer_phase_from_text(text: str) -> tuple[str, int, list[str]]:
    """
    Infer a construction phase from text.

    Returns:
        phase
        confidence
        matched terms
    """

    normalized_text = str(text or "").lower()
    matches: list[tuple[str, list[str]]] = []

    for phase, aliases in PHASE_ALIASES.items():
        matched_terms = [
            alias
            for alias in aliases
            if alias in normalized_text
        ]

        if matched_terms:
            matches.append((phase, matched_terms))

    if not matches:
        return "Entitlements", 25, []

    matches.sort(
        key=lambda item: (
            len(item[1]),
            DATA_CENTER_CONSTRUCTION_MODEL[item[0]]["sequence"],
        ),
        reverse=True,
    )

    phase, matched_terms = matches[0]

    confidence = min(
        50 + (len(matched_terms) * 10),
        95,
    )

    return phase, confidence, matched_terms


def build_phase_summary(phase: str) -> dict[str, Any]:
    """Build a normalized phase intelligence record."""

    definition = get_phase_definition(phase)

    if not definition:
        return {
            "phase": phase,
            "sequence": 0,
            "procurement_window": "Unknown",
            "trade_packages": [],
            "buying_roles": [],
            "likely_distributors": [],
            "product_families": [],
            "sales_action": "Monitor project development.",
            "revenue_intensity": "Unknown",
            "revenue_intensity_score": 0,
        }

    return {
        "phase": phase,
        "sequence": definition["sequence"],
        "typical_duration_days": definition["typical_duration_days"],
        "procurement_window": definition["procurement_window"],
        "trade_packages": list(definition["trade_packages"]),
        "buying_roles": list(definition["buying_roles"]),
        "likely_distributors": list(definition["likely_distributors"]),
        "product_families": list(definition["product_families"]),
        "sales_action": definition["sales_action"],
        "revenue_intensity": definition["revenue_intensity"],
        "revenue_intensity_score": get_revenue_intensity_score(phase),
    }
