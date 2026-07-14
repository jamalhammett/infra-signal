"""
Allen Hammett Intelligence Platform
Demand Intelligence Engine

Purpose
-------
Convert data-center construction signals into actionable sales intelligence:

- construction phase
- phase confidence
- buying window
- procurement urgency
- active trade packages
- required buying roles
- missing buying roles
- likely distributor
- product demand
- estimated revenue
- demand score
- next best sales action

This module contains no Streamlit UI and no database connection logic.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Iterable

import pandas as pd


# Supports either Knowledge/ or knowledge/ in GitHub.
# Linux and Streamlit Cloud are case-sensitive.
try:
    from Knowledge.data_center_construction import (
        build_phase_summary,
        get_phase_names,
        infer_phase_from_text,
    )
except ImportError:
    from knowledge.data_center_construction import (
        build_phase_summary,
        get_phase_names,
        infer_phase_from_text,
    )


@dataclass
class DemandIntelligence:
    """Unified demand-intelligence result for one project."""

    project_name: str = ""
    owner: str = ""
    developer: str = ""
    market: str = ""

    construction_phase: str = ""
    phase_confidence: int = 0
    matched_phase_terms: list[str] = field(default_factory=list)

    procurement_window: str = ""
    buying_window_days: int = 0
    procurement_urgency: str = "Monitor"
    procurement_clock_status: str = "MONITOR"

    trade_packages: list[str] = field(default_factory=list)
    buying_roles: list[str] = field(default_factory=list)
    existing_relationship_roles: list[str] = field(default_factory=list)
    missing_buying_roles: list[str] = field(default_factory=list)

    likely_distributors: list[str] = field(default_factory=list)
    recommended_distributor: str = "Unknown"
    distributor_confidence: float = 0

    product_families: list[str] = field(default_factory=list)
    product_demand_scores: dict[str, float] = field(default_factory=dict)
    top_products: list[str] = field(default_factory=list)

    revenue_intensity: str = ""
    revenue_intensity_score: float = 0
    estimated_revenue: float = 0
    demand_score: float = 0

    recommended_sales_action: str = ""
    next_best_action: str = ""
    explanation: str = ""


class DemandEngine:
    """
    Produces demand intelligence for individual projects and portfolios.

    Parameters
    ----------
    project_df:
        Project records. No database connection is performed here.

    relationship_df:
        Relationship records used to identify existing and missing
        buying-committee roles.

    distributor_engine:
        Optional DistributorEngine instance. If supplied, this engine will
        use its recommendations before falling back to the knowledge model.
    """

    BUYING_WINDOW_DAYS = {
        "Land Acquisition": 180,
        "Entitlements": 120,
        "Early Site Development": 60,
        "Underground Utilities": 45,
        "Foundations": 30,
        "Structural Frame": 30,
        "Building Envelope": 30,
        "Electrical Rough-In": 14,
        "Mechanical Rough-In": 14,
        "Interior Buildout": 21,
        "Commissioning": 14,
        "Operations": 30,
    }

    PHASE_SPEND_MULTIPLIERS = {
        "Land Acquisition": 0.05,
        "Entitlements": 0.08,
        "Early Site Development": 0.35,
        "Underground Utilities": 0.60,
        "Foundations": 0.85,
        "Structural Frame": 0.70,
        "Building Envelope": 0.45,
        "Electrical Rough-In": 1.00,
        "Mechanical Rough-In": 0.95,
        "Interior Buildout": 0.60,
        "Commissioning": 0.35,
        "Operations": 0.55,
    }

    PRODUCT_BASE_SCORES = {
        "FLEXVOLT": 95,
        "POWERSTACK": 92,
        "20V MAX": 88,
        "TOUGHSYSTEM Storage": 88,
        "Jobsite Storage": 85,
        "Portable Storage": 82,
        "Rotary Hammers": 90,
        "SDS Plus Rotary Hammers": 90,
        "SDS Max Rotary Hammers": 95,
        "Cordless Knockout Tools": 94,
        "Bandsaws": 89,
        "Cable Cutters": 91,
        "Cable Crimpers": 91,
        "Threading Tools": 88,
        "Press Tools": 88,
        "Lasers": 85,
        "Layout Lasers": 86,
        "Rotary Lasers": 87,
        "Laser Measurement": 82,
        "Concrete Anchors": 94,
        "Diamond Blades": 90,
        "Cut-Off Saws": 89,
        "Dust Extraction": 86,
        "Rebar Cutting": 86,
        "Cordless Impacts": 88,
        "High-Torque Impacts": 92,
        "Cordless Fastening": 86,
        "Grinders": 87,
        "Metal Cutting Saws": 88,
        "Magnetic Drills": 84,
        "Inspection Lighting": 82,
        "Lighting": 79,
        "Hand Tools": 75,
        "MRO Tool Kits": 83,
        "Maintenance Kits": 81,
        "Outdoor Power Equipment": 78,
        "Generators": 82,
        "Portable Power": 84,
        "Pumps": 79,
        "Safety Equipment": 74,
        "Fall Protection": 82,
        "Material Handling": 79,
        "Diagnostic Tools": 82,
        "Service and Repair Programs": 80,
    }

    ROLE_ALIASES = {
        "procurement": "Procurement",
        "purchasing": "Procurement",
        "buyer": "Procurement",
        "sourcing": "Procurement",
        "warehouse": "Procurement",
        "tool crib": "Procurement",
        "project executive": "Construction",
        "project manager": "Construction",
        "superintendent": "Construction",
        "foreman": "Construction",
        "construction": "Construction",
        "operations": "Operations",
        "facility": "Operations",
        "facilities": "Operations",
        "maintenance": "Operations",
        "mro": "Operations",
        "electrical": "Utility",
        "utility": "Utility",
        "power": "Utility",
        "transmission": "Utility",
        "engineering": "Engineering",
        "engineer": "Engineering",
        "design": "Engineering",
        "technical": "Engineering",
        "commissioning": "Engineering",
        "quality": "Engineering",
        "development": "Owner / Developer",
        "developer": "Owner / Developer",
        "real estate": "Owner / Developer",
        "site acquisition": "Owner / Developer",
        "program executive": "Owner / Developer",
        "safety": "Safety",
    }

    def __init__(
        self,
        project_df: pd.DataFrame | None = None,
        relationship_df: pd.DataFrame | None = None,
        distributor_engine: Any | None = None,
    ) -> None:
        self.projects = (
            project_df.copy()
            if isinstance(project_df, pd.DataFrame)
            else pd.DataFrame()
        )

        self.relationships = (
            relationship_df.copy()
            if isinstance(relationship_df, pd.DataFrame)
            else pd.DataFrame()
        )

        self.distributor_engine = distributor_engine

    # =========================================================
    # VALUE HELPERS
    # =========================================================

    @staticmethod
    def _clean(value: Any, fallback: str = "") -> str:
        if value is None:
            return fallback

        try:
            if pd.isna(value):
                return fallback
        except (TypeError, ValueError):
            pass

        text = str(value).strip()

        if text.lower() in {
            "",
            "nan",
            "none",
            "null",
            "n/a",
            "unknown",
        }:
            return fallback

        return text

    @staticmethod
    def _number(value: Any, default: float = 0) -> float:
        try:
            number = pd.to_numeric(value, errors="coerce")

            if pd.isna(number):
                return default

            return float(number)

        except (TypeError, ValueError):
            return default

    @classmethod
    def _first_value(
        cls,
        row: pd.Series | dict[str, Any],
        columns: Iterable[str],
        fallback: str = "",
    ) -> str:
        for column in columns:
            try:
                value = row.get(column)
            except AttributeError:
                value = None

            cleaned = cls._clean(value)

            if cleaned:
                return cleaned

        return fallback

    # =========================================================
    # PROJECT RECORD
    # =========================================================

    def project_record(self, project_name: str) -> pd.DataFrame:
        if self.projects.empty:
            return pd.DataFrame()

        candidate_columns = [
            "canonical_project_name",
            "project_name",
            "name",
        ]

        target = self._clean(project_name).lower()

        if not target:
            return pd.DataFrame()

        for column in candidate_columns:
            if column not in self.projects.columns:
                continue

            mask = (
                self.projects[column]
                .fillna("")
                .astype(str)
                .str.strip()
                .str.lower()
                == target
            )

            result = self.projects[mask].copy()

            if not result.empty:
                return result

        return pd.DataFrame()

    def build_project_text(
        self,
        row: pd.Series | dict[str, Any],
    ) -> str:
        fields = [
            "canonical_project_name",
            "project_name",
            "project_stage",
            "capture_stage",
            "construction_phase",
            "infrastructure_type",
            "permit_description",
            "raw_text",
            "strategic_notes",
            "source_name",
            "utility_dependency",
            "utility_provider",
            "general_contractor",
            "electrical_contractor",
            "mechanical_contractor",
            "civil_contractor",
        ]

        values: list[str] = []

        for field_name in fields:
            try:
                value = row.get(field_name)
            except AttributeError:
                value = None

            cleaned = self._clean(value)

            if cleaned:
                values.append(cleaned)

        return " ".join(values)

    # =========================================================
    # PHASE DETECTION
    # =========================================================

    def infer_phase(
        self,
        row: pd.Series | dict[str, Any],
    ) -> tuple[str, int, list[str]]:
        explicit_phase = self._first_value(
            row,
            [
                "construction_phase",
                "project_phase",
            ],
        )

        valid_phases = get_phase_names()

        if explicit_phase in valid_phases:
            return explicit_phase, 98, [explicit_phase.lower()]

        project_text = self.build_project_text(row)

        return infer_phase_from_text(project_text)

    # =========================================================
    # PROCUREMENT CLOCK
    # =========================================================

    def buying_window_days(self, phase: str) -> int:
        return self.BUYING_WINDOW_DAYS.get(phase, 60)

    @staticmethod
    def procurement_urgency(days: int) -> str:
        if days <= 14:
            return "Immediate"
        if days <= 30:
            return "High"
        if days <= 60:
            return "Near Term"
        if days <= 120:
            return "Positioning"
        return "Long Range"

    @staticmethod
    def procurement_clock_status(days: int) -> str:
        if days <= 14:
            return "SELL NOW"
        if days <= 30:
            return "ENGAGE NOW"
        if days <= 60:
            return "POSITION NOW"
        if days <= 120:
            return "BUILD COVERAGE"
        return "MONITOR"

    # =========================================================
    # RELATIONSHIP COVERAGE
    # =========================================================

    @classmethod
    def normalize_role(cls, role: Any) -> str:
        text = cls._clean(role).lower()

        if not text:
            return ""

        for keyword, canonical_role in cls.ROLE_ALIASES.items():
            if keyword in text:
                return canonical_role

        return cls._clean(role)

    def existing_relationship_roles(
        self,
        project_name: str,
        owner: str = "",
        developer: str = "",
    ) -> set[str]:
        if self.relationships.empty:
            return set()

        if "relationship_role" not in self.relationships.columns:
            return set()

        df = self.relationships.copy()
        masks: list[pd.Series] = []

        if (
            project_name
            and "canonical_project_name" in df.columns
        ):
            masks.append(
                df["canonical_project_name"]
                .fillna("")
                .astype(str)
                .str.strip()
                .str.lower()
                == project_name.strip().lower()
            )

        company_column = None

        for column in [
            "canonical_company",
            "company",
            "company_name",
        ]:
            if column in df.columns:
                company_column = column
                break

        if company_column:
            for company in [owner, developer]:
                cleaned_company = self._clean(company)

                if cleaned_company:
                    masks.append(
                        df[company_column]
                        .fillna("")
                        .astype(str)
                        .str.strip()
                        .str.lower()
                        == cleaned_company.lower()
                    )

        if not masks:
            return set()

        combined_mask = masks[0]

        for mask in masks[1:]:
            combined_mask = combined_mask | mask

        contacts = df[combined_mask]

        roles = {
            self.normalize_role(role)
            for role in contacts["relationship_role"]
            .dropna()
            .astype(str)
            .tolist()
        }

        return {role for role in roles if role}

    def missing_buying_roles(
        self,
        required_roles: list[str],
        existing_roles: set[str],
    ) -> list[str]:
        normalized_existing = {
            self.normalize_role(role)
            for role in existing_roles
        }

        missing: list[str] = []

        for role in required_roles:
            normalized_required = self.normalize_role(role)

            if normalized_required not in normalized_existing:
                missing.append(role)

        return list(dict.fromkeys(missing))

    # =========================================================
    # DISTRIBUTOR RECOMMENDATION
    # =========================================================

    def distributor_recommendation(
        self,
        project_name: str,
        row: pd.Series | dict[str, Any],
        phase: str,
        knowledge_distributors: list[str],
    ) -> tuple[str, float]:
        if self.distributor_engine is not None:
            try:
                recommendations = self.distributor_engine.recommend(
                    project_name
                )

                if (
                    isinstance(recommendations, pd.DataFrame)
                    and not recommendations.empty
                ):
                    distributor = self._clean(
                        recommendations.iloc[0].get("distributor"),
                        "Unknown",
                    )

                    confidence = self._number(
                        recommendations.iloc[0].get("confidence"),
                        0,
                    )

                    return distributor, confidence

            except Exception:
                pass

            try:
                recommendations = (
                    self.distributor_engine.recommend_from_values(
                        general_contractor=self._first_value(
                            row,
                            [
                                "general_contractor",
                                "gc",
                                "prime_contractor",
                            ],
                        ),
                        electrical_contractor=self._first_value(
                            row,
                            [
                                "electrical_contractor",
                                "mep_contractor",
                            ],
                        ),
                        mechanical_contractor=self._first_value(
                            row,
                            [
                                "mechanical_contractor",
                                "mep_contractor",
                            ],
                        ),
                        market=self._first_value(
                            row,
                            [
                                "market_cluster",
                                "market",
                                "county",
                                "state",
                            ],
                        ),
                        project_phase=phase,
                    )
                )

                if (
                    isinstance(recommendations, pd.DataFrame)
                    and not recommendations.empty
                ):
                    return (
                        self._clean(
                            recommendations.iloc[0].get("distributor"),
                            "Unknown",
                        ),
                        self._number(
                            recommendations.iloc[0].get("confidence"),
                            0,
                        ),
                    )

            except Exception:
                pass

        explicit_distributor = self._first_value(
            row,
            [
                "recommended_distributor",
                "distributor",
                "preferred_distributor",
            ],
        )

        if explicit_distributor:
            return explicit_distributor, 75

        if knowledge_distributors:
            return knowledge_distributors[0], 55

        return "Unknown", 0

    # =========================================================
    # PRODUCT DEMAND FORECAST
    # =========================================================

    def product_demand_scores(
        self,
        product_families: list[str],
        phase_confidence: int,
        revenue_intensity_score: float,
        estimated_power_mw: float,
        urgency: str,
    ) -> dict[str, float]:
        scores: dict[str, float] = {}

        scale_bonus = min(
            max(estimated_power_mw, 0) / 25,
            10,
        )

        urgency_bonus = {
            "Immediate": 10,
            "High": 7,
            "Near Term": 4,
            "Positioning": 2,
            "Long Range": 0,
        }.get(urgency, 0)

        for product in product_families:
            base_score = self.PRODUCT_BASE_SCORES.get(
                product,
                72,
            )

            score = (
                base_score * 0.55
                + phase_confidence * 0.20
                + revenue_intensity_score * 0.15
                + scale_bonus
                + urgency_bonus
            )

            scores[product] = round(
                min(max(score, 0), 100),
                1,
            )

        return dict(
            sorted(
                scores.items(),
                key=lambda item: item[1],
                reverse=True,
            )
        )

    # =========================================================
    # DEMAND AND REVENUE SCORING
    # =========================================================

    def demand_score(
        self,
        phase_confidence: float,
        revenue_intensity_score: float,
        buying_window_days: int,
        estimated_power_mw: float,
        contractor_known: bool,
        distributor_known: bool,
        relationship_gap_count: int,
    ) -> float:
        urgency_score = max(
            0,
            100 - buying_window_days,
        )

        scale_score = min(
            max(estimated_power_mw, 0) / 5,
            100,
        )

        relationship_readiness = max(
            0,
            100 - relationship_gap_count * 10,
        )

        score = (
            phase_confidence * 0.20
            + revenue_intensity_score * 0.25
            + urgency_score * 0.20
            + scale_score * 0.15
            + relationship_readiness * 0.10
            + (7 if contractor_known else 0)
            + (3 if distributor_known else 0)
        )

        return round(
            min(max(score, 0), 100),
            1,
        )

    def estimate_revenue(
        self,
        phase: str,
        revenue_intensity_score: float,
        estimated_power_mw: float,
        contractor_known: bool,
        distributor_known: bool,
        relationship_gap_count: int,
    ) -> float:
        phase_multiplier = self.PHASE_SPEND_MULTIPLIERS.get(
            phase,
            0.25,
        )

        base_revenue = revenue_intensity_score * 7500

        power_revenue = (
            min(max(estimated_power_mw, 0), 1000)
            * 2500
        )

        contractor_bonus = 200000 if contractor_known else 0
        distributor_bonus = 100000 if distributor_known else 0

        readiness_factor = max(
            0.65,
            1 - relationship_gap_count * 0.025,
        )

        estimate = (
            base_revenue
            + power_revenue
            + contractor_bonus
            + distributor_bonus
        ) * phase_multiplier * readiness_factor

        return round(max(estimate, 0), 2)

    # =========================================================
    # SALES PLAYBOOK
    # =========================================================

    @staticmethod
    def next_best_action(
        urgency: str,
        recommended_distributor: str,
        missing_roles: list[str],
        product_scores: dict[str, float],
        knowledge_action: str,
    ) -> str:
        actions: list[str] = []

        if urgency == "Immediate":
            actions.append(
                "Escalate this opportunity for action within 14 days."
            )
        elif urgency == "High":
            actions.append(
                "Begin contractor and distributor engagement immediately."
            )
        elif urgency == "Near Term":
            actions.append(
                "Position the account before the next trade-package release."
            )
        elif urgency == "Positioning":
            actions.append(
                "Build specifications and buying-committee coverage."
            )
        else:
            actions.append(
                "Maintain long-range account and project monitoring."
            )

        if (
            recommended_distributor
            and recommended_distributor != "Unknown"
        ):
            actions.append(
                f"Coordinate a joint pursuit with "
                f"{recommended_distributor}."
            )
        else:
            actions.append(
                "Validate the most likely distributor channel."
            )

        if missing_roles:
            actions.append(
                "Identify missing buying roles: "
                + ", ".join(missing_roles[:4])
                + "."
            )

        top_products = list(product_scores.keys())[:4]

        if top_products:
            actions.append(
                "Lead with "
                + ", ".join(top_products)
                + "."
            )

        if knowledge_action:
            actions.append(knowledge_action)

        return " ".join(actions)

    @staticmethod
    def build_explanation(
        result: DemandIntelligence,
    ) -> str:
        product_text = (
            ", ".join(result.top_products[:5])
            if result.top_products
            else "no product families confirmed"
        )

        missing_text = (
            ", ".join(result.missing_buying_roles[:4])
            if result.missing_buying_roles
            else "no major buying-role gaps identified"
        )

        distributor_text = (
            f"{result.recommended_distributor} at "
            f"{result.distributor_confidence:.0f}% confidence"
            if result.recommended_distributor != "Unknown"
            else "no confirmed distributor"
        )

        return (
            f"{result.project_name} is assessed in the "
            f"{result.construction_phase} phase with "
            f"{result.phase_confidence}% confidence. "
            f"The buying window is approximately "
            f"{result.buying_window_days} days and the Procurement Clock "
            f"is marked {result.procurement_clock_status}. "
            f"The recommended distributor channel is {distributor_text}. "
            f"Highest-priority demand includes {product_text}. "
            f"Buying-committee gaps include {missing_text}. "
            f"Estimated tool and equipment revenue is "
            f"${result.estimated_revenue:,.0f}, with a demand score of "
            f"{result.demand_score:.1f}/100."
        )

    # =========================================================
    # BUILD ONE PROJECT
    # =========================================================

    def build_from_row(
        self,
        row: pd.Series | dict[str, Any],
    ) -> DemandIntelligence:
        project_name = self._first_value(
            row,
            [
                "canonical_project_name",
                "project_name",
                "name",
            ],
            "Unnamed Project",
        )

        owner = self._first_value(
            row,
            [
                "owner",
                "canonical_company",
                "company_name",
                "applicant_name",
            ],
        )

        developer = self._first_value(
            row,
            [
                "developer",
                "owner",
                "company_name",
                "applicant_name",
            ],
        )

        market = self._first_value(
            row,
            [
                "market_cluster",
                "market",
                "county",
                "state",
            ],
        )

        phase, confidence, matched_terms = self.infer_phase(row)
        phase_summary = build_phase_summary(phase)

        buying_window = self.buying_window_days(phase)
        urgency = self.procurement_urgency(buying_window)
        clock_status = self.procurement_clock_status(buying_window)

        trade_packages = list(
            phase_summary.get("trade_packages", [])
        )

        buying_roles = list(
            phase_summary.get("buying_roles", [])
        )

        likely_distributors = list(
            phase_summary.get("likely_distributors", [])
        )

        product_families = list(
            phase_summary.get("product_families", [])
        )

        existing_roles = self.existing_relationship_roles(
            project_name=project_name,
            owner=owner,
            developer=developer,
        )

        missing_roles = self.missing_buying_roles(
            buying_roles,
            existing_roles,
        )

        recommended_distributor, distributor_confidence = (
            self.distributor_recommendation(
                project_name=project_name,
                row=row,
                phase=phase,
                knowledge_distributors=likely_distributors,
            )
        )

        revenue_intensity = self._clean(
            phase_summary.get("revenue_intensity"),
            "Unknown",
        )

        revenue_intensity_score = self._number(
            phase_summary.get("revenue_intensity_score"),
            0,
        )

        estimated_power_mw = self._number(
            row.get("estimated_power_mw", 0),
            0,
        )

        general_contractor = self._first_value(
            row,
            [
                "general_contractor",
                "gc",
                "prime_contractor",
            ],
        )

        electrical_contractor = self._first_value(
            row,
            [
                "electrical_contractor",
                "mep_contractor",
            ],
        )

        mechanical_contractor = self._first_value(
            row,
            [
                "mechanical_contractor",
                "mep_contractor",
            ],
        )

        contractor_known = any(
            [
                general_contractor,
                electrical_contractor,
                mechanical_contractor,
            ]
        )

        distributor_known = (
            recommended_distributor != "Unknown"
        )

        product_scores = self.product_demand_scores(
            product_families=product_families,
            phase_confidence=confidence,
            revenue_intensity_score=revenue_intensity_score,
            estimated_power_mw=estimated_power_mw,
            urgency=urgency,
        )

        demand_score = self.demand_score(
            phase_confidence=confidence,
            revenue_intensity_score=revenue_intensity_score,
            buying_window_days=buying_window,
            estimated_power_mw=estimated_power_mw,
            contractor_known=contractor_known,
            distributor_known=distributor_known,
            relationship_gap_count=len(missing_roles),
        )

        estimated_revenue = self.estimate_revenue(
            phase=phase,
            revenue_intensity_score=revenue_intensity_score,
            estimated_power_mw=estimated_power_mw,
            contractor_known=contractor_known,
            distributor_known=distributor_known,
            relationship_gap_count=len(missing_roles),
        )

        knowledge_action = self._clean(
            phase_summary.get("sales_action"),
            "Monitor project development.",
        )

        next_action = self.next_best_action(
            urgency=urgency,
            recommended_distributor=recommended_distributor,
            missing_roles=missing_roles,
            product_scores=product_scores,
            knowledge_action=knowledge_action,
        )

        result = DemandIntelligence(
            project_name=project_name,
            owner=owner,
            developer=developer,
            market=market,
            construction_phase=phase,
            phase_confidence=confidence,
            matched_phase_terms=matched_terms,
            procurement_window=self._clean(
                phase_summary.get("procurement_window")
            ),
            buying_window_days=buying_window,
            procurement_urgency=urgency,
            procurement_clock_status=clock_status,
            trade_packages=trade_packages,
            buying_roles=buying_roles,
            existing_relationship_roles=sorted(existing_roles),
            missing_buying_roles=missing_roles,
            likely_distributors=likely_distributors,
            recommended_distributor=recommended_distributor,
            distributor_confidence=distributor_confidence,
            product_families=product_families,
            product_demand_scores=product_scores,
            top_products=list(product_scores.keys())[:5],
            revenue_intensity=revenue_intensity,
            revenue_intensity_score=revenue_intensity_score,
            estimated_revenue=estimated_revenue,
            demand_score=demand_score,
            recommended_sales_action=knowledge_action,
            next_best_action=next_action,
        )

        result.explanation = self.build_explanation(result)

        return result

    def build(self, project_name: str) -> DemandIntelligence:
        project = self.project_record(project_name)

        if project.empty:
            return DemandIntelligence(
                project_name=project_name,
                next_best_action=(
                    "Validate the project name or project source record."
                ),
                explanation=(
                    "No matching project record was found."
                ),
            )

        return self.build_from_row(project.iloc[0])

    def build_dict(self, project_name: str) -> dict[str, Any]:
        return asdict(self.build(project_name))

    # =========================================================
    # PORTFOLIO
    # =========================================================

    def build_portfolio(
        self,
        limit: int | None = None,
    ) -> pd.DataFrame:
        if self.projects.empty:
            return pd.DataFrame()

        project_column = None

        for column in [
            "canonical_project_name",
            "project_name",
            "name",
        ]:
            if column in self.projects.columns:
                project_column = column
                break

        if not project_column:
            return pd.DataFrame()

        project_names = (
            self.projects[project_column]
            .dropna()
            .astype(str)
            .str.strip()
            .drop_duplicates()
        )

        if limit is not None:
            project_names = project_names.head(limit)

        rows = [
            self.build_dict(project_name)
            for project_name in project_names
        ]

        if not rows:
            return pd.DataFrame()

        portfolio = pd.DataFrame(rows)

        return portfolio.sort_values(
            by=[
                "demand_score",
                "estimated_revenue",
                "buying_window_days",
            ],
            ascending=[
                False,
                False,
                True,
            ],
        ).reset_index(drop=True)
