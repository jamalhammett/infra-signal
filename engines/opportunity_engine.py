"""
Allen Hammett Intelligence Platform
Opportunity Intelligence Orchestrator
Phase 12
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from engines.contractor_engine import ContractorEngine
from engines.distributor_engine import DistributorEngine
from engines.relationship_engine import RelationshipEngine
from engines.scoring_engine import ScoringEngine
from knowledge.data_center_construction import (
    build_phase_summary,
    infer_phase_from_text,
)
from models.opportunity import Opportunity


class OpportunityEngine:
    """
    Combines project, relationship, contractor, distributor,
    construction, demand, and scoring intelligence into one
    Opportunity record.
    """

    def __init__(
        self,
        project_df: pd.DataFrame,
        relationship_df: pd.DataFrame,
    ):
        self.projects = (
            project_df.copy()
            if project_df is not None
            else pd.DataFrame()
        )

        self.relationships = (
            relationship_df.copy()
            if relationship_df is not None
            else pd.DataFrame()
        )

        self.relationship_engine = RelationshipEngine(
            self.relationships
        )

        self.contractor_engine = ContractorEngine(
            self.projects
        )

        self.distributor_engine = DistributorEngine(
            self.projects
        )

        self.scoring_engine = ScoringEngine()

    @staticmethod
    def _clean(value: Any, fallback: str = "") -> str:
        if value is None:
            return fallback

        try:
            if pd.isna(value):
                return fallback
        except Exception:
            pass

        text = str(value).strip()

        if text.lower() in {
            "",
            "nan",
            "none",
            "null",
        }:
            return fallback

        return text

    @staticmethod
    def _number(
        value: Any,
        default: float = 0,
    ) -> float:
        try:
            number = pd.to_numeric(
                value,
                errors="coerce",
            )

            if pd.isna(number):
                return default

            return float(number)

        except Exception:
            return default

    def project_record(
        self,
        project_name: str,
    ) -> pd.DataFrame:
        if self.projects.empty:
            return pd.DataFrame()

        if "canonical_project_name" not in self.projects.columns:
            return pd.DataFrame()

        return self.projects[
            self.projects["canonical_project_name"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
            ==
            str(project_name).strip().lower()
        ].copy()

    def _first_value(
        self,
        project: pd.DataFrame,
        columns: list[str],
        fallback: str = "",
    ) -> str:
        if project.empty:
            return fallback

        for column in columns:
            if column not in project.columns:
                continue

            values = (
                project[column]
                .dropna()
                .astype(str)
                .str.strip()
            )

            values = values[
                ~values.str.lower().isin(
                    ["", "nan", "none", "null"]
                )
            ]

            if not values.empty:
                return values.iloc[0]

        return fallback

    def _project_text(
        self,
        project: pd.DataFrame,
    ) -> str:
        if project.empty:
            return ""

        row = project.iloc[0]

        fields = [
            "canonical_project_name",
            "project_name",
            "project_stage",
            "permit_description",
            "raw_text",
            "strategic_notes",
            "source_name",
            "infrastructure_type",
        ]

        values = [
            self._clean(row.get(field), "")
            for field in fields
        ]

        return " ".join(
            value
            for value in values
            if value
        )

    def _relationship_contacts(
        self,
        project_name: str,
        owner: str,
        developer: str,
    ) -> pd.DataFrame:
        project_contacts = self.relationship_engine.project(
            project_name
        )

        if not project_contacts.empty:
            return project_contacts.copy()

        account_frames = []

        for company in [owner, developer]:
            if not company:
                continue

            contacts = self.relationship_engine.account(
                company
            )

            if not contacts.empty:
                account_frames.append(contacts)

        if not account_frames:
            return pd.DataFrame()

        return (
            pd.concat(
                account_frames,
                ignore_index=True,
            )
            .drop_duplicates()
            .reset_index(drop=True)
        )

    @staticmethod
    def _missing_roles(
        contacts: pd.DataFrame,
    ) -> list[str]:
        required_roles = [
            "Owner / Developer",
            "General Executive",
            "Construction",
            "Engineering",
            "Operations",
            "Procurement",
            "Utility",
            "Government",
        ]

        if (
            contacts is None
            or contacts.empty
            or "relationship_role" not in contacts.columns
        ):
            return required_roles

        existing_roles = set(
            contacts["relationship_role"]
            .dropna()
            .astype(str)
            .tolist()
        )

        return [
            role
            for role in required_roles
            if role not in existing_roles
        ]

    @staticmethod
    def _buying_window_days(
        phase: str,
    ) -> int:
        windows = {
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

        return windows.get(phase, 60)

    @staticmethod
    def _estimated_revenue(
        revenue_intensity_score: float,
        project_mw: float,
        contractor_known: bool,
    ) -> float:
        base = revenue_intensity_score * 10000

        mw_factor = min(
            max(project_mw, 0),
            1000,
        ) * 1500

        contractor_factor = (
            250000
            if contractor_known
            else 0
        )

        return round(
            base
            + mw_factor
            + contractor_factor,
            2,
        )

    @staticmethod
    def _demand_score(
        revenue_intensity_score: float,
        phase_confidence: float,
        contractor_known: bool,
    ) -> float:
        score = (
            revenue_intensity_score * 0.60
            + phase_confidence * 0.30
            + (
                10
                if contractor_known
                else 0
            )
        )

        return round(
            min(score, 100),
            1,
        )

    @staticmethod
    def _executive_summary(
        opportunity: Opportunity,
        phase_confidence: int,
    ) -> str:
        contractor_text = (
            opportunity.electrical_contractor
            or opportunity.general_contractor
            or "No contractor confirmed"
        )

        distributor_text = (
            opportunity.distributor
            or "No distributor confirmed"
        )

        products = (
            ", ".join(
                opportunity.product_families[:5]
            )
            if opportunity.product_families
            else "No products mapped"
        )

        missing = (
            ", ".join(
                opportunity.missing_roles[:4]
            )
            if opportunity.missing_roles
            else "No critical relationship gaps"
        )

        return (
            f"{opportunity.project_name} is currently assessed in the "
            f"{opportunity.construction_phase} phase with "
            f"{phase_confidence}% phase confidence. "
            f"The primary contractor path is {contractor_text}. "
            f"The recommended distributor is {distributor_text} "
            f"with {opportunity.distributor_confidence:.0f}% confidence. "
            f"Priority product families include {products}. "
            f"Current relationship gaps include {missing}. "
            f"The opportunity is rated {opportunity.executive_priority} "
            f"with a capture score of {opportunity.capture_score:.1f}."
        )

    @staticmethod
    def _next_action(
        opportunity: Opportunity,
        knowledge_action: str,
    ) -> str:
        if (
            opportunity.distributor
            and opportunity.distributor != "Unknown"
        ):
            distributor_action = (
                f"Coordinate with "
                f"{opportunity.distributor}."
            )
        else:
            distributor_action = (
                "Validate the distributor channel."
            )

        if opportunity.missing_roles:
            relationship_action = (
                "Prioritize relationship development for "
                + ", ".join(
                    opportunity.missing_roles[:3]
                )
                + "."
            )
        else:
            relationship_action = (
                "Maintain buying-committee coverage."
            )

        return (
            f"{knowledge_action} "
            f"{distributor_action} "
            f"{relationship_action}"
        )

    def build(
        self,
        project_name: str,
    ) -> Opportunity:
        project = self.project_record(
            project_name
        )

        if project.empty:
            return Opportunity(
                project_name=project_name,
                executive_priority="NOT FOUND",
                ai_summary=(
                    "The requested project was not found."
                ),
                next_action=(
                    "Validate the project name or source record."
                ),
            )

        owner = self._first_value(
            project,
            [
                "owner",
                "canonical_company",
                "company_name",
                "applicant_name",
            ],
        )

        developer = self._first_value(
            project,
            [
                "developer",
                "owner",
                "company_name",
                "applicant_name",
            ],
        )

        market = self._first_value(
            project,
            [
                "market_cluster",
                "market",
                "county",
                "state",
            ],
        )

        contractor_summary = (
            self.contractor_engine.summary(
                project_name
            )
        )

        project_text = self._project_text(
            project
        )

        phase, phase_confidence, _ = (
            infer_phase_from_text(
                project_text
            )
        )

        phase_summary = build_phase_summary(
            phase
        )

        contacts = self._relationship_contacts(
            project_name=project_name,
            owner=owner,
            developer=developer,
        )

        scorecard = (
            self.scoring_engine.scorecard(
                contacts
            )
        )

        missing_roles = self._missing_roles(
            contacts
        )

        distributor_recommendations = (
            self.distributor_engine.recommend_from_values(
                general_contractor=contractor_summary.get(
                    "general_contractor",
                    "",
                ),
                electrical_contractor=contractor_summary.get(
                    "electrical_contractor",
                    "",
                ),
                mechanical_contractor=contractor_summary.get(
                    "mechanical_contractor",
                    "",
                ),
                market=market,
                project_phase=phase,
            )
        )

        if distributor_recommendations.empty:
            distributor = "Unknown"
            distributor_confidence = 0
        else:
            distributor = str(
                distributor_recommendations.iloc[0][
                    "distributor"
                ]
            )

            distributor_confidence = float(
                distributor_recommendations.iloc[0][
                    "confidence"
                ]
            )

        project_mw = self._number(
            project.iloc[0].get(
                "estimated_power_mw",
                0,
            )
        )

        contractor_known = any(
            self._clean(
                contractor_summary.get(
                    key,
                    "",
                )
            )
            not in {
                "",
                "Unknown",
            }
            for key in [
                "general_contractor",
                "electrical_contractor",
                "mechanical_contractor",
                "civil_contractor",
            ]
        )

        revenue_intensity_score = self._number(
            phase_summary.get(
                "revenue_intensity_score",
                0,
            )
        )

        demand_score = self._demand_score(
            revenue_intensity_score=(
                revenue_intensity_score
            ),
            phase_confidence=phase_confidence,
            contractor_known=contractor_known,
        )

        estimated_revenue = (
            self._estimated_revenue(
                revenue_intensity_score=(
                    revenue_intensity_score
                ),
                project_mw=project_mw,
                contractor_known=contractor_known,
            )
        )

        project_score = self._number(
            project.iloc[0].get(
                "early_capture_score",
                0,
            )
        )

        capture_score = (
            self.scoring_engine.capture_score(
                relationship_score=scorecard.get(
                    "relationship_score",
                    0,
                ),
                coverage_score=scorecard.get(
                    "coverage_score",
                    0,
                ),
                project_score=min(
                    project_score,
                    100,
                ),
                demand_score=demand_score,
            )
        )

        priority = (
            self.scoring_engine.executive_priority(
                capture_score
            )
        )

        opportunity = Opportunity(
            project_name=project_name,
            owner=owner,
            developer=developer,
            market=market,
            general_contractor=contractor_summary.get(
                "general_contractor",
                "Unknown",
            ),
            electrical_contractor=contractor_summary.get(
                "electrical_contractor",
                "Unknown",
            ),
            mechanical_contractor=contractor_summary.get(
                "mechanical_contractor",
                "Unknown",
            ),
            civil_contractor=contractor_summary.get(
                "civil_contractor",
                "Unknown",
            ),
            construction_phase=phase,
            buying_window_days=(
                self._buying_window_days(
                    phase
                )
            ),
            trade_packages=phase_summary.get(
                "trade_packages",
                [],
            ),
            contacts=len(contacts),
            relationship_score=scorecard.get(
                "relationship_score",
                0,
            ),
            coverage_score=scorecard.get(
                "coverage_score",
                0,
            ),
            influence_score=(
                self.relationship_engine.influence(
                    owner
                )
                if owner
                else 0
            ),
            missing_roles=missing_roles,
            distributor=distributor,
            distributor_confidence=(
                distributor_confidence
            ),
            product_families=phase_summary.get(
                "product_families",
                [],
            ),
            estimated_revenue=estimated_revenue,
            demand_score=demand_score,
            capture_score=capture_score,
            executive_priority=priority,
        )

        opportunity.ai_summary = (
            self._executive_summary(
                opportunity,
                phase_confidence,
            )
        )

        opportunity.next_action = (
            self._next_action(
                opportunity,
                phase_summary.get(
                    "sales_action",
                    "Monitor the opportunity.",
                ),
            )
        )

        return opportunity

    def build_dict(
        self,
        project_name: str,
    ) -> dict[str, Any]:
        opportunity = self.build(
            project_name
        )

        return {
            field_name: getattr(
                opportunity,
                field_name,
            )
            for field_name in (
                opportunity.__dataclass_fields__
            )
        }

    def build_portfolio(
        self,
        limit: int | None = None,
    ) -> pd.DataFrame:
        if (
            self.projects.empty
            or "canonical_project_name"
            not in self.projects.columns
        ):
            return pd.DataFrame()

        project_names = (
            self.projects[
                "canonical_project_name"
            ]
            .dropna()
            .astype(str)
            .str.strip()
            .drop_duplicates()
        )

        if limit is not None:
            project_names = project_names.head(
                limit
            )

        rows = [
            self.build_dict(
                project_name
            )
            for project_name in project_names
        ]

        if not rows:
            return pd.DataFrame()

        portfolio = pd.DataFrame(
            rows
        )

        return portfolio.sort_values(
            by=[
                "capture_score",
                "demand_score",
                "estimated_revenue",
            ],
            ascending=False,
        ).reset_index(drop=True)
