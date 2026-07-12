"""
Allen Hammett Intelligence Platform
Distributor Intelligence Engine
Phase 12
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class DistributorRecommendation:
    distributor: str
    confidence: int
    rationale: str
    recommended_action: str


class DistributorEngine:
    """
    Recommends likely distributor channels for infrastructure opportunities.

    The engine uses:
    - general contractor
    - electrical contractor
    - mechanical contractor
    - construction phase
    - project type
    - market
    """

    CONTRACTOR_DISTRIBUTOR_MAP = {
        "DPR Construction": ["White Cap", "Fastenal", "Grainger"],
        "Turner Construction": ["White Cap", "HD Supply", "Grainger"],
        "HITT": ["White Cap", "Fastenal"],
        "Clayco": ["White Cap", "Grainger"],
        "Whiting-Turner": ["Fastenal", "Grainger"],
        "Holder Construction": ["White Cap", "Fastenal"],
        "JE Dunn": ["White Cap", "Grainger"],
        "Fortis Construction": ["White Cap", "Fastenal"],
        "Rosendin": ["Graybar", "Wesco"],
        "M.C. Dean": ["Graybar", "Border States"],
        "Dynaelectric": ["Graybar", "Wesco"],
        "Cupertino Electric": ["Wesco", "Graybar"],
        "Faith Technologies": ["Graybar", "Wesco"],
        "The Bell Company": ["White Cap", "Grainger"],
    }

    TRADE_DISTRIBUTOR_MAP = {
        "Electrical": ["Graybar", "Wesco", "Border States", "Rexel"],
        "Mechanical": ["Grainger", "Ferguson", "HD Supply"],
        "Concrete": ["White Cap", "Fastenal"],
        "Civil": ["White Cap", "Grainger"],
        "General Construction": ["White Cap", "Fastenal", "Grainger"],
        "Operations": ["Grainger", "Fastenal", "HD Supply"],
    }

    MARKET_DISTRIBUTOR_MAP = {
        "Ashburn": ["Graybar", "Wesco", "White Cap", "Grainger"],
        "Northern Virginia": ["Graybar", "Wesco", "White Cap", "Fastenal"],
        "Virginia": ["Graybar", "Wesco", "White Cap", "Grainger"],
        "Maryland": ["Graybar", "Wesco", "White Cap"],
        "Texas": ["Graybar", "Wesco", "Border States", "White Cap"],
        "Arizona": ["Graybar", "Wesco", "White Cap"],
    }

    def __init__(self, project_df: pd.DataFrame | None = None):
        self.projects = project_df.copy() if project_df is not None else pd.DataFrame()

    @staticmethod
    def _clean(value: Any) -> str:
        if value is None:
            return ""

        try:
            if pd.isna(value):
                return ""
        except Exception:
            pass

        return str(value).strip()

    @staticmethod
    def _normalize(value: Any) -> str:
        return DistributorEngine._clean(value).lower()

    def project(self, project_name: str) -> pd.DataFrame:
        if self.projects.empty:
            return pd.DataFrame()

        if "canonical_project_name" not in self.projects.columns:
            return pd.DataFrame()

        target = self._normalize(project_name)

        return self.projects[
            self.projects["canonical_project_name"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
            == target
        ].copy()

    def _first_value(
        self,
        project: pd.DataFrame,
        candidate_columns: list[str],
    ) -> str:
        if project is None or project.empty:
            return ""

        for column in candidate_columns:
            if column not in project.columns:
                continue

            values = project[column].dropna().astype(str).str.strip()
            values = values[values != ""]

            if not values.empty:
                return values.iloc[0]

        return ""

    def general_contractor(self, project: pd.DataFrame) -> str:
        return self._first_value(
            project,
            [
                "general_contractor",
                "gc",
                "prime_contractor",
                "contractor",
            ],
        )

    def electrical_contractor(self, project: pd.DataFrame) -> str:
        return self._first_value(
            project,
            [
                "electrical_contractor",
                "electrical",
                "mep_contractor",
            ],
        )

    def mechanical_contractor(self, project: pd.DataFrame) -> str:
        return self._first_value(
            project,
            [
                "mechanical_contractor",
                "mechanical",
                "mep_contractor",
            ],
        )

    def market(self, project: pd.DataFrame) -> str:
        return self._first_value(
            project,
            [
                "market_cluster",
                "market",
                "county",
                "state",
            ],
        )

    def project_phase(self, project: pd.DataFrame) -> str:
        return self._first_value(
            project,
            [
                "project_phase",
                "project_stage",
                "capture_stage",
            ],
        )

    def _score_distributor(
        self,
        distributor: str,
        contractor_hits: int,
        trade_hits: int,
        market_hits: int,
    ) -> int:
        score = 20

        score += contractor_hits * 25
        score += trade_hits * 15
        score += market_hits * 10

        return min(score, 100)

    def recommend_from_values(
        self,
        general_contractor: str = "",
        electrical_contractor: str = "",
        mechanical_contractor: str = "",
        market: str = "",
        project_phase: str = "",
    ) -> pd.DataFrame:
        scores: dict[str, dict[str, Any]] = {}

        contractor_values = [
            self._clean(general_contractor),
            self._clean(electrical_contractor),
            self._clean(mechanical_contractor),
        ]

        for contractor in contractor_values:
            if not contractor:
                continue

            for known_contractor, distributors in self.CONTRACTOR_DISTRIBUTOR_MAP.items():
                if self._normalize(known_contractor) in self._normalize(contractor):
                    for distributor in distributors:
                        record = scores.setdefault(
                            distributor,
                            {
                                "contractor_hits": 0,
                                "trade_hits": 0,
                                "market_hits": 0,
                                "reasons": [],
                            },
                        )
                        record["contractor_hits"] += 1
                        record["reasons"].append(
                            f"Matched contractor ecosystem: {known_contractor}"
                        )

        electrical_text = self._normalize(electrical_contractor)
        mechanical_text = self._normalize(mechanical_contractor)
        phase_text = self._normalize(project_phase)

        trade_matches = []

        if electrical_text:
            trade_matches.append("Electrical")

        if mechanical_text:
            trade_matches.append("Mechanical")

        if any(
            term in phase_text
            for term in ["concrete", "civil", "grading", "site work", "sitework"]
        ):
            trade_matches.append("Concrete")
            trade_matches.append("Civil")

        if not trade_matches:
            trade_matches.append("General Construction")

        for trade in trade_matches:
            for distributor in self.TRADE_DISTRIBUTOR_MAP.get(trade, []):
                record = scores.setdefault(
                    distributor,
                    {
                        "contractor_hits": 0,
                        "trade_hits": 0,
                        "market_hits": 0,
                        "reasons": [],
                    },
                )
                record["trade_hits"] += 1
                record["reasons"].append(f"Aligned to trade package: {trade}")

        market_text = self._normalize(market)

        for known_market, distributors in self.MARKET_DISTRIBUTOR_MAP.items():
            if self._normalize(known_market) in market_text:
                for distributor in distributors:
                    record = scores.setdefault(
                        distributor,
                        {
                            "contractor_hits": 0,
                            "trade_hits": 0,
                            "market_hits": 0,
                            "reasons": [],
                        },
                    )
                    record["market_hits"] += 1
                    record["reasons"].append(
                        f"Strong channel presence in market: {known_market}"
                    )

        if not scores:
            scores = {
                "Graybar": {
                    "contractor_hits": 0,
                    "trade_hits": 1,
                    "market_hits": 0,
                    "reasons": ["Default mission-critical electrical channel"],
                },
                "White Cap": {
                    "contractor_hits": 0,
                    "trade_hits": 1,
                    "market_hits": 0,
                    "reasons": ["Default commercial construction channel"],
                },
                "Grainger": {
                    "contractor_hits": 0,
                    "trade_hits": 1,
                    "market_hits": 0,
                    "reasons": ["Default MRO and operations channel"],
                },
            }

        rows = []

        for distributor, detail in scores.items():
            confidence = self._score_distributor(
                distributor=distributor,
                contractor_hits=detail["contractor_hits"],
                trade_hits=detail["trade_hits"],
                market_hits=detail["market_hits"],
            )

            rationale = "; ".join(dict.fromkeys(detail["reasons"]))

            if confidence >= 80:
                action = (
                    f"Coordinate immediate joint pursuit with the "
                    f"{distributor} strategic account team."
                )
            elif confidence >= 60:
                action = (
                    f"Validate whether {distributor} is attached to the "
                    f"contractor procurement path."
                )
            else:
                action = (
                    f"Monitor {distributor} and confirm account ownership "
                    f"before field mobilization."
                )

            rows.append(
                {
                    "distributor": distributor,
                    "confidence": confidence,
                    "rationale": rationale,
                    "recommended_action": action,
                }
            )

        return pd.DataFrame(rows).sort_values(
            by=["confidence", "distributor"],
            ascending=[False, True],
        ).reset_index(drop=True)

    def recommend(self, project_name: str) -> pd.DataFrame:
        project = self.project(project_name)

        if project.empty:
            return pd.DataFrame(
                columns=[
                    "distributor",
                    "confidence",
                    "rationale",
                    "recommended_action",
                ]
            )

        return self.recommend_from_values(
            general_contractor=self.general_contractor(project),
            electrical_contractor=self.electrical_contractor(project),
            mechanical_contractor=self.mechanical_contractor(project),
            market=self.market(project),
            project_phase=self.project_phase(project),
        )

    def top_recommendation(self, project_name: str) -> DistributorRecommendation:
        recommendations = self.recommend(project_name)

        if recommendations.empty:
            return DistributorRecommendation(
                distributor="Unknown",
                confidence=0,
                rationale="No distributor evidence available.",
                recommended_action="Build contractor and trade-package coverage.",
            )

        row = recommendations.iloc[0]

        return DistributorRecommendation(
            distributor=str(row["distributor"]),
            confidence=int(row["confidence"]),
            rationale=str(row["rationale"]),
            recommended_action=str(row["recommended_action"]),
        )

    def portfolio_recommendations(self) -> pd.DataFrame:
        if self.projects.empty:
            return pd.DataFrame()

        if "canonical_project_name" not in self.projects.columns:
            return pd.DataFrame()

        rows = []

        project_names = (
            self.projects["canonical_project_name"]
            .dropna()
            .astype(str)
            .str.strip()
            .drop_duplicates()
        )

        for project_name in project_names:
            top = self.top_recommendation(project_name)

            rows.append(
                {
                    "canonical_project_name": project_name,
                    "recommended_distributor": top.distributor,
                    "distributor_confidence": top.confidence,
                    "distributor_rationale": top.rationale,
                    "distributor_action": top.recommended_action,
                }
            )

        return pd.DataFrame(rows).sort_values(
            by=["distributor_confidence", "canonical_project_name"],
            ascending=[False, True],
        ).reset_index(drop=True)
