"""AHIP Executive Engine.

Aggregates operational signals into an explainable executive brief without
coupling the domain layer to Streamlit, PostgreSQL, or a specific AI provider.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
import logging
from statistics import mean
from typing import Any, Iterable, Mapping, Sequence
from uuid import uuid4

logger = logging.getLogger(__name__)
MODULE_VERSION = "1.0.0"


class HealthBand(str, Enum):
    CRITICAL = "critical"
    AT_RISK = "at_risk"
    STABLE = "stable"
    STRONG = "strong"


class ExecutivePriority(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass(frozen=True, slots=True)
class ExecutiveMetric:
    name: str
    value: float
    target: float | None = None
    weight: float = 1.0
    higher_is_better: bool = True
    source: str = "unknown"
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("name must not be empty")
        if self.weight < 0:
            raise ValueError("weight must not be negative")


@dataclass(frozen=True, slots=True)
class ExecutiveRisk:
    title: str
    severity: float
    likelihood: float
    impact: float
    owner: str | None = None
    description: str = ""
    source: str = "unknown"

    def __post_init__(self) -> None:
        for field_name, value in (
            ("severity", self.severity),
            ("likelihood", self.likelihood),
            ("impact", self.impact),
        ):
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{field_name} must be between 0.0 and 1.0")


@dataclass(frozen=True, slots=True)
class ExecutiveOpportunity:
    name: str
    projected_value: float
    win_probability: float
    urgency: float
    strategic_fit: float
    owner: str | None = None
    source: str = "unknown"

    def __post_init__(self) -> None:
        if self.projected_value < 0:
            raise ValueError("projected_value must not be negative")
        for field_name, value in (
            ("win_probability", self.win_probability),
            ("urgency", self.urgency),
            ("strategic_fit", self.strategic_fit),
        ):
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{field_name} must be between 0.0 and 1.0")


@dataclass(frozen=True, slots=True)
class ExecutiveContext:
    organization_id: str
    reporting_period: str
    metrics: tuple[ExecutiveMetric, ...] = ()
    risks: tuple[ExecutiveRisk, ...] = ()
    opportunities: tuple[ExecutiveOpportunity, ...] = ()
    notes: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.organization_id.strip():
            raise ValueError("organization_id must not be empty")
        if not self.reporting_period.strip():
            raise ValueError("reporting_period must not be empty")


@dataclass(frozen=True, slots=True)
class ExecutiveAction:
    priority: ExecutivePriority
    title: str
    rationale: str
    owner: str
    due_in_days: int


@dataclass(frozen=True, slots=True)
class ExecutiveBrief:
    brief_id: str
    organization_id: str
    reporting_period: str
    business_health_score: float
    health_band: HealthBand
    projected_pipeline_value: float
    probability_weighted_pipeline: float
    top_opportunities: tuple[str, ...]
    top_risks: tuple[str, ...]
    actions: tuple[ExecutiveAction, ...]
    headline: str
    narrative: str
    generated_at: datetime
    model_version: str = MODULE_VERSION

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["health_band"] = self.health_band.value
        payload["generated_at"] = self.generated_at.isoformat()
        payload["actions"] = [
            {
                **asdict(action),
                "priority": action.priority.value,
            }
            for action in self.actions
        ]
        return payload


class ExecutiveEngine:
    """Produces explainable executive-level operational intelligence."""

    def generate_brief(self, context: ExecutiveContext) -> ExecutiveBrief:
        health_score = self._calculate_health(context.metrics)
        health_band = self._health_band(health_score)
        pipeline = sum(item.projected_value for item in context.opportunities)
        weighted_pipeline = sum(
            item.projected_value * item.win_probability
            for item in context.opportunities
        )
        ranked_opportunities = self._rank_opportunities(context.opportunities)
        ranked_risks = self._rank_risks(context.risks)
        actions = self._build_actions(
            health_band=health_band,
            opportunities=ranked_opportunities,
            risks=ranked_risks,
        )

        top_opportunity_names = tuple(item.name for item in ranked_opportunities[:5])
        top_risk_names = tuple(item.title for item in ranked_risks[:5])

        headline = self._headline(
            health_band=health_band,
            weighted_pipeline=weighted_pipeline,
            risk_count=len(ranked_risks),
        )
        narrative = self._narrative(
            context=context,
            health_score=health_score,
            pipeline=pipeline,
            weighted_pipeline=weighted_pipeline,
            opportunities=ranked_opportunities,
            risks=ranked_risks,
        )

        logger.info(
            "Executive brief generated",
            extra={
                "organization_id": context.organization_id,
                "health_score": health_score,
                "health_band": health_band.value,
                "opportunity_count": len(context.opportunities),
                "risk_count": len(context.risks),
            },
        )

        return ExecutiveBrief(
            brief_id=str(uuid4()),
            organization_id=context.organization_id,
            reporting_period=context.reporting_period,
            business_health_score=round(health_score, 2),
            health_band=health_band,
            projected_pipeline_value=round(pipeline, 2),
            probability_weighted_pipeline=round(weighted_pipeline, 2),
            top_opportunities=top_opportunity_names,
            top_risks=top_risk_names,
            actions=actions,
            headline=headline,
            narrative=narrative,
            generated_at=datetime.now(timezone.utc),
        )

    @staticmethod
    def _calculate_health(metrics: Sequence[ExecutiveMetric]) -> float:
        if not metrics:
            return 50.0

        weighted_scores: list[float] = []
        weights: list[float] = []

        for metric in metrics:
            if metric.weight == 0:
                continue

            if metric.target is None:
                score = max(0.0, min(100.0, metric.value))
            elif metric.target == 0:
                score = 100.0 if metric.value == 0 else 0.0
            else:
                ratio = metric.value / metric.target
                if not metric.higher_is_better:
                    ratio = metric.target / metric.value if metric.value else 2.0
                score = max(0.0, min(100.0, ratio * 100.0))

            weighted_scores.append(score * metric.weight)
            weights.append(metric.weight)

        if not weights or sum(weights) == 0:
            return 50.0
        return sum(weighted_scores) / sum(weights)

    @staticmethod
    def _health_band(score: float) -> HealthBand:
        if score < 40:
            return HealthBand.CRITICAL
        if score < 60:
            return HealthBand.AT_RISK
        if score < 80:
            return HealthBand.STABLE
        return HealthBand.STRONG

    @staticmethod
    def _rank_opportunities(
        opportunities: Iterable[ExecutiveOpportunity],
    ) -> tuple[ExecutiveOpportunity, ...]:
        return tuple(
            sorted(
                opportunities,
                key=lambda item: (
                    item.projected_value
                    * item.win_probability
                    * (0.5 + item.urgency * 0.25 + item.strategic_fit * 0.25)
                ),
                reverse=True,
            )
        )

    @staticmethod
    def _rank_risks(risks: Iterable[ExecutiveRisk]) -> tuple[ExecutiveRisk, ...]:
        return tuple(
            sorted(
                risks,
                key=lambda item: item.severity * item.likelihood * item.impact,
                reverse=True,
            )
        )

    def _build_actions(
        self,
        *,
        health_band: HealthBand,
        opportunities: Sequence[ExecutiveOpportunity],
        risks: Sequence[ExecutiveRisk],
    ) -> tuple[ExecutiveAction, ...]:
        actions: list[ExecutiveAction] = []

        if health_band is HealthBand.CRITICAL:
            actions.append(
                ExecutiveAction(
                    priority=ExecutivePriority.CRITICAL,
                    title="Launch executive recovery review",
                    rationale="Overall business health is below the acceptable operating threshold.",
                    owner="CEO",
                    due_in_days=1,
                )
            )
        elif health_band is HealthBand.AT_RISK:
            actions.append(
                ExecutiveAction(
                    priority=ExecutivePriority.HIGH,
                    title="Approve corrective operating plan",
                    rationale="Key operating metrics are under target.",
                    owner="COO",
                    due_in_days=3,
                )
            )

        if risks:
            top = risks[0]
            actions.append(
                ExecutiveAction(
                    priority=(
                        ExecutivePriority.CRITICAL
                        if top.severity >= 0.8
                        else ExecutivePriority.HIGH
                    ),
                    title=f"Mitigate: {top.title}",
                    rationale=top.description or "Highest-ranked enterprise risk.",
                    owner=top.owner or "Executive Sponsor",
                    due_in_days=2 if top.severity >= 0.8 else 5,
                )
            )

        if opportunities:
            top = opportunities[0]
            actions.append(
                ExecutiveAction(
                    priority=(
                        ExecutivePriority.CRITICAL
                        if top.urgency >= 0.85
                        else ExecutivePriority.HIGH
                    ),
                    title=f"Advance: {top.name}",
                    rationale=(
                        f"Top weighted opportunity with projected value "
                        f"${top.projected_value:,.2f}."
                    ),
                    owner=top.owner or "Revenue Leader",
                    due_in_days=1 if top.urgency >= 0.85 else 3,
                )
            )

        if not actions:
            actions.append(
                ExecutiveAction(
                    priority=ExecutivePriority.MEDIUM,
                    title="Maintain operating cadence",
                    rationale="No critical exception requires immediate executive intervention.",
                    owner="Executive Team",
                    due_in_days=7,
                )
            )

        return tuple(actions[:5])

    @staticmethod
    def _headline(
        *,
        health_band: HealthBand,
        weighted_pipeline: float,
        risk_count: int,
    ) -> str:
        return (
            f"Business health is {health_band.value.replace('_', ' ')}, "
            f"with ${weighted_pipeline:,.0f} in probability-weighted pipeline "
            f"and {risk_count} tracked enterprise risk(s)."
        )

    @staticmethod
    def _narrative(
        *,
        context: ExecutiveContext,
        health_score: float,
        pipeline: float,
        weighted_pipeline: float,
        opportunities: Sequence[ExecutiveOpportunity],
        risks: Sequence[ExecutiveRisk],
    ) -> str:
        parts = [
            (
                f"For {context.reporting_period}, the organization posted a "
                f"business health score of {health_score:.1f}."
            ),
            (
                f"Gross pipeline totals ${pipeline:,.2f}, with "
                f"${weighted_pipeline:,.2f} probability weighted."
            ),
        ]

        if opportunities:
            parts.append(
                f"The highest-priority opportunity is {opportunities[0].name}."
            )
        if risks:
            parts.append(
                f"The highest-priority risk is {risks[0].title}."
            )
        if context.notes:
            parts.append("Executive notes: " + " ".join(context.notes[:3]))

        return " ".join(parts)


__all__ = [
    "ExecutiveAction",
    "ExecutiveBrief",
    "ExecutiveContext",
    "ExecutiveEngine",
    "ExecutiveMetric",
    "ExecutiveOpportunity",
    "ExecutivePriority",
    "ExecutiveRisk",
    "HealthBand",
]
