from engines.executive_engine import (
    ExecutiveContext,
    ExecutiveEngine,
    ExecutiveMetric,
    ExecutiveOpportunity,
    ExecutiveRisk,
    HealthBand,
)


def main() -> None:
    context = ExecutiveContext(
        organization_id="allen-hammett-inc",
        reporting_period="2026-Q3",
        metrics=(
            ExecutiveMetric("Revenue attainment", 85, 100, weight=2.0),
            ExecutiveMetric("Pipeline coverage", 3.1, 3.0, weight=1.5),
            ExecutiveMetric("Delivery health", 92, 100, weight=1.0),
        ),
        opportunities=(
            ExecutiveOpportunity(
                "Infrastructure Intelligence Platform",
                projected_value=750000,
                win_probability=0.65,
                urgency=0.9,
                strategic_fit=0.95,
                owner="Chief Revenue Officer",
            ),
        ),
        risks=(
            ExecutiveRisk(
                "Concentrated revenue dependency",
                severity=0.75,
                likelihood=0.6,
                impact=0.8,
                owner="CEO",
                description="Revenue concentration could impair growth resilience.",
            ),
        ),
    )

    brief = ExecutiveEngine().generate_brief(context)

    assert brief.organization_id == "allen-hammett-inc"
    assert 0 <= brief.business_health_score <= 100
    assert brief.projected_pipeline_value == 750000
    assert brief.probability_weighted_pipeline == 487500
    assert brief.health_band in set(HealthBand)
    assert brief.actions
    assert brief.to_dict()["health_band"] == brief.health_band.value

    print("PASS: Executive Engine")


if __name__ == "__main__":
    main()
