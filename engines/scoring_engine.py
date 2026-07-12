"""
Allen Hammett Intelligence Platform
Scoring Intelligence Engine
Phase 12
"""

import pandas as pd


class ScoringEngine:

    def __init__(self):
        pass

    ####################################################################
    # Relationship Score
    ####################################################################

    def relationship_score(self, contacts):

        if contacts is None or len(contacts) == 0:
            return 0

        score = 0

        score += min(len(contacts) * 5, 40)

        if "influence_score" in contacts.columns:
            score += min(
                contacts["influence_score"].fillna(0).mean(),
                40
            )

        if "email" in contacts.columns:
            score += contacts["email"].notna().mean() * 10

        if "linkedin_url" in contacts.columns:
            score += contacts["linkedin_url"].notna().mean() * 10

        return round(score, 1)

    ####################################################################
    # Coverage Score
    ####################################################################

    def coverage_score(self, contacts):

        if contacts is None or len(contacts) == 0:
            return 0

        roles = [
            "Owner / Developer",
            "General Executive",
            "Construction",
            "Engineering",
            "Operations",
            "Procurement",
            "Utility",
            "Government"
        ]

        if "relationship_role" not in contacts.columns:
            return 0

        found = contacts["relationship_role"].dropna().unique()

        matched = len(set(found).intersection(roles))

        return round((matched / len(roles)) * 100, 1)

    ####################################################################
    # Capture Score
    ####################################################################

    def capture_score(
        self,
        relationship_score,
        coverage_score,
        project_score=0,
        demand_score=0,
    ):

        score = (
            relationship_score * 0.40 +
            coverage_score * 0.30 +
            project_score * 0.20 +
            demand_score * 0.10
        )

        return round(score, 1)

    ####################################################################
    # Executive Priority
    ####################################################################

    def executive_priority(self, capture_score):

        if capture_score >= 90:
            return "CRITICAL"

        if capture_score >= 75:
            return "PRIME"

        if capture_score >= 60:
            return "HIGH"

        if capture_score >= 40:
            return "MEDIUM"

        return "MONITOR"

    ####################################################################
    # Summary
    ####################################################################

    def scorecard(self, contacts):

        relationship = self.relationship_score(contacts)

        coverage = self.coverage_score(contacts)

        capture = self.capture_score(
            relationship,
            coverage
        )

        return {
            "relationship_score": relationship,
            "coverage_score": coverage,
            "capture_score": capture,
            "priority": self.executive_priority(capture),
        }
