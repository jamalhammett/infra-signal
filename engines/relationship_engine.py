"""
Allen Hammett Intelligence Platform
Relationship Intelligence Engine
Phase 12
"""

import pandas as pd


class RelationshipEngine:

    def __init__(self, dataframe):

        self.df = dataframe.copy()

    ####################################################################
    # SEARCH
    ####################################################################

    def search(self, text):

        if text is None or text == "":
            return self.df

        text = text.lower()

        return self.df[
            self.df.astype(str)
            .apply(lambda row: row.str.lower().str.contains(text))
            .any(axis=1)
        ]

    ####################################################################
    # ACCOUNT
    ####################################################################

    def account(self, company):

        return self.df[
            self.df["canonical_company"]
            .fillna("")
            .str.lower()
            ==
            company.lower()
        ]

    ####################################################################
    # PROJECT
    ####################################################################

    def project(self, project):

        return self.df[
            self.df["canonical_project_name"]
            .fillna("")
            .str.lower()
            ==
            project.lower()
        ]

    ####################################################################
    # COVERAGE
    ####################################################################

    def coverage(self, company):

        contacts = self.account(company)

        return len(contacts)

    ####################################################################
    # AVERAGE INFLUENCE
    ####################################################################

    def influence(self, company):

        contacts = self.account(company)

        if len(contacts) == 0:
            return 0

        return round(
            contacts["influence_score"].fillna(0).mean(),
            1,
        )

    ####################################################################
    # EMAIL COVERAGE
    ####################################################################

    def email_coverage(self, company):

        contacts = self.account(company)

        if len(contacts) == 0:
            return 0

        return round(
            contacts["email"].notna().mean() * 100,
            1,
        )

    ####################################################################
    # LINKEDIN COVERAGE
    ####################################################################

    def linkedin_coverage(self, company):

        contacts = self.account(company)

        if len(contacts) == 0:
            return 0

        return round(
            contacts["linkedin_url"].notna().mean() * 100,
            1,
        )

    ####################################################################
    # BUYING COMMITTEE
    ####################################################################

    def buying_committee(self, company):

        contacts = self.account(company)

        priority = [

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
            return contacts

        contacts["rank"] = contacts.relationship_role.apply(
            lambda x: priority.index(x)
            if x in priority
            else 99
        )

        return contacts.sort_values("rank")

    ####################################################################
    # SUMMARY
    ####################################################################

    def summary(self, company):

        return {

            "contacts":

                self.coverage(company),

            "avg_influence":

                self.influence(company),

            "email_pct":

                self.email_coverage(company),

            "linkedin_pct":

                self.linkedin_coverage(company)

        }
