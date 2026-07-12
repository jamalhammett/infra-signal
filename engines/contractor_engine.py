"""
Allen Hammett Intelligence Platform
Contractor Intelligence Engine
Phase 12
"""

import pandas as pd


class ContractorEngine:

    def __init__(self, project_df):

        self.projects = project_df.copy()

    ###########################################################
    # Search
    ###########################################################

    def search(self, text):

        if text is None or text == "":
            return self.projects

        text = text.lower()

        return self.projects[
            self.projects.astype(str)
            .apply(lambda row: row.str.lower().str.contains(text))
            .any(axis=1)
        ]

    ###########################################################
    # Single Project
    ###########################################################

    def project(self, project_name):

        if "canonical_project_name" not in self.projects.columns:
            return pd.DataFrame()

        return self.projects[
            self.projects["canonical_project_name"]
            .fillna("")
            .str.lower()
            ==
            project_name.lower()
        ]

    ###########################################################
    # Owner
    ###########################################################

    def owner(self, project):

        if "owner" not in project.columns:
            return "Unknown"

        values = project["owner"].dropna().unique()

        if len(values) == 0:
            return "Unknown"

        return values[0]

    ###########################################################
    # Developer
    ###########################################################

    def developer(self, project):

        if "developer" not in project.columns:
            return "Unknown"

        values = project["developer"].dropna().unique()

        if len(values) == 0:
            return "Unknown"

        return values[0]

    ###########################################################
    # General Contractor
    ###########################################################

    def general_contractor(self, project):

        if "general_contractor" not in project.columns:
            return "Unknown"

        values = project["general_contractor"].dropna().unique()

        if len(values) == 0:
            return "Unknown"

        return values[0]

    ###########################################################
    # Electrical Contractor
    ###########################################################

    def electrical_contractor(self, project):

        if "electrical_contractor" not in project.columns:
            return "Unknown"

        values = project["electrical_contractor"].dropna().unique()

        if len(values) == 0:
            return "Unknown"

        return values[0]

    ###########################################################
    # Mechanical Contractor
    ###########################################################

    def mechanical_contractor(self, project):

        if "mechanical_contractor" not in project.columns:
            return "Unknown"

        values = project["mechanical_contractor"].dropna().unique()

        if len(values) == 0:
            return "Unknown"

        return values[0]

    ###########################################################
    # Civil Contractor
    ###########################################################

    def civil_contractor(self, project):

        if "civil_contractor" not in project.columns:
            return "Unknown"

        values = project["civil_contractor"].dropna().unique()

        if len(values) == 0:
            return "Unknown"

        return values[0]

    ###########################################################
    # Commissioning
    ###########################################################

    def commissioning_agent(self, project):

        if "commissioning_agent" not in project.columns:
            return "Unknown"

        values = project["commissioning_agent"].dropna().unique()

        if len(values) == 0:
            return "Unknown"

        return values[0]

    ###########################################################
    # Summary
    ###########################################################

    def summary(self, project_name):

        project = self.project(project_name)

        return {

            "owner":
                self.owner(project),

            "developer":
                self.developer(project),

            "general_contractor":
                self.general_contractor(project),

            "electrical_contractor":
                self.electrical_contractor(project),

            "mechanical_contractor":
                self.mechanical_contractor(project),

            "civil_contractor":
                self.civil_contractor(project),

            "commissioning":
                self.commissioning_agent(project)

        }
