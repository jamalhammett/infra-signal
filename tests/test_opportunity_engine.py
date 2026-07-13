"""
Opportunity Engine Validation

This script validates the intelligence engines without
starting Streamlit.
"""

import pandas as pd

from engines.opportunity_engine import OpportunityEngine


############################################################
# SAMPLE PROJECT
############################################################

project_df = pd.DataFrame([

    {

        "canonical_project_name":"Aligned IAD-3",

        "owner":"Aligned",

        "developer":"Aligned",

        "general_contractor":"DPR Construction",

        "electrical_contractor":"M.C. Dean",

        "mechanical_contractor":"Southland",

        "market":"Ashburn",

        "estimated_power_mw":120,

        "project_stage":"Electrical Rough-In"

    }

])

############################################################
# SAMPLE RELATIONSHIPS
############################################################

relationship_df = pd.DataFrame([

    {

        "canonical_company":"Aligned",

        "canonical_project_name":"Aligned IAD-3",

        "full_name":"Robert Smith",

        "relationship_role":"Construction",

        "email":"rsmith@aligneddc.com",

        "linkedin_url":"https://linkedin.com/in/test",

        "influence_score":75

    },

    {

        "canonical_company":"Aligned",

        "canonical_project_name":"Aligned IAD-3",

        "full_name":"Sarah Jones",

        "relationship_role":"Operations",

        "email":"sjones@aligneddc.com",

        "linkedin_url":"https://linkedin.com/in/test2",

        "influence_score":82

    }

])

############################################################

engine = OpportunityEngine(

    project_df,

    relationship_df

)

opportunity = engine.build(

    "Aligned IAD-3"

)

print()

print("PROJECT")

print(opportunity.project_name)

print()

print("PHASE")

print(opportunity.construction_phase)

print()

print("CONTRACTOR")

print(opportunity.electrical_contractor)

print()

print("DISTRIBUTOR")

print(opportunity.distributor)

print()

print("PRODUCTS")

print(opportunity.product_families)

print()

print("CAPTURE SCORE")

print(opportunity.capture_score)

print()

print("AI SUMMARY")

print(opportunity.ai_summary)

print()

print("NEXT ACTION")

print(opportunity.next_action)
