"""
Simple keyword-based skill extraction against a curated taxonomy. This is a
deliberately pragmatic approach for a portfolio project — production systems
at this scale would typically use NER/ML-based extraction, but keyword
matching against a maintained list is transparent, debuggable, and entirely
sufficient to answer "which skills are most in demand."
"""

from __future__ import annotations

SKILL_TAXONOMY: dict[str, str] = {
    # skill_name: skill_category
    "python": "Programming Language",
    "sql": "Programming Language",
    "pyspark": "Data Engineering",
    "spark": "Data Engineering",
    "delta lake": "Data Engineering",
    "azure data factory": "Cloud/ETL",
    "azure": "Cloud",
    "aws": "Cloud",
    "gcp": "Cloud",
    "power bi": "BI/Visualization",
    "tableau": "BI/Visualization",
    "docker": "DevOps",
    "kubernetes": "DevOps",
    "airflow": "Orchestration",
    "kafka": "Streaming",
    "snowflake": "Data Warehouse",
    "databricks": "Data Engineering",
    "machine learning": "Data Science",
    "java": "Programming Language",
    "scala": "Programming Language",
}


def extract_skills(text: str | None) -> list[str]:
    """Pure-Python skill extraction, unit-testable without Spark."""
    if not text:
        return []
    lowered = text.lower()
    return sorted({skill for skill in SKILL_TAXONOMY if skill in lowered})