# centrepointAPI/models/subject.py

from pydantic import BaseModel
from typing import Optional, List
from datetime import date

class Subject(BaseModel):
    """Represents a CentrePoint subject with demographic and study-related metadata."""

    id: int
    studyId: int
    siteId: int
    siteIdentifier: Optional[str] = None
    subjectIdentifier: str
    dob: Optional[date] = None
    gender: Optional[str] = None
    timezone: Optional[str] = None
    wearPosition: Optional[str] = None
    weight: Optional[float] = None
    weightUnit: Optional[str] = None
    height: Optional[float] = None
    heightUnit: Optional[str] = None
    assignmentStatus: Optional[str] = None
    assignmentId: Optional[int] = None

class PaginatedSubjects(BaseModel):
    """Container for paginated Subject results from the CentrePoint API."""

    items: List[Subject]
    totalCount: int
    limit: int
    offset: int

