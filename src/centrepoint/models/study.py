# centrepointAPI/models/study.py

from pydantic import BaseModel
from datetime import datetime
from typing import List

class Study(BaseModel):
    id: int
    name: str
    organizationName: str
    createdDateTime: datetime
    studyStatus: str
    defaultWearPosition: str
    wearPositions: List[str]
    monitorDataCollectionMode: str

class PaginatedStudies(BaseModel):
    items: List[Study]
    totalCount: int
    limit: int
    offset: int

