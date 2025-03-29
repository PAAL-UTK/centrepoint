# centrepointAPI/models/data_access.py

from pydantic import BaseModel
from datetime import datetime
from typing import List

class DataAccessFile(BaseModel):
    studyId: int
    subjectId: int
    dataCategory: str
    date: datetime
    fileName: str
    fileFormat: str
    modifiedDate: datetime
    downloadUrl: str
    downloadUrlExpiresOn: datetime

class PaginatedDataAccessFiles(BaseModel):
    items: List[DataAccessFile]
    totalCount: int
    limit: int
    offset: int
