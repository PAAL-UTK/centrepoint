# scripts/test_data_access.py

from centrepoint.auth import CentrePointAuth
from centrepoint.api.studies import StudiesAPI
from centrepoint.api.subjects import SubjectsAPI
from centrepoint.api.data_access import DataAccessAPI
from centrepoint.utils import paginate
from datetime import datetime

# Init
auth = CentrePointAuth()
study_api = StudiesAPI(auth)
subject_api = SubjectsAPI(auth)
data_api = DataAccessAPI(auth)

# Use first study + subject
study = study_api.list_studies().items[0]
subject = subject_api.list_subjects(study.id).items[0]

print(f"\n📥 Getting raw-accelerometer data files for subject {subject.subjectIdentifier}...")

files = data_api.list_files(
    study_id=study.id,
    subject_id=subject.id,
    data_category="raw-accelerometer",
    start_date=datetime(2021, 1, 1),
    end_date=datetime(2021, 1, 2),
)

print(f"Found {files.totalCount} files:")
for f in files.items:
    print(f" - {f.fileName} ({f.fileFormat}) -> {f.downloadUrl[:60]}...")
