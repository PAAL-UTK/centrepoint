# scripts/test_subjects.py

from centrepoint.auth import CentrePointAuth
from centrepoint.api.studies import StudiesAPI
from centrepoint.api.subjects import SubjectsAPI
from centrepoint.utils.paginate import paginate

# Get study ID from first study
auth = CentrePointAuth()
study_api = StudiesAPI(auth)
subject_api = SubjectsAPI(auth)

study = study_api.list_studies().items[0]
print(f"\n📘 Pulling subjects from study: {study.name} (ID: {study.id})")

subjects = subject_api.list_subjects(study_id=study.id)
print(f"Found {subjects.totalCount} subjects.\n")

for subj in subjects.items:
    print(f" - {subj.subjectIdentifier} ({subj.gender}, {subj.dob})")

if subjects.items:
    detailed = subject_api.get_subject(study.id, subjects.items[0].id)
    print("\n🧬 Full details for first subject:")
    print(detailed.model_dump_json(indent=2))

for subject in paginate(lambda limit, offset: subject_api.list_subjects(study.id, limit, offset)):
    print(f"Subject: {subject.subjectIdentifier}")

