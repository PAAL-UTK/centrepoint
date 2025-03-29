# scripts/test_studies.py

from centrepoint.auth import CentrePointAuth
from centrepoint.api.studies import StudiesAPI

auth = CentrePointAuth()
api = StudiesAPI(auth)

studies = api.list_studies()
print(f"\n📚 Found {studies.totalCount} studies.\n")
for s in studies.items:
    print(f" - {s.id}: {s.name} ({s.studyStatus})")

if studies.items:
    study = api.get_study(studies.items[0].id)
    print(f"\n📄 Full details for first study:\n{study.model_dump_json(indent=2)}")

