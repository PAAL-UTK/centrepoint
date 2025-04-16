from centrepoint.cli import list_subjects
from types import SimpleNamespace
import pytest


def test_main_lists_subjects(monkeypatch):
    # Fake result set
    fake_rows = [(101, "SUBJ_A"), (102, "SUBJ_B")]

    class FakeCursor:
        def execute(self, sql):
            self.sql = sql
            return self
        def fetchall(self):
            return fake_rows
        def close(self):
            pass

    class FakeConn:
        def __init__(self, *args, **kwargs): pass
        def execute(self, sql):
            self._sql = sql
            return FakeCursor()
        def close(self): pass

    monkeypatch.setattr(list_subjects, "duckdb", SimpleNamespace(connect=lambda path: FakeConn()))
    monkeypatch.setattr(list_subjects, "Console", lambda: SimpleNamespace(print=lambda *a, **k: None))

    monkeypatch.setattr(
        list_subjects, "get_parser",
        lambda: SimpleNamespace(parse_args=lambda: SimpleNamespace(filter=None))
    )

    list_subjects.main()

