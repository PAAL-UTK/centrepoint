from typing import List
from centrepoint.utils.paginate import paginate


class FakePage:
    items: List[int]
    totalCount: int
    limit: int
    offset: int

    def __init__(self, items, totalCount, limit, offset):
        self.items = items
        self.totalCount = totalCount
        self.limit = limit
        self.offset = offset


def test_paginate_multiple_pages():
    calls = []

    def fake_get_page(limit: int, offset: int) -> FakePage:
        calls.append((limit, offset))
        total = 6
        return FakePage(
            items=list(range(offset, min(offset + limit, total))),
            totalCount=total,
            limit=limit,
            offset=offset
        )

    results = list(paginate(fake_get_page, limit=2))
    assert results == [0, 1, 2, 3, 4, 5]
    assert calls == [(2, 0), (2, 2), (2, 4)]


def test_paginate_single_page():
    def fake_get_page(limit: int, offset: int) -> FakePage:
        return FakePage(
            items=[1, 2],
            totalCount=2,
            limit=limit,
            offset=offset
        )

    results = list(paginate(fake_get_page, limit=10))
    assert results == [1, 2]

