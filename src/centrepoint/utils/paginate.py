# centrepointAPI/utils/pagination.py

from typing import Callable, Iterator, TypeVar, Protocol

T = TypeVar("T")

class Paginated(Protocol[T]):
    items: list[T]
    totalCount: int
    limit: int
    offset: int

def paginate(get_page: Callable[[int, int], Paginated[T]], limit: int = 100) -> Iterator[T]:
    offset = 0
    while True:
        page = get_page(limit, offset)
        yield from page.items

        offset += page.limit
        if offset >= page.totalCount:
            break


