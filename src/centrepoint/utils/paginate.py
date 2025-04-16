# centrepointAPI/utils/pagination.py

from typing import Callable, Iterator, TypeVar, Protocol

T = TypeVar("T")

class Paginated(Protocol[T]):
    """Protocol representing a paginated response object."""

    items: list[T]
    totalCount: int
    limit: int
    offset: int

def paginate(get_page: Callable[[int, int], Paginated[T]], limit: int = 100) -> Iterator[T]:
    """Iterates over paginated API results by repeatedly calling the provided page-fetch function.

    Args:
        get_page (Callable[[int, int], Paginated[T]]): Function that returns a Paginated page given a limit and offset.
        limit (int, optional): Number of items to request per page. Defaults to 100.

    Yields:
        Iterator[T]: Items from each paginated response.
    """
    offset = 0
    while True:
        page = get_page(limit, offset)
        yield from page.items

        offset += page.limit
        if offset >= page.totalCount:
            break
