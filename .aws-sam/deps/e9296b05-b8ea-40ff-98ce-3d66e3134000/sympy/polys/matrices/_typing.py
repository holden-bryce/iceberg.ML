from typing import TypeVar, Protocol


T = TypeVar('T')


class RingElement(Protocol):
    """A ring element.

    Must support ``+``, ``-``, ``*``, ``**`` and ``-``.
    """
    def __add__(self: T, other: T, /) -> T: ...
    def __sub__(self: T, other: T, /) -> T: ...
    def __mul__(self: T, other: T, /) -> T: ...
    def __pow__(self: T, other: int, /) -> T: ...
    def __neg__(self: T, /) -> T: ...
