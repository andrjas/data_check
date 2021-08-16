from dateutil.parser import isoparse
from typing import List


def date_parser(ds):
    if isinstance(ds, str):
        return isoparse(ds)
    return ds


def parse_date_hint(query: str) -> List[str]:
    lines = [l.strip() for l in query.splitlines()]
    comment_lines = [
        l.replace("--", "", 1).strip() for l in lines if l.startswith("--")
    ]
    date_hints = [
        l.replace("date:", "", 1) for l in comment_lines if l.startswith("date:")
    ]

    hints = []
    for dh in date_hints:
        _hints = dh.split(",")
        hints.extend(h.strip() for h in _hints)
    return hints
