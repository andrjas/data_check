from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from data_check.file_ops import expand_files, read_sql_file

if TYPE_CHECKING:
    from data_check import DataCheck


def load_lookup(
    data_check: DataCheck, lf: Path, lookups_path: Path
) -> tuple[str, list[Any]]:
    lf_path = lf.relative_to(lookups_path)  # remove "lookups"
    if lf_path.parent.parts:
        lf_path_str = "__".join(lf_path.parent.parts) + "__" + lf_path.stem
    else:
        lf_path_str = lf_path.stem
    if data_check.output.verbose:
        data_check.output.print(f"load lookup: {lf_path}")
    res = data_check.sql.run_query(
        read_sql_file(lf, template_data=data_check.template_data)
    )
    first_column = res.iloc[:, 0]
    lkp_value: list[Any] = first_column.values.tolist()  # type: ignore
    return (lf_path_str, lkp_value)


def load_lookups_from_path(data_check: DataCheck) -> dict[str, Any]:
    lookups_path = data_check.config.lookups_path
    lookup_data: dict[str, Any] = {}
    if lookups_path.exists():
        lookup_files = expand_files([lookups_path])
        results: list[tuple[str, list[Any]]] = data_check.runner.run_any(
            load_lookup,
            [
                {"data_check": data_check, "lf": lf, "lookups_path": lookups_path}
                for lf in lookup_files
            ],
        )
        for lf_path, lkp_value in results:
            lookup_data[lf_path] = lkp_value
    return lookup_data
