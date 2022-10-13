import re
from pathlib import Path

import pytest


def all_click_options():
    main_py = Path(__file__).parent.parent.parent / "data_check" / "cli" / "main.py"
    assert main_py.exists()
    options = re.findall(r'"--?[^"\s]+"', main_py.read_text())
    return [o.rstrip('"').lstrip('"') for o in options]


@pytest.mark.skip(reason="cli subcommands need different parsing. Skipped for now.")
def test_all_options_are_documented():
    all_options = all_click_options()
    print(all_options)
    usage_md = Path(__file__).parent.parent.parent / "docs" / "usage.md"
    assert usage_md.exists()
    usage_txt = usage_md.read_text()

    join = []
    for opt in all_options:
        if re.search(r"\* \`data_check\s+[^\`]*" + opt + r"[\`\s\/]", usage_txt):
            join.append(opt)
    assert set(join) == set(all_options)
