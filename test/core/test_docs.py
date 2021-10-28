from os import O_TRUNC
from pathlib import Path
import re


def all_click_options():
    main_py = Path(__file__).parent.parent.parent / "data_check" / "__main__.py"
    assert main_py.exists()
    options = re.findall(r'"--?[^"\s]+"', main_py.read_text())
    return [o.rstrip('"').lstrip('"') for o in options]


def test_all_options_are_documented():
    all_options = all_click_options()
    print(all_options)
    usage_md = Path(__file__).parent.parent.parent / "docs" / "usage.md"
    assert usage_md.exists()
    usage_txt = usage_md.read_text()

    join = []
    for opt in all_options:
        if re.search(r"\* \`data_check\s+[^\`]*" + opt + "[\`\s\/]", usage_txt):
            join.append(opt)
    assert set(join) == set(all_options)