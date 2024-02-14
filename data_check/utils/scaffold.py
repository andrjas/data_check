import shutil
from pathlib import Path

from data_check.exceptions import DataCheckException

SCAFFOLD_TEMPLATES = "scaffold_templates"


def _create_dir(directory: Path):
    if not directory.exists():
        directory.mkdir()
    elif not directory.is_dir():
        raise DataCheckException(f"{directory} exists, but is not a directory")


def _copy_file(src: Path, dst: Path):
    if not dst.exists():
        shutil.copy(src, dst)
    else:
        raise DataCheckException(f"{dst.name} already exists")


def create_project(project_path: Path):
    cur_folder = Path(__file__).parent
    conf_file = "data_check.yml"
    project_template = cur_folder / SCAFFOLD_TEMPLATES / conf_file

    assert project_template.exists()
    _create_dir(project_path)

    config = project_path / conf_file
    _copy_file(project_template, config)

    checks_path = project_path / "checks"
    checks_path.mkdir(exist_ok=True)


def create_pipeline(pipeline_path: Path):
    cur_folder = Path(__file__).parent
    conf_file = "data_check_pipeline.yml"
    pipeline_template = cur_folder / SCAFFOLD_TEMPLATES / conf_file

    assert pipeline_template.exists()
    _create_dir(pipeline_path)

    config = pipeline_path / conf_file
    _copy_file(pipeline_template, config)

    for f in ("prepare", "step/1/data", "step/1/check"):
        (pipeline_path / f).mkdir(parents=True, exist_ok=True)
