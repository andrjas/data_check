from setuptools import setup, find_packages
from pathlib import Path


def get_version(rel_path):
    for line in Path(rel_path).read_text().splitlines():
        if line.startswith("__version__"):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")


long_description = Path("README.md").read_text()

requirements = Path("requirements.txt").read_text().splitlines()
postgres_requirements = (
    Path("dependencies/requirements-postgres.txt").read_text().splitlines()
)
oracle_requirements = (
    Path("dependencies/requirements-oracle.txt").read_text().splitlines()
)
mysql_requirements = (
    Path("dependencies/requirements-mysql.txt").read_text().splitlines()
)
mssql_requirements = (
    Path("dependencies/requirements-mssql.txt").read_text().splitlines()
)

setup(
    name="data_check",
    version=get_version("data_check/__init__.py"),
    packages=find_packages(),
    entry_points={
        "console_scripts": ["data_check = data_check.__main__:main"],
    },
    description="simple data validation",
    author="Andreas Rjasanow",
    author_email="andrjas@gmail.com",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/andrjas/data_check",
    install_requires=requirements,
    extras_require={
        "postgres": postgres_requirements,
        "oracle": oracle_requirements,
        "mysql": mysql_requirements,
        "mssql": mssql_requirements,
    },
    python_requires=">=3.6",
    keywords="data validation testing quality",
    license="MIT",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Other Audience",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Database",
        "Topic :: Software Development",
        "Topic :: Software Development :: Testing",
    ],
)
