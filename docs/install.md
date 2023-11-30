# Installing data_check

Depending on your system there are many ways to install data_check. Generally, the steps are always the same:

- create a virtual environment
- activate the virtual environment
- optionally: install pip
- install data_check

You should then be able to run `data_check`.

To run data_check next time in a new terminal you must just activate the virtual environment.

Note: While the tool is called 'data_check' the package that you install is called 'data-check'.

## With pipx

The easiest way to install data_check is to use [pipx](https://github.com/pipxproject/pipx):

`pipx install data-check`

To upgrade data_check via pipx: `pipx upgrade data-check`

## With Anaconda/Miniconda

Create a new conda environment:

`conda create --name data_check python=3.11`

Activate the environment:

`conda activate data_check`

and install pip:

`conda install pip`

Install data_check via pip:

`python3 -m pip install data-check`

To upgrade data_check in the conda environment: `python3 -m pip install data-check --upgrade`

### With venv

Create a virtual environment (`<venv>` point to a relative or absolute path, e.g. c:\venvs\data_check)

`python3 -m venv <venv>`

Activate the environment:

Bash: `source <venv>/bin/activate`
PowerShell: `<venv>\Scripts\Activate.ps1`

The [Python documentation](https://docs.python.org/3/library/venv.html) has more options how to enable a virtual environment.

Install data_check via pip:

`python3 -m pip install data-check`

To upgrade data_check in the virtual environment: `python3 -m pip install data-check --upgrade`

## Databases

Installing data_check alone will only support SQLite, which is bundled with Python. You need additional drivers for other databases.
See [databases](databases.md) for instructions for each supported database.
