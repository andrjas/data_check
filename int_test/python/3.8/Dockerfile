FROM python:3.8

RUN python -m pip install -U pip && python3 -m pip install poetry

COPY --chown=data_check . .

RUN poetry install
CMD poetry run pytest test
