FROM python:3.9

RUN python -m pip install -U pip && python3 -m pip install poetry

COPY --chown=data_check . .

RUN poetry install
CMD poetry run pytest test
