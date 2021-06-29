FROM python:3.6.2

RUN python -m pip install -U pip && python3 -m pip install poetry

WORKDIR /app

COPY --chown=data_check . .

RUN poetry install
CMD poetry run pytest test
