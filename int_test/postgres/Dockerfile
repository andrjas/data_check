FROM python:3.8

# This Dockerfile creates an image with postgres client and data_check

RUN useradd -m data_check && mkdir /app && chown data_check /app
USER data_check

WORKDIR /app
ENV PATH="/home/data_check/.local/bin/:${PATH}"

RUN python3 -m pip install --user poetry

COPY --chown=data_check pyproject.toml .
COPY --chown=data_check poetry.lock .
RUN poetry install -E postgres --no-root

COPY --chown=data_check . .
COPY --chown=data_check int_test/scripts .
RUN poetry install

ENV POSTGRES_PWD="data_check"

CMD ["sh", "-c", "/app/run_tests.sh postgres"]
