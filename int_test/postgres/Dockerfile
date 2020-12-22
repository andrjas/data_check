FROM python:3.8

# This Dockerfile creates an image with postgres client and data_check

RUN useradd -m data_check && mkdir /app && chown data_check /app
USER data_check

WORKDIR /app
ENV PATH="/home/data_check/.local/bin/:${PATH}"

COPY --chown=data_check requirements.txt .
COPY --chown=data_check dependencies/requirements-dev.txt .
COPY --chown=data_check dependencies/requirements-postgres.txt .


RUN python3 -m pip install --user --no-cache-dir -r requirements.txt && \
    python3 -m pip install --user --no-cache-dir -r requirements-dev.txt && \
    python3 -m pip install --user --no-cache-dir -r requirements-postgres.txt

COPY --chown=data_check . .
COPY --chown=data_check int_test/scripts .

RUN python3 -m pip install --user -e .

CMD ["sh", "-c", "/app/run_tests.sh postgres"]
