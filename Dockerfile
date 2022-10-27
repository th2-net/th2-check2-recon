FROM python:3.9-slim
ARG app_name
ARG app_version

# Install poetry
RUN pip install -U pip \
    && curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
ENV PATH="${PATH}:/root/.poetry/bin"

WORKDIR /usr/src/app
COPY . .
RUN poetry config virtualenvs.create false \
  && poetry install --no-interaction --no-ansi
