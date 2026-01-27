FROM python:3.13.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code

COPY pyproject.toml .python-version uv.lock ./

RUN uv sync --locked

COPY main.py src/ ./

ENTRYPOINT [ "python", "main.py" ]