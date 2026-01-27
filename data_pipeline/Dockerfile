FROM python:3.13.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /app
# Use the python virtual environment created by uv
ENV PATH="app/.venv/bin:$PATH"

COPY pyproject.toml .python-version uv.lock ./

RUN uv sync --locked

COPY main.py . 
COPY  src/ src/

ENTRYPOINT ["python", "main.py" ]