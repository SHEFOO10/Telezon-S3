FROM python:3.12-alpine

# Install build dependencies & uv binary
RUN apk add --no-cache \
    gcc \
    musl-dev \
    python3-dev \
    libffi-dev \
    make
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

# Install dependencies directly from pyproject.toml
COPY pyproject.toml .
RUN uv pip install --system -r pyproject.toml

# Copy application code
COPY . .

EXPOSE 8000

CMD ["sh", "-c", "fastapi run --host ${HOST:-0.0.0.0} --port ${PORT:-8000} app/main.py"]