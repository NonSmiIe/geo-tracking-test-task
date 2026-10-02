FROM ghcr.io/astral-sh/uv:0.11.12 AS uv

FROM python:3.12.13-slim AS base
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1 PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
ENV PATH="/app/.venv/bin:$PATH"
COPY pyproject.toml uv.lock ./

FROM base AS test
RUN uv sync --frozen
COPY . .

FROM base AS runtime
RUN uv sync --frozen --no-dev
COPY geo_tracking geo_tracking
COPY generator.py ./
COPY migrations migrations
COPY alembic.ini ./
RUN useradd --uid 10001 --no-create-home app
USER 10001
EXPOSE 8000
ENTRYPOINT ["uvicorn", "--host", "0.0.0.0", "--port", "8000", "--no-access-log", "--log-level", "warning", "--timeout-graceful-shutdown", "15"]
CMD ["--ws-max-size", "65536", "geo_tracking.api.app:app"]
