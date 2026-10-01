FROM ghcr.io/astral-sh/uv:0.11.12 AS uv
FROM python:3.12.13-slim
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1 PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev
COPY geo_tracking geo_tracking
COPY migrations migrations
COPY alembic.ini ./
RUN useradd --uid 10001 --no-create-home app
USER 10001
ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8000
CMD ["sh", "-c", "alembic upgrade head && exec python -m geo_tracking"]
