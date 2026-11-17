FROM python:3.12-slim

# Instalujemy narzędzie uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Kopiujemy pliki konfiguracyjne, README oraz cały katalog src
COPY pyproject.toml uv.lock README.md ./
COPY src/ src/

# Synchronizujemy zależności
RUN uv sync --frozen --no-dev

# Domyślna komenda uruchamiająca scheduler
CMD ["uv", "run", "python", "-m", "air_quality_ml.scheduler"]