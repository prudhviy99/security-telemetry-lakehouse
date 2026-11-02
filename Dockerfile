FROM eclipse-temurin:21-jammy

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY tests ./tests
COPY scenarios ./scenarios
COPY notebooks ./notebooks
COPY sql ./sql
COPY data ./data
COPY docs ./docs

RUN pip3 install --no-cache-dir -e ".[dev,spark]"

CMD ["pytest", "-q"]
