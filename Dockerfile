FROM eclipse-temurin:21-jdk-noble

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3.12 python3.12-venv \
    && rm -rf /var/lib/apt/lists/*

ENV VIRTUAL_ENV=/opt/venv
RUN python3.12 -m venv "$VIRTUAL_ENV"
ENV PATH="$VIRTUAL_ENV/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TELEMETRY_REQUIRE_SPARK_TESTS=1

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY tests ./tests
COPY scenarios ./scenarios
COPY notebooks ./notebooks
COPY sql ./sql
COPY data ./data
COPY docs ./docs

RUN python -m pip install --no-cache-dir -e ".[dev,spark]" \
    && python -c "import sys; assert sys.version_info[:2] == (3, 12)" \
    && java -version

CMD ["python", "-m", "pytest", "--require-spark-tests", "-q"]
