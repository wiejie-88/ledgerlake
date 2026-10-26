# LedgerLake dashboard: Streamlit reading the serving database as the analyst role. Multi-arch base (arm64 OK).
FROM python:3.11.16-slim-bookworm

COPY docker/dashboard/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --root-user-action=ignore -r /tmp/requirements.txt && pip check

RUN useradd --create-home --uid 1000 dashboard
USER dashboard
WORKDIR /app
COPY --chown=dashboard dashboard /app

EXPOSE 8501
CMD ["streamlit", "run", "app.py"]
