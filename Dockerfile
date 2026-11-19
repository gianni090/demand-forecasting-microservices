FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/* \
    && install -d -o 10001 -g 10001 /data

COPY requirements.txt ./
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY services/ ./services/
COPY mocks/ ./mocks/
COPY api/ ./api/

ARG SERVICE
RUN test -n "$SERVICE" && python -m services.container --help >/dev/null
ENV LAB_SERVICE=${SERVICE}
EXPOSE 8000

USER 10001:10001
CMD ["sh", "-c", "exec python -m services.container \"$LAB_SERVICE\""]
