# 1. Image de base minimale Chainguard, figée par digest (jamais un tag seul)
FROM cgr.dev/chainguard/wolfi-base:latest@sha256:WOLFI_DIGEST
RUN apk add --no-cache python-3.12

# 2. Pas de .pyc, sortie non bufferisée (logs en temps réel)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 3. Répertoire applicatif
WORKDIR /app

# 4-5. Dépendances d'abord (couche stable, mise en cache).
#      pip n'est présent que le temps de ce RUN : confluent-kafka et
#      psycopg2-binary sont des roues précompilées, rien n'appelle pip ensuite.
COPY requirements.txt ./
RUN apk add --no-cache py3.12-pip \
    && pip install --no-cache-dir --break-system-packages -r requirements.txt \
    && apk del --no-cache py3.12-pip

# 6. Code applicatif ensuite (change souvent)
COPY lecteur.py consumer.py ./

# 7. Compte non privilégié, même UID qu'en séance 1
RUN adduser -D -u 10001 appuser \
    && chown -R 10001:10001 /app
USER 10001

# 8. Consommateur de persistance par défaut
CMD ["python3", "consumer.py"]
