# 1. Image de base légère, version figée (jamais latest)
FROM python:3.12-slim

# 2. Comportement de Python : pas de .pyc, sortie non bufferisée (docker logs)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 3. Répertoire applicatif
WORKDIR /app

# 4-5. Dépendances d'abord : couche qui change rarement, donc mise en cache
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# 6. Code applicatif ensuite : change souvent, n'invalide que cette couche
COPY producer.py ./

# 7. Compte non privilégié (UID 10001) + droits sur le répertoire de travail
#    (/app/logs créé ici pour que le volume nommé hérite du bon propriétaire)
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin appuser \
    && mkdir -p /app/logs \
    && chown -R 10001:10001 /app

# 8. Bascule définitive, avant CMD
USER 10001

# 9. Commande par défaut
CMD ["python", "producer.py"]
