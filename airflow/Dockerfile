# Image Airflow officielle, version FIGÉE (jamais "latest" : le comportement du scheduler
# change entre versions mineures, et votre corrigé ne serait plus reproductible).
# 2.10.5 = dernière 2.x ; Python 3.12 comme le producteur et le consommateur.
FROM apache/airflow:2.10.5-python3.12

# L'image officielle tourne déjà en utilisateur non-root (airflow, uid 50000).
# pip s'exécute donc sans privilège : c'est voulu, ne repassez pas root.
# --no-cache-dir : on n'embarque pas le cache pip dans la couche.
RUN pip install --no-cache-dir \
      confluent-kafka==2.6.1
