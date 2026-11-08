# ------------------------------------------------------------------
# JupyterLab des TP : client Python Kafka (confluent-kafka) et CLI Docker.
#
# La CLI Kafka n'est pas installee ici : les exemples en ligne de
# commande s'executent dans le conteneur tools (docker exec), avec les
# memes outils que dans les enonces Markdown.
# ------------------------------------------------------------------
FROM docker:29.9.0-cli AS docker-cli

# confluent-kafka ne publie pas de wheel pour Alpine (musl) : image Debian.
FROM python:3.13.15-slim-trixie

# CLI Docker et plugin Compose : binaires statiques, independants de la distribution.
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker-cli /usr/local/libexec/docker/cli-plugins/docker-compose \
     /usr/local/libexec/docker/cli-plugins/docker-compose

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

ENV PYTHONUNBUFFERED=1
EXPOSE 8888
