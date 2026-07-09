# Test runner with Docker CLI and Docker Compose for cluster integration tests.
ARG RUBY_VERSION=4
FROM ruby:${RUBY_VERSION}-bookworm

ARG DOCKER_VERSION=27.5.1
ARG COMPOSE_VERSION=2.32.4

RUN apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
       ca-certificates \
       curl \
  && arch="$(uname -m)" \
  && case "${arch}" in \
       aarch64) docker_arch=aarch64; compose_arch=aarch64 ;; \
       x86_64) docker_arch=x86_64; compose_arch=x86_64 ;; \
       *) echo "unsupported architecture: ${arch}" >&2; exit 1 ;; \
     esac \
  && curl -fsSL "https://download.docker.com/linux/static/stable/${docker_arch}/docker-${DOCKER_VERSION}.tgz" \
       | tar xz -C /usr/local/bin --strip-components=1 docker/docker \
  && mkdir -p /usr/local/lib/docker/cli-plugins \
  && curl -fsSL "https://github.com/docker/compose/releases/download/v${COMPOSE_VERSION}/docker-compose-linux-${compose_arch}" \
       -o /usr/local/lib/docker/cli-plugins/docker-compose \
  && chmod +x /usr/local/lib/docker/cli-plugins/docker-compose \
  && ln -sf /usr/local/lib/docker/cli-plugins/docker-compose /usr/local/bin/docker-compose \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /client
