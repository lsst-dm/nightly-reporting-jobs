ARG STACK_TAG="w_2024_30"
FROM lsstsqre/centos:7-stack-lsst_distrib-${STACK_TAG}
USER root
RUN <<EOT
  curl -O -L https://github.com/grafana/loki/releases/download/v2.9.9/logcli-2.9.9.x86_64.rpm
  rpm -i logcli-2.9.9.x86_64.rpm
  rm logcli-2.9.9.x86_64.rpm
EOT
USER lsst
WORKDIR /
COPY scripts scripts/
