ARG STACK_TAG="w_2024_30"
FROM lsstsqre/centos:7-stack-lsst_distrib-${STACK_TAG}
WORKDIR /scripts
COPY scripts scripts/
RUN <<EOT
  set -ex
  source /opt/lsst/software/stack/loadLSST.bash
  setup lsst_distrib
EOT
