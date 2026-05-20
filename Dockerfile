ARG STACK_TAG="w_latest"
FROM ghcr.io/lsst/scipipe:al9-${STACK_TAG}
USER lsst
RUN <<EOT
  set -ex
  source /opt/lsst/software/stack/loadLSST.bash
  conda install -c conda-forge lsst-efd-client
EOT

USER root
RUN <<EOT
  set -ex
  curl -O -L https://github.com/grafana/loki/releases/download/v2.9.9/logcli-2.9.9.x86_64.rpm
  rpm -i logcli-2.9.9.x86_64.rpm
  rm logcli-2.9.9.x86_64.rpm
  # Fix the bash-completion bug in gdalinfo
  sed -i "s/_gdal-config/_gdal_config/g" \
    /opt/lsst/software/stack/conda/envs/lsst-scipipe-*/share/bash-completion/completions/gdalinfo
EOT

RUN groupadd -g 4085 -o rubin_users \
    && useradd -u 17951 -g 4085 lsstsvc1
USER lsstsvc1

WORKDIR /
COPY scripts scripts/
