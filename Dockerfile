ARG STACK_TAG="w_latest"
FROM ghcr.io/lsst/scipipe:al9-${STACK_TAG}

USER root
RUN groupadd -g 1126 -o gu \
    && useradd -u 48045 -g 1126 rubinppb
USER rubinppb
