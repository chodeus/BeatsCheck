FROM alpine:3.23@sha256:5b10f432ef3da1b8d4c7eb6c487f2f5a8f096bc91145e68878dd4a5019afde11

ARG BUILD_DATE
ARG VCS_REF
ARG VERSION
ARG BUILD_NUMBER

LABEL org.opencontainers.image.title="beatscheck" \
      org.opencontainers.image.description="Audio file integrity checker using ffmpeg decode testing" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.authors="chodeus" \
      org.opencontainers.image.source="https://github.com/chodeus/BeatsCheck" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.base.name="alpine:3.23" \
      net.unraid.docker.icon="https://raw.githubusercontent.com/chodeus/BeatsCheck/main/icon.png" \
      build.number="${BUILD_NUMBER}"

ENV PUID=99 \
    PGID=100 \
    UMASK=002 \
    TZ=UTC \
    PYTHONUNBUFFERED=1 \
    CONFIG_DIR=/config \
    BUILD_NUMBER=${BUILD_NUMBER}

RUN apk --no-cache upgrade && \
    apk --no-cache add \
    python3 \
    ffmpeg \
    shadow \
    su-exec \
    tini \
    tzdata

WORKDIR /app

COPY VERSION /app/VERSION
COPY scripts/entrypoint.sh /app/
COPY scripts/delete.sh /app/
COPY scripts/rescan.sh /app/
COPY scripts/reset-webui-password.sh /app/
COPY app/static/ /app/static/
COPY app/webui.py /app/
COPY app/main.py /app/
RUN chmod +x /app/entrypoint.sh /app/delete.sh /app/rescan.sh \
             /app/reset-webui-password.sh && \
    ln -s /app/delete.sh /usr/local/bin/delete && \
    ln -s /app/rescan.sh /usr/local/bin/rescan && \
    ln -s /app/reset-webui-password.sh /usr/local/bin/reset-webui-password

EXPOSE 8484

VOLUME ["/data", "/config"]

STOPSIGNAL SIGTERM

HEALTHCHECK --interval=60s --timeout=5s --start-period=30s --retries=3 \
  CMD pgrep -f "main.py" > /dev/null && \
      { [ ! -f /config/.heartbeat ] || \
        [ "$(( $(date +%s) - $(cat /config/.heartbeat 2>/dev/null || echo 0) ))" -lt 660 ]; } \
      || exit 1

ENTRYPOINT ["/sbin/tini", "--", "/app/entrypoint.sh"]
CMD []
