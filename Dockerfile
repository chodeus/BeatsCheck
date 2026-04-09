FROM alpine:3.23

ARG BUILD_DATE
ARG VCS_REF
ARG VERSION

LABEL org.opencontainers.image.title="beatscheck" \
      org.opencontainers.image.description="Audio file integrity checker using ffmpeg decode testing" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.authors="chodeus" \
      org.opencontainers.image.source="https://github.com/chodeus/BeatsCheck" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.base.name="alpine:3.21"

ENV PUID=99 \
    PGID=100 \
    UMASK=002 \
    TZ=UTC \
    PYTHONUNBUFFERED=1 \
    MUSIC_DIR=/music \
    OUTPUT_DIR=/corrupted \
    CONFIG_DIR=/config \
    MODE=report \
    WORKERS=4 \
    RUN_INTERVAL=0 \
    DELETE_AFTER=0 \
    LOG_LEVEL=INFO

RUN apk --no-cache add \
    python3 \
    ffmpeg \
    shadow \
    su-exec \
    tini \
    tzdata

WORKDIR /app

COPY beats_check.py /app/
COPY entrypoint.sh /app/
COPY delete.sh /app/
COPY rescan.sh /app/
RUN chmod +x /app/entrypoint.sh /app/delete.sh /app/rescan.sh

VOLUME ["/music", "/corrupted", "/config"]

STOPSIGNAL SIGTERM

HEALTHCHECK --interval=60s --timeout=5s --start-period=30s --retries=3 \
  CMD pgrep -f "beats_check.py" > /dev/null && \
      { [ ! -f /config/.heartbeat ] || \
        [ "$(( $(date +%s) - $(cat /config/.heartbeat 2>/dev/null || echo 0) ))" -lt 660 ]; } \
      || exit 1

ENTRYPOINT ["/sbin/tini", "--", "/app/entrypoint.sh"]
CMD []
