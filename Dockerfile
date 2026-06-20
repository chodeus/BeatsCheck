# ---- ffmpeg fetch stage ----
# Download + SHA-256 verify the static ffmpeg from chodeus/ffmpeg-static (our
# verified mirror of the FFmpeg-project-recommended BtbN builds). Runs on the
# build host; the binary is validated against musl in the final stage below.
FROM --platform=$BUILDPLATFORM alpine:3.23@sha256:5b10f432ef3da1b8d4c7eb6c487f2f5a8f096bc91145e68878dd4a5019afde11 AS ffmpeg-fetch
ARG TARGETARCH
# renovate: datasource=github-releases depName=chodeus/ffmpeg-static
ARG FFMPEG_VERSION=n8.1.2
RUN apk add --no-cache ca-certificates wget
RUN set -eux; \
    arch="${TARGETARCH:-$(uname -m | sed -e 's/x86_64/amd64/' -e 's/aarch64/arm64/')}"; \
    case "$arch" in \
      amd64) asset="ffmpeg-linux64" ;; \
      arm64) asset="ffmpeg-linuxarm64" ;; \
      *) echo "unsupported architecture: $arch" >&2; exit 1 ;; \
    esac; \
    base="https://github.com/chodeus/ffmpeg-static/releases/download/${FFMPEG_VERSION}"; \
    mkdir -p /out; \
    wget -qO /out/ffmpeg "${base}/${asset}"; \
    wget -qO /tmp/SHA256SUMS "${base}/SHA256SUMS"; \
    expected="$(awk -v f="$asset" '$2 == f { print $1 }' /tmp/SHA256SUMS)"; \
    actual="$(sha256sum /out/ffmpeg | awk '{ print $1 }')"; \
    [ -n "$expected" ] && [ "$expected" = "$actual" ] \
      || { echo "ffmpeg SHA-256 mismatch for $asset (want=$expected got=$actual)" >&2; exit 1; }; \
    chmod +x /out/ffmpeg

# ---- final image ----
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
    su-exec \
    tini \
    tzdata

# FFmpeg 8.1.x from chodeus/ffmpeg-static (fetched + verified in the stage
# above), replacing Alpine's older packaged ffmpeg. The self-check fails the
# build loudly if the glibc-static binary can't run on Alpine's musl — turning
# a would-be runtime failure into a build-time one.
COPY --from=ffmpeg-fetch /out/ffmpeg /usr/local/bin/ffmpeg
RUN ffmpeg -version

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
    chmod -R a+rX /app && \
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
