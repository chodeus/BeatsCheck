# ---- ffmpeg fetch stage ----
# Download + SHA-256 verify the static ffmpeg from chodeus/ffmpeg-static. The
# linux assets are built there from pinned upstream source (LGPL-3.0), not
# mirrored — only that repo's Windows assets are BtbN mirrors, and we take none.
# Runs on the build host; validated against musl in the final stage below.
FROM --platform=$BUILDPLATFORM alpine:3.24@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b AS ffmpeg-fetch
ARG TARGETARCH
# renovate: datasource=github-releases depName=chodeus/ffmpeg-static
ARG FFMPEG_VERSION=n9.0.1
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
FROM alpine:3.24@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b

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

# The pinned FFmpeg from the fetch stage above (see FFMPEG_VERSION) is copied
# into the final image; no ffmpeg is installed from apk. The self-check fails
# the build loudly if the static binary can't run on Alpine's musl — turning a
# would-be runtime failure into a build-time one.
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
# Bytecode baked here; PYTHONDONTWRITEBYTECODE keeps /app read-only at runtime.
# The assertion replaces `chmod -R a+rX`, which re-materialised the tree in a layer.
RUN chmod +x /app/entrypoint.sh /app/delete.sh /app/rescan.sh \
             /app/reset-webui-password.sh && \
    python3 -m compileall -q /app/main.py /app/webui.py && \
    unreadable="$(find /app \( -type f ! -perm -0004 \) -o \( -type d ! -perm -0005 \) | head -20)"; \
    if [ -n "$unreadable" ]; then echo "not world-readable:"; echo "$unreadable"; exit 1; fi && \
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
