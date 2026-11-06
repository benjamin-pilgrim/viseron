# FLV audio recovery overlay on the deployed storage-recovery application.
ARG BASE_IMAGE=ghcr.io/benjamin-pilgrim/viseron@sha256:671793e54a0bb3338870308144e11b610203a4ade79e5aacb2356a936d886560
FROM ${BASE_IMAGE}
ARG DIAGNOSTIC_COMMIT
COPY --chmod=0755 go2rtc /usr/local/bin/go2rtc
COPY --chmod=0644 stream.py /src/viseron/components/ffmpeg/stream.py
COPY storage/ /src/viseron/components/storage/
COPY frontend-dist/ /src/viseron/frontend/
ENV VISERON_FLV_TIMING_DIAGNOSTICS=1
LABEL io.viseron.flv-diagnostics.commit=${DIAGNOSTIC_COMMIT} \
      io.viseron.go2rtc.upstream=b5948cfb25404cc5cb37b166ecaa2dca20b11d4b
