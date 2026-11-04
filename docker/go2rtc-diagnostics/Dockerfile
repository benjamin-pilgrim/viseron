# Diagnostic overlay: the deployed Viseron application stays at b011adbf.
ARG BASE_IMAGE=ghcr.io/benjamin-pilgrim/viseron@sha256:c2cd219ff406adb306f5a9b5b2730117b340957df711071aa23c5fe0efb32c61
FROM ${BASE_IMAGE}
ARG DIAGNOSTIC_COMMIT
COPY --chmod=0755 go2rtc /usr/local/bin/go2rtc
ENV VISERON_FLV_TIMING_DIAGNOSTICS=1
LABEL io.viseron.flv-diagnostics.commit=${DIAGNOSTIC_COMMIT} \
      io.viseron.go2rtc.upstream=b5948cfb25404cc5cb37b166ecaa2dca20b11d4b
