# Preserve the deployed FLV timestamp fix and diagnostics; change only storage replay.
FROM ghcr.io/benjamin-pilgrim/viseron@sha256:b34d9667886a20e50d7668bd91f8cb9ce9daa14826a818ffaed2108663840a6c
ARG RECOVERY_COMMIT
COPY viseron/components/storage/pending_journal.py /src/viseron/components/storage/pending_journal.py
LABEL io.viseron.storage-recovery.commit=${RECOVERY_COMMIT}
