import Hls, { type ErrorData, type HlsConfig } from "hls.js";
import React from "react";

import { getToken } from "lib/tokens";
import * as types from "lib/types";

export enum HlsPlaybackMode {
  Synced = "synced",
  Vod = "vod",
  Live = "live",
}

const HLS_LIVE_SYNC_SEGMENT_COUNT = 3;
const HLS_LIVE_MAX_LATENCY_SEGMENT_COUNT = 6;
const HLS_DISABLED_LIVE_MAX_LATENCY = Infinity;
export const HLS_SEEK_STEP_SECONDS = 10;

const BASE_HLS_CONFIG = {
  autoStartLoad: false,
  maxBufferLength: 30, // 30 seconds of forward buffer
  backBufferLength: 15, // 15 seconds of back buffer
  liveDurationInfinity: false, // Has to be false to seek backwards
} satisfies Partial<HlsConfig>;

export const HLS_CONFIG_BY_PLAYBACK_MODE = {
  // Synced playback is app-controlled wall-clock playback used by the Events grid.
  // hls.js must not independently seek to the live edge in this mode.
  [HlsPlaybackMode.Synced]: {
    ...BASE_HLS_CONFIG,
    liveSyncDurationCount: HLS_LIVE_SYNC_SEGMENT_COUNT,
    liveMaxLatencyDurationCount: HLS_DISABLED_LIVE_MAX_LATENCY,
  },
  [HlsPlaybackMode.Vod]: {
    ...BASE_HLS_CONFIG,
    liveSyncDurationCount: HLS_LIVE_SYNC_SEGMENT_COUNT,
    liveMaxLatencyDurationCount: HLS_DISABLED_LIVE_MAX_LATENCY,
  },
  [HlsPlaybackMode.Live]: {
    ...BASE_HLS_CONFIG,
    liveSyncDurationCount: HLS_LIVE_SYNC_SEGMENT_COUNT,
    liveMaxLatencyDurationCount: HLS_LIVE_MAX_LATENCY_SEGMENT_COUNT,
  },
} satisfies Record<HlsPlaybackMode, Partial<HlsConfig>>;

// Creates an HLS.js instance with standard configuration and authentication setup.
export function createHlsInstance(
  auth: types.AuthEnabledResponse,
  hlsClientIdRef: React.MutableRefObject<string>,
  modeOrConfig: HlsPlaybackMode | Partial<HlsConfig> = {},
): Hls {
  const config =
    typeof modeOrConfig === "string"
      ? HLS_CONFIG_BY_PLAYBACK_MODE[modeOrConfig]
      : {
          ...HLS_CONFIG_BY_PLAYBACK_MODE[HlsPlaybackMode.Live],
          ...modeOrConfig,
        };
  return new Hls({
    ...config,
    async xhrSetup(xhr, _url) {
      xhr.withCredentials = true;
      if (auth.enabled) {
        const token = await getToken();
        if (token) {
          xhr.setRequestHeader("X-Requested-With", "XMLHttpRequest");
          xhr.setRequestHeader("Authorization", `Bearer ${token}`);
        }
      }
      xhr.setRequestHeader("Hls-Client-Id", hlsClientIdRef.current);
    },
  });
}

export function startLoadAtCurrentTime(hls: Hls): void {
  const currentTime = hls.media?.currentTime;
  if (Number.isFinite(currentTime)) {
    hls.startLoad(currentTime);
    return;
  }
  hls.startLoad();
}

export function startLoadAtBeginning(hls: Hls): void {
  hls.startLoad(0);
}

function getBoundedMediaTime(
  media: HTMLMediaElement,
  mediaTime: number,
): number | null {
  if (!Number.isFinite(mediaTime)) {
    return null;
  }
  const lowerBoundedTime = Math.max(mediaTime, 0);
  const seekable = media.seekable;
  if (seekable?.length) {
    const seekableStart = seekable.start(0);
    const seekableEnd = seekable.end(seekable.length - 1);
    if (Number.isFinite(seekableStart) && Number.isFinite(seekableEnd)) {
      return Math.min(Math.max(lowerBoundedTime, seekableStart), seekableEnd);
    }
  }
  if (Number.isFinite(media.duration)) {
    return Math.min(lowerBoundedTime, media.duration);
  }
  return lowerBoundedTime;
}

export function seekMediaAndStartLoad(
  hls: Hls,
  media: HTMLMediaElement,
  mediaTime: number,
): void {
  const boundedMediaTime = getBoundedMediaTime(media, mediaTime);
  if (boundedMediaTime === null) {
    return;
  }
  media.currentTime = boundedMediaTime;
  hls.startLoad(boundedMediaTime);
}

export function seekHlsByOffset(hls: Hls, offsetSeconds: number): void {
  if (!hls.media) {
    return;
  }
  seekMediaAndStartLoad(hls, hls.media, hls.media.currentTime + offsetSeconds);
}

export function seekHlsToLiveEdge(
  hls: Hls,
  liveEdgeDelaySeconds: number,
): void {
  if (!hls.media || !Number.isFinite(hls.media.duration)) {
    return;
  }
  seekMediaAndStartLoad(
    hls,
    hls.media,
    hls.media.duration - liveEdgeDelaySeconds,
  );
}

// Ignorable HLS error details that don't require user notification.
// - FRAG_GAP: Natural since recordings are not necessarily continuous
// - BUFFER_STALLED_ERROR: Happens when too close to live edge, automatically stabilizes
// - BUFFER_SEEK_OVER_HOLE: Happens when seeking over a gap in the recording
const IGNORABLE_ERROR_DETAILS = new Set([
  Hls.ErrorDetails.FRAG_GAP,
  Hls.ErrorDetails.BUFFER_STALLED_ERROR,
  Hls.ErrorDetails.BUFFER_SEEK_OVER_HOLE,
]);

export interface HlsErrorHandlerOptions {
  hlsRef: React.MutableRefObject<Hls | null>;
  setHlsRefsError: (
    hlsRef: React.MutableRefObject<Hls | null>,
    error: string | null,
  ) => void;
  delayedInitializationTimeoutRef: React.MutableRefObject<
    NodeJS.Timeout | undefined
  >;
  delayedRecoveryTimeoutRef: React.MutableRefObject<NodeJS.Timeout | undefined>;
  onReinitialize: () => void;
}

// A video-only fMP4 run and a muxed audio/video run require different MSE
// buffer layouts. Keep the old buffer playable until the new run starts.
function setupBufferLayoutRecovery(
  hls: Hls,
  hlsRef: React.MutableRefObject<Hls | null>,
): (data: ErrorData) => boolean {
  let activeBuffers = new Set<string>();
  let incompatibleLayout = false;
  let pending: { cancel: () => void } | null = null;

  const cancel = () => {
    pending?.cancel();
    pending = null;
  };
  const reset = () => {
    cancel();
    activeBuffers.clear();
    incompatibleLayout = false;
  };
  hls.on(Hls.Events.BUFFER_CREATED, (_event, data) => {
    activeBuffers = new Set(Object.keys(data.tracks));
  });
  hls.on(Hls.Events.BUFFER_CODECS, (_event, data) => {
    incompatibleLayout =
      activeBuffers.size > 0 &&
      ["audio", "video", "audiovideo"].some(
        (name) => name in data && !activeBuffers.has(name),
      );
  });
  hls.on(Hls.Events.MEDIA_DETACHING, reset);
  hls.on(Hls.Events.BUFFER_RESET, reset);
  hls.on(Hls.Events.DESTROYING, reset);

  return (data) => {
    if (
      !incompatibleLayout ||
      data.details !== Hls.ErrorDetails.BUFFER_APPEND_ERROR
    )
      return false;
    const media = hls.media;
    const boundary = data.frag?.start;
    if (!media || boundary === undefined || !Number.isFinite(boundary))
      return false;
    if (pending) return true;

    const recover = (seeking = false) => {
      if (hlsRef.current !== hls) {
        cancel();
        return;
      }
      if (!seeking && media.currentTime < boundary - 0.1 && !media.ended)
        return;
      const position = seeking
        ? media.currentTime
        : Math.max(media.currentTime, boundary);
      const wasPlaying = !media.paused;
      cancel();
      hls.recoverMediaError();
      seekMediaAndStartLoad(hls, media, position);
      if (wasPlaying) media.play().catch(() => {});
    };
    const onTime = () => recover();
    const onSeek = () => recover(true);
    pending = {
      cancel: () => {
        media.removeEventListener("timeupdate", onTime);
        media.removeEventListener("seeking", onSeek);
      },
    };
    media.addEventListener("timeupdate", onTime);
    media.addEventListener("seeking", onSeek);
    hls.stopLoad();
    recover();
    return true;
  };
}

// Sets up standardized error handling for an HLS.js instance.
// Handles both recoverable and fatal errors with appropriate retry strategies.
export function setupHlsErrorHandling(
  hls: Hls,
  options: HlsErrorHandlerOptions,
): void {
  const {
    hlsRef,
    setHlsRefsError,
    delayedInitializationTimeoutRef,
    delayedRecoveryTimeoutRef,
    onReinitialize,
  } = options;

  // Reset error state when a fragment is loaded successfully
  hls.on(Hls.Events.FRAG_LOADED, () => {
    setHlsRefsError(hlsRef, null);
  });

  // Delayed initialization retry for fatal errors
  const delayedInitialization = () => {
    if (delayedInitializationTimeoutRef.current) {
      return;
    }

    delayedInitializationTimeoutRef.current = setTimeout(() => {
      onReinitialize();
      delayedInitializationTimeoutRef.current = undefined;
    }, 5000);
  };

  // Delayed recovery for media errors
  const delayedRecovery = () => {
    if (delayedRecoveryTimeoutRef.current) {
      return;
    }

    delayedRecoveryTimeoutRef.current = setTimeout(() => {
      hlsRef.current?.recoverMediaError();
      delayedRecoveryTimeoutRef.current = undefined;
    }, 5000);
  };

  const recoverLayout = setupBufferLayoutRecovery(hls, hlsRef);

  // Main error handler
  hls.on(Hls.Events.ERROR, (_event, data) => {
    if (hlsRef.current !== hls) return;
    // A seek intentionally cancels the previous request.
    if (!data.fatal && data.details === Hls.ErrorDetails.INTERNAL_ABORTED)
      return;
    if (recoverLayout(data)) {
      if (delayedRecoveryTimeoutRef.current) {
        clearTimeout(delayedRecoveryTimeoutRef.current);
        delayedRecoveryTimeoutRef.current = undefined;
      }
      setHlsRefsError(hlsRef, null);
      return;
    }
    // Check if this is an ignorable error
    if (!IGNORABLE_ERROR_DETAILS.has(data.details)) {
      console.log("HLSJS Error:", data);
      setHlsRefsError(hlsRef, data.error.message.slice(0, 200));
    }

    if (data.fatal) {
      switch (data.type) {
        case Hls.ErrorTypes.NETWORK_ERROR:
          if (data.details === Hls.ErrorDetails.MANIFEST_LOAD_ERROR) {
            delayedInitialization();
          }
          if (hlsRef.current) {
            startLoadAtCurrentTime(hlsRef.current);
          }
          break;

        case Hls.ErrorTypes.MEDIA_ERROR:
          delayedRecovery();
          break;

        default:
          delayedInitialization();
          break;
      }
    }
  });
}

// Cleans up an HLS instance and associated timeouts.
export function cleanupHlsInstance(
  hlsRef: React.MutableRefObject<Hls | null>,
  removeHlsRef: (ref: React.MutableRefObject<Hls | null>) => void,
  delayedInitializationTimeoutRef: React.MutableRefObject<
    NodeJS.Timeout | undefined
  >,
  delayedRecoveryTimeoutRef?: React.MutableRefObject<
    NodeJS.Timeout | undefined
  >,
): void {
  if (hlsRef.current) {
    hlsRef.current.destroy();
    removeHlsRef(hlsRef);
    hlsRef.current = null;
  }
  if (delayedInitializationTimeoutRef.current) {
    clearTimeout(delayedInitializationTimeoutRef.current);
  }
  if (delayedRecoveryTimeoutRef?.current) {
    clearTimeout(delayedRecoveryTimeoutRef.current);
  }
}
