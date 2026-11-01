import Hls from "hls.js";
import { afterEach, describe, expect, it, vi } from "vitest";

import { setupHlsErrorHandling } from "components/player/hlsplayer/utils";

describe("playlist resynchronization", () => {
  afterEach(() => vi.useRealTimers());

  it.each([
    { details: Hls.ErrorDetails.LEVEL_LOAD_ERROR, response: { code: 410 } },
    { details: Hls.ErrorDetails.MANIFEST_LOAD_ERROR, response: { code: 410 } },
    { details: Hls.ErrorDetails.LEVEL_PARSING_ERROR, fatal: true },
  ])(
    "reinitializes the player after a changed playlist: $details",
    (failure) => {
      vi.useFakeTimers();
      const handlers = new Map<string, (...args: any[]) => void>();
      const hls = {
        on: vi.fn((event: string, handler: (...args: any[]) => void) => {
          handlers.set(event, handler);
        }),
        stopLoad: vi.fn(),
        startLoad: vi.fn(),
      };
      const hlsRef = { current: hls as unknown as Hls };
      const reinitialize = vi.fn();
      const setError = vi.fn();
      setupHlsErrorHandling(hlsRef.current, {
        hlsRef,
        setHlsRefsError: setError,
        delayedInitializationTimeoutRef: { current: undefined },
        delayedRecoveryTimeoutRef: { current: undefined },
        onReinitialize: reinitialize,
      });

      const onError = handlers.get(Hls.Events.ERROR)!;
      onError(Hls.Events.ERROR, {
        type: Hls.ErrorTypes.NETWORK_ERROR,
        error: new Error("playlist changed"),
        fatal: true,
        ...failure,
      });
      expect(hls.stopLoad).toHaveBeenCalledOnce();
      expect(hls.startLoad).not.toHaveBeenCalled();
      expect(setError).toHaveBeenCalledWith(
        hlsRef,
        expect.stringContaining("reconnecting"),
      );
      vi.advanceTimersByTime(1000);
      expect(reinitialize).toHaveBeenCalledOnce();
    },
  );
});
