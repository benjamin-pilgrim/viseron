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

describe("timeline request cancellation", () => {
  afterEach(() => vi.restoreAllMocks());

  function createHandler() {
    const handlers = new Map<string, (...args: any[]) => void>();
    const hls = {
      on: vi.fn((event: string, handler: (...args: any[]) => void) => {
        handlers.set(event, handler);
      }),
      stopLoad: vi.fn(),
      startLoad: vi.fn(),
      recoverMediaError: vi.fn(),
    };
    const hlsRef = { current: hls as unknown as Hls };
    const setError = vi.fn();
    const reinitialize = vi.fn();
    vi.spyOn(console, "log").mockImplementation(() => {});
    setupHlsErrorHandling(hlsRef.current, {
      hlsRef,
      setHlsRefsError: setError,
      delayedInitializationTimeoutRef: { current: undefined },
      delayedRecoveryTimeoutRef: { current: undefined },
      onReinitialize: reinitialize,
    });
    return {
      hls,
      hlsRef,
      setError,
      reinitialize,
      onError: handlers.get(Hls.Events.ERROR)!,
    };
  }

  it("does not display or recover a nonfatal aborted fragment request during a seek", () => {
    const { hls, setError, reinitialize, onError } = createHandler();
    onError(Hls.Events.ERROR, {
      type: Hls.ErrorTypes.NETWORK_ERROR,
      details: Hls.ErrorDetails.INTERNAL_ABORTED,
      error: new Error("aborted"),
      fatal: false,
    });
    expect(setError).not.toHaveBeenCalled();
    expect(console.log).not.toHaveBeenCalled();
    expect(hls.startLoad).not.toHaveBeenCalled();
    expect(hls.stopLoad).not.toHaveBeenCalled();
    expect(hls.recoverMediaError).not.toHaveBeenCalled();
    expect(reinitialize).not.toHaveBeenCalled();
  });

  it.each([
    { details: Hls.ErrorDetails.INTERNAL_ABORTED, fatal: true },
    { details: Hls.ErrorDetails.FRAG_LOAD_ERROR, fatal: false },
    { details: Hls.ErrorDetails.FRAG_LOAD_TIMEOUT, fatal: false },
  ])(
    "still displays real network failures: $details (fatal=$fatal)",
    (failure) => {
      const { hlsRef, setError, onError } = createHandler();
      onError(Hls.Events.ERROR, {
        type: Hls.ErrorTypes.NETWORK_ERROR,
        error: new Error("aborted"),
        ...failure,
      });
      expect(setError).toHaveBeenCalledWith(hlsRef, "aborted");
    },
  );
});
