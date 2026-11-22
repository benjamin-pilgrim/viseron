import Hls from "hls.js";

import { setupHlsErrorHandling } from "components/player/hlsplayer/utils";

function player() {
  const listeners = new Map<
    string,
    ((event: string, data: unknown) => void)[]
  >();
  const media = document.createElement("video");
  Object.defineProperty(media, "paused", { value: false, configurable: true });
  media.play = vi.fn().mockResolvedValue(undefined);
  const trigger = (event: string, data?: unknown) =>
    listeners.get(event)?.forEach((fn) => fn(event, data));
  const instance = {
    media,
    on: (event: string, listener: (event: string, data: unknown) => void) => {
      listeners.set(event, [...(listeners.get(event) ?? []), listener]);
    },
    stopLoad: vi.fn(),
    startLoad: vi.fn(),
    recoverMediaError: vi.fn(() => trigger(Hls.Events.MEDIA_DETACHING)),
    destroy: vi.fn(() => trigger(Hls.Events.DESTROYING)),
  };
  const hls = instance as unknown as Hls;
  const hlsRef = { current: hls as Hls | null };
  const error = vi.fn();
  setupHlsErrorHandling(hls, {
    hlsRef,
    setHlsRefsError: error,
    delayedInitializationTimeoutRef: { current: undefined },
    delayedRecoveryTimeoutRef: { current: undefined },
    onReinitialize: vi.fn(),
  });
  trigger(Hls.Events.BUFFER_CREATED, { tracks: { video: {} } });
  return { media, instance, trigger, error, hlsRef };
}
const layoutError = {
  type: Hls.ErrorTypes.MEDIA_ERROR,
  details: Hls.ErrorDetails.BUFFER_APPEND_ERROR,
  fatal: true,
  sourceBufferName: "audiovideo",
  frag: { start: 8 },
  error: new Error("missing audiovideo buffer"),
};
beforeEach(() => {
  vi.useFakeTimers();
  vi.spyOn(console, "log").mockImplementation(() => {});
});
afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});
test("finishes buffered video before rebuilding for added audio", () => {
  const p = player();
  p.media.currentTime = 2;
  p.trigger(Hls.Events.BUFFER_CODECS, { audiovideo: {} });
  p.trigger(Hls.Events.ERROR, layoutError);
  expect(p.media.currentTime).toBe(2);
  expect(p.instance.recoverMediaError).not.toHaveBeenCalled();
  expect(p.error.mock.calls.every(([, message]) => message === null)).toBe(
    true,
  );
  p.media.currentTime = 7.98;
  p.media.dispatchEvent(new Event("timeupdate"));
  expect(p.media.currentTime).toBe(8);
  expect(p.instance.startLoad).toHaveBeenCalledWith(8);
  expect(p.instance.recoverMediaError).toHaveBeenCalledTimes(1);
});
test("a seek during the pending layout change loads the requested position", () => {
  const p = player();
  p.media.currentTime = 2;
  p.trigger(Hls.Events.BUFFER_CODECS, { audiovideo: {} });
  p.trigger(Hls.Events.ERROR, layoutError);
  p.media.currentTime = 1;
  p.media.dispatchEvent(new Event("seeking"));
  expect(p.media.currentTime).toBe(1);
  expect(p.instance.startLoad).toHaveBeenCalledWith(1);
  expect(p.instance.recoverMediaError).toHaveBeenCalledTimes(1);
});
test("destroying a player cancels pending boundary recovery", () => {
  const p = player();
  p.trigger(Hls.Events.BUFFER_CODECS, { audiovideo: {} });
  p.trigger(Hls.Events.ERROR, layoutError);
  p.instance.destroy();
  p.hlsRef.current = null;
  p.media.currentTime = 8;
  p.media.dispatchEvent(new Event("timeupdate"));
  expect(p.instance.recoverMediaError).not.toHaveBeenCalled();
});
test("ordinary append errors remain visible and use normal recovery", () => {
  const p = player();
  p.trigger(Hls.Events.BUFFER_CODECS, { video: {} });
  p.trigger(Hls.Events.ERROR, {
    ...layoutError,
    error: new Error("bad media"),
  });
  expect(p.error).toHaveBeenCalledWith(p.hlsRef, "bad media");
  vi.runAllTimers();
  expect(p.instance.recoverMediaError).toHaveBeenCalled();
});
test("expected nonfatal seek cancellations do not cover the video with an error", () => {
  const p = player();
  p.trigger(Hls.Events.ERROR, {
    type: Hls.ErrorTypes.NETWORK_ERROR,
    details: Hls.ErrorDetails.INTERNAL_ABORTED,
    fatal: false,
    error: new Error("Aborted"),
  });
  expect(p.error).not.toHaveBeenCalled();
});

test("append-progress failures remain visible during and after a layout transition", () => {
  const p = player();
  p.trigger(Hls.Events.BUFFER_CODECS, { audiovideo: {} });
  p.trigger(Hls.Events.ERROR, layoutError);
  const progressError = {
    ...layoutError,
    fatal: false,
    details: Hls.ErrorDetails.BUFFER_APPEND_NO_PROGRESS,
    error: new Error("append made no progress"),
  };
  p.trigger(Hls.Events.ERROR, progressError);
  expect(p.error).toHaveBeenCalledWith(p.hlsRef, "append made no progress");
  p.media.currentTime = 8;
  p.media.dispatchEvent(new Event("timeupdate"));
  p.error.mockClear();
  p.trigger(Hls.Events.ERROR, progressError);
  expect(p.error).toHaveBeenCalledWith(p.hlsRef, "append made no progress");
});

test("boundary recovery preserves a paused player's state", () => {
  const p = player();
  Object.defineProperty(p.media, "paused", { value: true });
  p.media.currentTime = 8;
  p.trigger(Hls.Events.BUFFER_CODECS, { audiovideo: {} });
  p.trigger(Hls.Events.ERROR, layoutError);
  expect(p.instance.recoverMediaError).toHaveBeenCalled();
  expect(p.media.play).not.toHaveBeenCalled();
});

test("fatal aborted requests remain visible", () => {
  const p = player();
  p.trigger(Hls.Events.ERROR, {
    type: Hls.ErrorTypes.NETWORK_ERROR,
    details: Hls.ErrorDetails.INTERNAL_ABORTED,
    fatal: true,
    error: new Error("fatal request failure"),
  });
  expect(p.error).toHaveBeenCalledWith(p.hlsRef, "fatal request failure");
});
