import { act, render } from "@testing-library/react";
import Hls from "hls.js";
import { afterEach, describe, expect, it, vi } from "vitest";

import SyncManager from "components/events/SyncManager";
import { observeSeek } from "components/events/seekAcknowledgement";
import { useHlsStore, useReferencePlayerStore } from "components/events/utils";

const makePlayer = (date: number) => ({
  current: {
    playingDate: new Date(date),
    latency: 20,
    currentLevel: 0,
    media: {
      paused: false,
      readyState: 4,
      muted: true,
      currentTime: 12,
      playbackRate: 1,
      seekable: { length: 1, start: () => 0, end: () => 100 },
      play: vi.fn().mockResolvedValue(undefined),
      pause: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    },
    levels: [
      {
        details: {
          fragments: [
            { start: 0, duration: 100, programDateTime: 1_700_000_000_000 },
          ],
        },
      },
    ],
    on: vi.fn(),
    off: vi.fn(),
  } as unknown as Hls,
});

afterEach(() => {
  useHlsStore.setState({ hlsRefs: [], hlsRefsError: new Map() });
  vi.useRealTimers();
});

describe("multi-camera playback", () => {
  it("corrects a small camera drift without a visible seek", async () => {
    vi.useFakeTimers();
    const fast = makePlayer(1_700_000_020_000);
    const slow = makePlayer(1_700_000_019_000);
    useHlsStore.setState({ hlsRefs: [fast, slow] });
    useReferencePlayerStore.setState({
      isPlaying: true,
      playbackSpeed: 1,
      requestedTimestamp: 1_700_000_000,
      playingDateRef: { current: 1_700_000_010 },
    });
    render(
      <SyncManager>
        <div>players</div>
      </SyncManager>,
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(300);
    });
    expect(slow.current.media!.currentTime).toBe(12);
    expect(slow.current.media!.playbackRate).toBeGreaterThan(1);
  });
  it("seeks a severely delayed camera to the reference, not the live edge", async () => {
    vi.useFakeTimers();
    const fast = makePlayer(1_700_000_020_000);
    const slow = makePlayer(1_700_000_010_000);
    useHlsStore.setState({ hlsRefs: [fast, slow] });
    useReferencePlayerStore.setState({
      isPlaying: true,
      playbackSpeed: 1,
      requestedTimestamp: 1_700_000_000,
      playingDateRef: { current: 1_700_000_010 },
    });
    render(
      <SyncManager>
        <div>players</div>
      </SyncManager>,
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(300);
    });
    expect(slow.current.media!.currentTime).toBe(20);
  });

  it("uses the selected speed as the baseline for drift correction", async () => {
    vi.useFakeTimers();
    const fast = makePlayer(1_700_000_020_000);
    const slow = makePlayer(1_700_000_019_000);
    useHlsStore.setState({ hlsRefs: [fast, slow] });
    useReferencePlayerStore.setState({
      isPlaying: true,
      playbackSpeed: 2,
      requestedTimestamp: 1_700_000_000,
      playingDateRef: { current: 1_700_000_010 },
    });
    render(
      <SyncManager>
        <div>players</div>
      </SyncManager>,
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(300);
    });
    expect(fast.current.media!.playbackRate).toBe(2);
    expect(slow.current.media!.playbackRate).toBeGreaterThan(2);
  });
  it("resumes a stalled camera with modest drift", async () => {
    vi.useFakeTimers();
    const fast = makePlayer(1_700_000_020_000);
    const slow = makePlayer(1_700_000_019_000);
    Object.defineProperty(slow.current.media!, "paused", { value: true });
    useHlsStore.setState({ hlsRefs: [fast, slow] });
    useReferencePlayerStore.setState({
      isPlaying: true,
      playbackSpeed: 1,
      requestedTimestamp: 1_700_000_000,
      playingDateRef: { current: 1_700_000_010 },
    });
    render(
      <SyncManager>
        <div>players</div>
      </SyncManager>,
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(300);
    });
    expect(slow.current.media!.play).toHaveBeenCalled();
    expect(slow.current.media!.currentTime).toBe(20);
  });
  it("does not seek after playback is paused while synchronization waits", async () => {
    vi.useFakeTimers();
    const fast = makePlayer(1_700_000_020_000);
    const slow = makePlayer(1_700_000_010_000);
    useHlsStore.setState({ hlsRefs: [fast, slow] });
    useReferencePlayerStore.setState({
      isPlaying: true,
      playbackSpeed: 1,
      requestedTimestamp: 1_700_000_010,
      playingDateRef: { current: 1_700_000_010 },
    });
    render(
      <SyncManager>
        <div>players</div>
      </SyncManager>,
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(100);
      useReferencePlayerStore.setState({ isPlaying: false });
      await vi.advanceTimersByTimeAsync(1200);
    });
    expect(slow.current.media!.currentTime).toBe(12);
  });

  it("resumes a paused camera already aligned to the reference", async () => {
    vi.useFakeTimers();
    const fast = makePlayer(1_700_000_020_000);
    const slow = makePlayer(1_700_000_019_800);
    Object.defineProperty(slow.current.media!, "paused", { value: true });
    useHlsStore.setState({ hlsRefs: [fast, slow] });
    useReferencePlayerStore.setState({
      isPlaying: true,
      playbackSpeed: 1,
      requestedTimestamp: 1_700_000_000,
      playingDateRef: { current: 1_700_000_010 },
    });
    render(
      <SyncManager>
        <div>players</div>
      </SyncManager>,
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(300);
    });
    expect(slow.current.media!.play).toHaveBeenCalled();
  });

  it("ignores a buffering camera when choosing the reference", async () => {
    vi.useFakeTimers();
    const ready = makePlayer(1_700_000_020_000);
    const buffering = makePlayer(1_700_000_030_000);
    Object.defineProperty(buffering.current.media!, "readyState", { value: 2 });
    useHlsStore.setState({ hlsRefs: [buffering, ready] });
    useReferencePlayerStore.setState({
      isPlaying: true,
      playbackSpeed: 1,
      requestedTimestamp: 1_700_000_000,
      playingDateRef: { current: 1_700_000_010 },
    });
    render(
      <SyncManager>
        <div>players</div>
      </SyncManager>,
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(300);
    });
    expect(useReferencePlayerStore.getState().referencePlayer).toBe(
      ready.current,
    );
  });

  it("recovers a seekable player that has no current playback date", async () => {
    vi.useFakeTimers();
    const ready = makePlayer(1_700_000_020_000);
    const missing = makePlayer(1_700_000_010_000);
    Object.defineProperty(missing.current, "playingDate", { value: null });
    useHlsStore.setState({ hlsRefs: [ready, missing] });
    useReferencePlayerStore.setState({
      isPlaying: true,
      playbackSpeed: 1,
      requestedTimestamp: 1_700_000_000,
      playingDateRef: { current: 1_700_000_010 },
    });
    render(
      <SyncManager>
        <div>players</div>
      </SyncManager>,
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(300);
    });
    expect(missing.current.media!.currentTime).toBe(20);
  });

  it("hard seeks drift too large to correct promptly", async () => {
    vi.useFakeTimers();
    const fast = makePlayer(1_700_000_020_000);
    const slow = makePlayer(1_700_000_017_000);
    useHlsStore.setState({ hlsRefs: [fast, slow] });
    useReferencePlayerStore.setState({
      isPlaying: true,
      playbackSpeed: 10,
      requestedTimestamp: 1_700_000_000,
      playingDateRef: { current: 1_700_000_010 },
    });
    render(
      <SyncManager>
        <div>players</div>
      </SyncManager>,
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(300);
    });
    expect(slow.current.media!.currentTime).toBe(20);
  });
  it("attempts recovery after a camera remains buffering", async () => {
    vi.useFakeTimers();
    const ready = makePlayer(1_700_000_020_000);
    const buffering = makePlayer(1_700_000_010_000);
    Object.defineProperty(buffering.current.media!, "readyState", { value: 2 });
    useHlsStore.setState({ hlsRefs: [ready, buffering] });
    useReferencePlayerStore.setState({
      isPlaying: true,
      playbackSpeed: 1,
      requestedTimestamp: 1_700_000_000,
      playingDateRef: { current: 1_700_000_010 },
    });
    render(
      <SyncManager>
        <div>players</div>
      </SyncManager>,
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2500);
    });
    expect(buffering.current.media!.currentTime).toBe(20);
  });
  it("retries a shared buffering outage with bounded attempts and honors pause", async () => {
    vi.useFakeTimers();
    const first = makePlayer(1_700_000_020_000);
    const second = makePlayer(1_700_000_019_000);
    Object.defineProperty(first.current.media!, "readyState", { value: 2 });
    Object.defineProperty(second.current.media!, "readyState", { value: 2 });
    useHlsStore.setState({ hlsRefs: [first, second] });
    useReferencePlayerStore.setState({
      isPlaying: true,
      requestedTimestamp: 1_700_000_000,
      playingDateRef: { current: 1_700_000_010 },
    });
    render(
      <SyncManager>
        <div>players</div>
      </SyncManager>,
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(1900);
    });
    expect(first.current.media!.play).not.toHaveBeenCalled();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(500);
    });
    expect(first.current.media!.play).toHaveBeenCalledTimes(1);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(4000);
    });
    expect(first.current.media!.play).toHaveBeenCalledTimes(1);
    await act(async () => {
      useReferencePlayerStore.setState({ isPlaying: false });
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(6000);
    });
    expect(first.current.media!.play).toHaveBeenCalledTimes(1);
  });
  it("does not recover an all-buffering outage over a fresh seek", async () => {
    vi.useFakeTimers();
    const first = makePlayer(1_700_000_020_000);
    Object.defineProperty(first.current.media!, "readyState", { value: 2 });
    useHlsStore.setState({ hlsRefs: [first] });
    useReferencePlayerStore.setState({
      isPlaying: true,
      requestedTimestamp: 1_700_000_000,
      playingDateRef: { current: 1_700_000_010 },
    });
    render(
      <SyncManager>
        <div>players</div>
      </SyncManager>,
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(1900);
    });
    await act(async () => {
      useReferencePlayerStore.setState({
        requestedTimestamp: 1_700_000_030,
        playingDateRef: { current: 1_700_000_030 },
      });
      await vi.advanceTimersByTimeAsync(800);
    });
    expect(first.current.media!.play).not.toHaveBeenCalled();
    expect(first.current.media!.currentTime).toBe(12);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(500);
    });
    expect(first.current.media!.currentTime).toBe(30);
  });
  it("tries another camera when the first remains buffering", async () => {
    vi.useFakeTimers();
    const first = makePlayer(1_700_000_020_000);
    const second = makePlayer(1_700_000_019_000);
    Object.defineProperty(first.current.media!, "readyState", { value: 2 });
    Object.defineProperty(second.current.media!, "readyState", { value: 2 });
    useHlsStore.setState({ hlsRefs: [first, second] });
    useReferencePlayerStore.setState({
      isPlaying: true,
      requestedTimestamp: 1_700_000_000,
      playingDateRef: { current: 1_700_000_010 },
    });
    render(
      <SyncManager>
        <div>players</div>
      </SyncManager>,
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(3500);
    });
    expect(first.current.media!.play).toHaveBeenCalled();
    expect(second.current.media!.play).toHaveBeenCalled();
  });
  it("does not let an old player overwrite a slow backward seek", async () => {
    vi.useFakeTimers();
    const first = makePlayer(1_700_000_060_000);
    useHlsStore.setState({ hlsRefs: [first] });
    useReferencePlayerStore.setState({
      isPlaying: true,
      requestedTimestamp: 1_700_000_050,
      playingDateRef: { current: 1_700_000_060 },
    });
    render(
      <SyncManager>
        <div>players</div>
      </SyncManager>,
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(200);
    });
    await act(async () => {
      useReferencePlayerStore.getState().setRequestedTimestamp(1_700_000_010);
      await vi.advanceTimersByTimeAsync(3000);
    });
    expect(useReferencePlayerStore.getState().playingDateRef.current).toBe(
      1_700_000_010,
    );
  });
  it("accepts a freshly sought player advancing at 10x without rewinding", async () => {
    vi.useFakeTimers();
    const fresh = makePlayer(1_700_000_060_000);
    const stale = makePlayer(1_700_000_060_000);
    useHlsStore.setState({ hlsRefs: [fresh, stale] });
    useReferencePlayerStore.setState({
      isPlaying: true,
      playbackSpeed: 10,
      requestedTimestamp: 1_700_000_050,
      playingDateRef: { current: 1_700_000_060 },
    });
    render(
      <SyncManager>
        <div>players</div>
      </SyncManager>,
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(200);
    });
    const seekAt = performance.now();
    let elapsedWithoutTicks = 0;
    const media = fresh.current.media!;
    const events = new EventTarget();
    media.addEventListener = events.addEventListener.bind(events);
    media.removeEventListener = events.removeEventListener.bind(events);
    Object.defineProperty(media, "currentTime", {
      configurable: true,
      get: () => 10 + (performance.now() - seekAt + elapsedWithoutTicks) / 100,
    });
    Object.defineProperty(fresh.current, "playingDate", {
      configurable: true,
      get: () =>
        new Date(
          1_700_000_010_000 +
            (performance.now() - seekAt + elapsedWithoutTicks) * 10,
        ),
    });
    await act(async () => {
      useReferencePlayerStore.getState().setRequestedTimestamp(1_700_000_010);
      observeSeek(
        fresh.current,
        media,
        useReferencePlayerStore.getState().seekGeneration,
        10,
      );
      events.dispatchEvent(new Event("seeked"));
      // Media advances while the main thread misses the early sync ticks.
      elapsedWithoutTicks = 2000;
      await vi.advanceTimersByTimeAsync(1500);
    });
    expect(useReferencePlayerStore.getState().referencePlayer).toBe(
      fresh.current,
    );
    expect(
      useReferencePlayerStore.getState().playingDateRef.current,
    ).toBeGreaterThan(1_700_000_020);
    expect(fresh.current.media!.currentTime).toBeGreaterThan(20);
  });
  it("does not jump over a distant unavailable gap", async () => {
    vi.useFakeTimers();
    const first = makePlayer(1_700_000_020_000);
    Object.defineProperty(first.current.media!, "paused", { value: true });
    first.current.levels[0].details!.fragments[0].programDateTime = 1_700_000_100_000;
    useHlsStore.setState({ hlsRefs: [first] });
    useReferencePlayerStore.setState({
      isPlaying: true,
      requestedTimestamp: 1_700_000_000,
      playingDateRef: { current: 1_700_000_010 },
    });
    render(
      <SyncManager>
        <div>players</div>
      </SyncManager>,
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(3000);
    });
    expect(first.current.media!.currentTime).toBe(12);
    expect(first.current.media!.play).not.toHaveBeenCalled();
  });
  it("does not recover to a fragment outside the seekable range", async () => {
    vi.useFakeTimers();
    const first = makePlayer(1_700_000_020_000);
    Object.defineProperty(first.current.media!, "paused", { value: true });
    Object.defineProperty(first.current.media!, "seekable", {
      value: { length: 1, start: () => 0, end: () => 5 },
    });
    useHlsStore.setState({ hlsRefs: [first] });
    useReferencePlayerStore.setState({
      isPlaying: true,
      requestedTimestamp: 1_700_000_000,
      playingDateRef: { current: 1_700_000_010 },
    });
    render(
      <SyncManager>
        <div>players</div>
      </SyncManager>,
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(3000);
    });
    expect(first.current.media!.currentTime).toBe(12);
    expect(first.current.media!.play).not.toHaveBeenCalled();
  });
  it("does not resume a user-paused player on HLS errors", async () => {
    vi.useFakeTimers();
    const player = makePlayer(1_700_000_020_000);
    useHlsStore.setState({ hlsRefs: [player] });
    useReferencePlayerStore.setState({
      isPlaying: false,
      playbackSpeed: 1,
      requestedTimestamp: 1_700_000_000,
      playingDateRef: { current: 1_700_000_010 },
    });
    render(
      <SyncManager>
        <div>players</div>
      </SyncManager>,
    );
    const callback = vi
      .mocked(player.current.on)
      .mock.calls.find(
        ([event]) => event === Hls.Events.ERROR,
      )![1] as unknown as (event: string, data: { details: string }) => void;
    act(() =>
      callback(Hls.Events.ERROR, {
        details: Hls.ErrorDetails.BUFFER_STALLED_ERROR,
      }),
    );
    expect(player.current.media!.play).not.toHaveBeenCalled();
  });

  it("resumes an aligned player without rewinding to the fragment start", async () => {
    vi.useFakeTimers();
    const player = makePlayer(1_700_000_020_000);
    Object.defineProperty(player.current.media!, "paused", { value: true });
    player.current.media!.currentTime = 20;
    useHlsStore.setState({ hlsRefs: [player] });
    useReferencePlayerStore.setState({
      isPlaying: true,
      playbackSpeed: 1,
      requestedTimestamp: 1_700_000_000,
      playingDateRef: { current: 1_700_000_020 },
    });
    render(
      <SyncManager>
        <div>players</div>
      </SyncManager>,
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(300);
    });
    expect(player.current.media!.play).toHaveBeenCalled();
    expect(player.current.media!.currentTime).toBe(20);
  });

  it("does not clear a new playback error when a previous play completes after pause", async () => {
    vi.useFakeTimers();
    const player = makePlayer(1_700_000_020_000);
    Object.defineProperty(player.current, "playingDate", { value: null });
    let resolvePlay!: () => void;
    vi.mocked(player.current.media!.play).mockReturnValue(
      new Promise<void>((resolve) => {
        resolvePlay = resolve;
      }),
    );
    useHlsStore.setState({
      hlsRefs: [player],
      hlsRefsError: new Map([[player, "unavailable"]]),
    });
    useReferencePlayerStore.setState({
      isPlaying: true,
      playbackSpeed: 1,
      requestedTimestamp: 1_700_000_000,
      playingDateRef: { current: 1_700_000_010 },
    });
    render(
      <SyncManager>
        <div>players</div>
      </SyncManager>,
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(150);
    });
    await act(async () => {
      useReferencePlayerStore.setState({ isPlaying: false });
      resolvePlay();
      await Promise.resolve();
    });
    expect(useHlsStore.getState().hlsRefsError.get(player)).toBe("unavailable");
    expect(player.current.media!.pause).toHaveBeenCalled();
  });
  it("removes only its own error listener when a player is removed", () => {
    const player = makePlayer(1_700_000_020_000);
    useHlsStore.setState({ hlsRefs: [player] });
    const view = render(
      <SyncManager>
        <div>players</div>
      </SyncManager>,
    );
    const handler = vi
      .mocked(player.current.on)
      .mock.calls.find(([event]) => event === Hls.Events.ERROR)![1];
    view.unmount();
    expect(player.current.off).toHaveBeenCalledWith(Hls.Events.ERROR, handler);
  });
  it("honors a pause made while an error recovery play is pending", async () => {
    vi.useFakeTimers();
    const player = makePlayer(1_700_000_020_000);
    let resolvePlay!: () => void;
    vi.mocked(player.current.media!.play).mockReturnValue(
      new Promise<void>((resolve) => {
        resolvePlay = resolve;
      }),
    );
    useHlsStore.setState({ hlsRefs: [player] });
    useReferencePlayerStore.setState({
      isPlaying: true,
      playbackSpeed: 1,
      requestedTimestamp: 1_700_000_000,
      playingDateRef: { current: 1_700_000_010 },
    });
    render(
      <SyncManager>
        <div>players</div>
      </SyncManager>,
    );
    const handler = vi
      .mocked(player.current.on)
      .mock.calls.find(
        ([event]) => event === Hls.Events.ERROR,
      )![1] as unknown as (event: string, data: { details: string }) => void;
    await act(async () => {
      handler(Hls.Events.ERROR, {
        details: Hls.ErrorDetails.BUFFER_STALLED_ERROR,
      });
      useReferencePlayerStore.setState({ isPlaying: false });
      resolvePlay();
      await Promise.resolve();
    });
    expect(player.current.media!.pause).toHaveBeenCalled();
  });
});
