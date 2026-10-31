import { act, render } from "@testing-library/react";
import Hls from "hls.js";
import { afterEach, describe, expect, it, vi } from "vitest";

import SyncManager from "components/events/SyncManager";
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
});
