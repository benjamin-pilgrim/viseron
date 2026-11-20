import { act, render } from "@testing-library/react";
import Hls, { Level } from "hls.js";
import { afterEach, describe, expect, it, vi } from "vitest";

import SyncManager from "components/events/SyncManager";
import { useHlsStore, useReferencePlayerStore } from "components/events/utils";

describe("SyncManager", () => {
  const players: Hls[] = [];

  const createPlayer = () => {
    const hls = new Hls();
    // Stands in for the error recovery that the player components register
    hls.on(Hls.Events.ERROR, vi.fn());
    players.push(hls);
    return { current: hls };
  };

  afterEach(() => {
    useHlsStore.setState({ hlsRefs: [] });
    players.splice(0).forEach((hls) => hls.destroy());
  });

  it("keeps the other error listeners of a player when another player is added", () => {
    const player = createPlayer();
    const otherListeners = player.current.listeners(Hls.Events.ERROR);
    useHlsStore.setState({ hlsRefs: [player] });
    render(<SyncManager>{null}</SyncManager>);

    act(() => {
      useHlsStore.setState({ hlsRefs: [player, createPlayer()] });
    });

    const listeners = player.current.listeners(Hls.Events.ERROR);
    expect(listeners).toEqual(expect.arrayContaining(otherListeners));
    expect(listeners).toHaveLength(otherListeners.length + 1);
  });

  it("resumes a paused single player at the next fragment", () => {
    const player = createPlayer();
    const date = new Date("2026-01-01T12:00:00Z");
    const video = document.createElement("video");
    const play = vi.spyOn(video, "play").mockResolvedValue();
    vi.spyOn(player.current, "media", "get").mockReturnValue(video);
    vi.spyOn(player.current, "playingDate", "get").mockReturnValue(date);
    vi.spyOn(player.current, "currentLevel", "get").mockReturnValue(0);
    vi.spyOn(player.current, "levels", "get").mockReturnValue([
      {
        details: {
          fragments: [{ start: 15, programDateTime: date.getTime() + 5000 }],
        },
      } as Level,
    ]);
    vi.spyOn(player.current, "startLoad").mockImplementation(() => {});
    const originalState = useReferencePlayerStore.getState();
    useReferencePlayerStore.setState({
      isPlaying: true,
      playingDateRef: { current: date.getTime() / 1000 },
    });
    useHlsStore.setState({ hlsRefs: [player] });
    const view = render(<SyncManager>{null}</SyncManager>);
    try {
      expect(video.currentTime).toBe(15);
      expect(play).toHaveBeenCalled();
    } finally {
      view.unmount();
      useReferencePlayerStore.setState(originalState);
      vi.restoreAllMocks();
    }
  });

  it("removes only its own error listeners when unmounted", () => {
    const player = createPlayer();
    const otherListeners = player.current.listeners(Hls.Events.ERROR);
    useHlsStore.setState({ hlsRefs: [player] });
    const { unmount } = render(<SyncManager>{null}</SyncManager>);

    unmount();

    expect(player.current.listeners(Hls.Events.ERROR)).toEqual(otherListeners);
  });
});
