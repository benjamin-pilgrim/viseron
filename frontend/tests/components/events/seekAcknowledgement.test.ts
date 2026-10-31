import Hls from "hls.js";
import { describe, expect, it, vi } from "vitest";

import {
  hasCompletedSeek,
  observeSeek,
} from "components/events/seekAcknowledgement";

describe("seek acknowledgement", () => {
  it("only accepts the latest generation at its target", () => {
    const player = {} as Hls;
    const media = document.createElement("video");
    observeSeek(player, media, 1, 5);
    observeSeek(player, media, 2, 20);
    media.currentTime = 5;
    media.dispatchEvent(new Event("seeked"));
    expect(hasCompletedSeek(player, 1)).toBe(false);
    expect(hasCompletedSeek(player, 2)).toBe(false);
    media.currentTime = 20;
    media.dispatchEvent(new Event("seeked"));
    expect(hasCompletedSeek(player, 2)).toBe(true);
    observeSeek(player, media, 3, 20);
    media.currentTime = 20;
    media.dispatchEvent(new Event("seeked"));
    expect(hasCompletedSeek(player, 3)).toBe(true);
    expect(hasCompletedSeek(player, 2)).toBe(false);
  });
  it("accepts a delayed seeked event after media advances at 10x", () => {
    vi.useFakeTimers();
    try {
      const player = {} as Hls;
      const media = document.createElement("video");
      media.playbackRate = 10;
      observeSeek(player, media, 4, 10);
      vi.advanceTimersByTime(1500);
      media.currentTime = 25;
      media.dispatchEvent(new Event("seeked"));
      expect(hasCompletedSeek(player, 4)).toBe(true);
    } finally {
      vi.useRealTimers();
    }
  });
  it("ignores an old event while the latest seek is still in flight", () => {
    const player = {} as Hls;
    const media = document.createElement("video");
    let seeking = true;
    Object.defineProperty(media, "seeking", { get: () => seeking });
    observeSeek(player, media, 1, 5);
    observeSeek(player, media, 2, 20);
    media.currentTime = 20;
    media.dispatchEvent(new Event("seeked"));
    expect(hasCompletedSeek(player, 2)).toBe(false);
    seeking = false;
    media.dispatchEvent(new Event("seeked"));
    expect(hasCompletedSeek(player, 2)).toBe(true);
  });
});
