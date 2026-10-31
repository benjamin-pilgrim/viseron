import Hls from "hls.js";
import { describe, expect, it, vi } from "vitest";

import {
  confirmAlreadyAlignedSeek,
  hasCompletedSeek,
  noteSeekPlaybackRate,
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
  it("remembers earlier speed changes even when rate events are delayed", () => {
    vi.useFakeTimers();
    try {
      const player = {} as Hls;
      const media = document.createElement("video");
      media.playbackRate = 0.5;
      observeSeek(player, media, 10, 10);
      noteSeekPlaybackRate(player, 10);
      vi.advanceTimersByTime(1000);
      media.currentTime = 18;
      media.dispatchEvent(new Event("seeked"));
      expect(hasCompletedSeek(player, 10)).toBe(true);
      observeSeek(player, media, 11, 10);
      media.currentTime = 30;
      media.dispatchEvent(new Event("seeked"));
      expect(hasCompletedSeek(player, 11)).toBe(false);
    } finally {
      vi.useRealTimers();
    }
  });
  it("remembers the initial 10x speed after slowing before seeked", () => {
    vi.useFakeTimers();
    try {
      const player = {} as Hls;
      const media = document.createElement("video");
      media.playbackRate = 10;
      observeSeek(player, media, 12, 10);
      vi.advanceTimersByTime(1000);
      media.currentTime = 18;
      media.playbackRate = 0.5;
      media.dispatchEvent(new Event("ratechange"));
      media.dispatchEvent(new Event("seeked"));
      expect(hasCompletedSeek(player, 12)).toBe(true);
    } finally {
      vi.useRealTimers();
    }
  });
  it("does not infer a no-op acknowledgement from another or initially unaligned generation", () => {
    vi.useFakeTimers();
    try {
      const player = {} as Hls;
      const media = document.createElement("video");
      Object.defineProperty(media, "readyState", {
        value: HTMLMediaElement.HAVE_FUTURE_DATA,
      });
      media.currentTime = 60;
      observeSeek(player, media, 10, 10);
      media.currentTime = 10;
      vi.advanceTimersByTime(1200);
      expect(confirmAlreadyAlignedSeek(player, media, 10)).toBe(false);
      observeSeek(player, media, 11, 10);
      vi.advanceTimersByTime(1200);
      expect(confirmAlreadyAlignedSeek(player, media, 10)).toBe(false);
      expect(confirmAlreadyAlignedSeek(player, media, 11)).toBe(true);
    } finally {
      vi.useRealTimers();
    }
  });
});
