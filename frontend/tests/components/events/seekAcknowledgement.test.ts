import Hls from "hls.js";
import { describe, expect, it } from "vitest";

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
    observeSeek(player, media, 3, 20);
    media.currentTime = 20;
    media.dispatchEvent(new Event("seeked"));
    expect(hasCompletedSeek(player, 3)).toBe(true);
    expect(hasCompletedSeek(player, 2)).toBe(false);
  });
});
