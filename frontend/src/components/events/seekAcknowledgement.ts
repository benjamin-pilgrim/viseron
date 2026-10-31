import Hls from "hls.js";

// A seeked event belongs to the latest explicit seek issued for this player.
// Keep this separate from playback position: hls.js can report an old date
// briefly after the media element has moved.
const seeks = new WeakMap<
  Hls,
  { generation: number; complete: boolean; cleanup: () => void }
>();

export function observeSeek(
  player: Hls,
  media: HTMLMediaElement,
  generation: number,
  target: number,
) {
  seeks.get(player)?.cleanup();
  const startedAt = performance.now();
  const seek = {
    generation,
    complete: false,
    cleanup: () => media.removeEventListener("seeked", onSeeked),
  };
  seeks.set(player, seek);
  const onSeeked = () => {
    media.removeEventListener("seeked", onSeeked);
    if (
      seeks.get(player) === seek &&
      Math.abs(media.currentTime - target) < 2
    ) {
      seek.complete = true;
    }
  };
  media.addEventListener("seeked", onSeeked, { once: true });
}

export function hasCompletedSeek(player: Hls, generation: number): boolean {
  const seek = seeks.get(player);
  return seek?.generation === generation && seek.complete;
}
