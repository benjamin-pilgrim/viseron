import Hls from "hls.js";

// A seeked event belongs to the latest explicit seek issued for this player.
// Keep this separate from playback position: hls.js can report an old date
// briefly after the media element has moved.
const seeks = new WeakMap<
  Hls,
  {
    generation: number;
    complete: boolean;
    maxRate: number;
    startedAt: number;
    blockRecovery: boolean;
    cleanup: () => void;
  }
>();

export function observeSeek(
  player: Hls,
  media: HTMLMediaElement,
  generation: number,
  target: number,
  blockRecovery = true,
) {
  seeks.get(player)?.cleanup();
  const startedAt = performance.now();
  const seek = {
    generation,
    complete: false,
    startedAt,
    blockRecovery,
    maxRate: Math.max(1, media.playbackRate),
    cleanup: () => {
      media.removeEventListener("seeked", onSeeked);
      media.removeEventListener("ratechange", onRateChange);
    },
  };
  seeks.set(player, seek);
  const onRateChange = () => {
    seek.maxRate = Math.max(seek.maxRate, media.playbackRate);
  };
  const onSeeked = () => {
    // A superseded seek can still have a queued event. Keep listening until
    // the current seek has actually settled at a plausible media position.
    const elapsed = Math.max(0, (performance.now() - startedAt) / 1000);
    const furthest =
      target + elapsed * Math.max(seek.maxRate, media.playbackRate) + 2;
    if (
      seeks.get(player) === seek &&
      !media.seeking &&
      media.currentTime >= target - 2 &&
      media.currentTime <= furthest
    ) {
      seek.complete = true;
      seek.cleanup();
    }
  };
  media.addEventListener("seeked", onSeeked);
  media.addEventListener("ratechange", onRateChange);
}

// Record application-controlled changes synchronously: ratechange events can
// be queued until after a subsequent rate change has already occurred.
export function noteSeekPlaybackRate(player: Hls, rate: number): void {
  const seek = seeks.get(player);
  if (seek && !seek.complete) seek.maxRate = Math.max(seek.maxRate, rate);
}

export function hasCompletedSeek(player: Hls, generation: number): boolean {
  const seek = seeks.get(player);
  return seek?.generation === generation && seek.complete;
}

// Give an in-flight seek time to finish before recovery issues another one.
// An upper limit preserves recovery when the browser never emits seeked.
export function hasPendingSeek(player: Hls, generation: number): boolean {
  const seek = seeks.get(player);
  return (
    !!seek &&
    seek.generation === generation &&
    seek.blockRecovery &&
    !seek.complete &&
    performance.now() - seek.startedAt < 5000
  );
}
