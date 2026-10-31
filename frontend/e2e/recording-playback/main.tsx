import Hls from "hls.js";
import React from "react";
import { createRoot } from "react-dom/client";

import SyncManager from "components/events/SyncManager";
import { observeSeek } from "components/events/seekAcknowledgement";
import { useHlsStore, useReferencePlayerStore } from "components/events/utils";

const videos = [0, 1].map((index) => {
  const video = document.createElement("video");
  video.id = `camera-${index}`;
  video.muted = true;
  document.body.append(video);
  return video;
});
const players = videos.map((video) => {
  const player = new Hls();
  player.attachMedia(video);
  player.loadSource("/playback-fixture/index.m3u8");
  return player;
});
const debugTrace: unknown[] = [];
const recordState = (event: string) => {
  const state = useReferencePlayerStore.getState();
  debugTrace.push({
    event,
    at: performance.now(),
    requested: state.requestedTimestamp,
    generation: state.seekGeneration,
    playing: state.isPlaying,
    reference: players.indexOf(state.referencePlayer!),
    players: players.map((player, i) => ({
      time: videos[i].currentTime,
      paused: videos[i].paused,
      seeking: videos[i].seeking,
      rate: videos[i].playbackRate,
      ready: videos[i].readyState,
      ended: videos[i].ended,
      date: player.playingDate?.getTime(),
      buffered: Array.from({ length: videos[i].buffered.length }, (_, j) => [
        videos[i].buffered.start(j),
        videos[i].buffered.end(j),
      ]),
    })),
  });
};
videos.forEach((video, i) =>
  [
    "seeking",
    "seeked",
    "ratechange",
    "waiting",
    "playing",
    "pause",
    "ended",
  ].forEach((event) =>
    video.addEventListener(event, () => recordState(`${i}:${event}`)),
  ),
);
players.forEach((player, i) =>
  player.on(Hls.Events.ERROR, (_, data) =>
    recordState(`${i}:hls:${data.details}`),
  ),
);
setInterval(() => recordState("tick"), 250);
Object.assign(window, {
  debugTrace,
  playbackVideos: videos,
  playbackPlayers: players,
  seekForTest: (timestamp: number, target: number) => {
    useReferencePlayerStore.getState().setRequestedTimestamp(timestamp);
    const generation = useReferencePlayerStore.getState().seekGeneration;
    const video = videos[0];
    const add = video.addEventListener.bind(video);
    const remove = video.removeEventListener.bind(video);
    let wrapped: EventListener | undefined;
    video.addEventListener = ((
      type: string,
      listener: EventListener,
      options?: AddEventListenerOptions,
    ) => {
      if (type !== "seeked") return add(type, listener, options);
      wrapped = (event) => {
        recordState("wrapped seeked");
        const before = video.currentTime;
        const poll = setInterval(() => {
          if (video.currentTime - before <= 3) return;
          clearInterval(poll);
          recordState("advanced 3");
          useReferencePlayerStore.getState().setPlaybackSpeed(0.5);
          video.playbackRate = 0.5;
          setTimeout(() => {
            Object.assign(window, {
              delayedSeekAdvance: video.currentTime - before,
            });
            recordState("deliver seeked");
            listener.call(video, event);
          }, 1300);
        }, 50);
      };
      add(type, wrapped, options);
    }) as typeof video.addEventListener;
    video.removeEventListener = ((
      type: string,
      listener: EventListener,
      options?: EventListenerOptions,
    ) => {
      remove(type, type === "seeked" && wrapped ? wrapped : listener, options);
    }) as typeof video.removeEventListener;
    observeSeek(players[0], video, generation, target);
    videos[0].currentTime = target;
  },
  alignedSeekWithoutEvent: () => {
    const video = videos[0];
    const player = players[0];
    const current = player.playingDate!;
    useReferencePlayerStore
      .getState()
      .setRequestedTimestamp(current.getTime() / 1000);
    const generation = useReferencePlayerStore.getState().seekGeneration;
    const target = video.currentTime;
    const add = video.addEventListener.bind(video);
    video.addEventListener = ((
      type: string,
      listener: EventListener,
      options?: AddEventListenerOptions,
    ) => {
      if (type !== "seeked") add(type, listener, options);
    }) as typeof video.addEventListener;
    observeSeek(player, video, generation, target);
    video.addEventListener = add as typeof video.addEventListener;
    video.currentTime = target;
    return target;
  },
  referenceForTest: () => useReferencePlayerStore.getState().referencePlayer,
  setPlaybackSpeed: (speed: number) =>
    useReferencePlayerStore.getState().setPlaybackSpeed(speed),
});
createRoot(document.getElementById("root")!).render(
  <SyncManager>
    <div>Camera playback</div>
  </SyncManager>,
);
Promise.all(
  players.map(
    (player) =>
      new Promise<void>((resolve) => {
        player.on(Hls.Events.MANIFEST_PARSED, () => resolve());
      }),
  ),
).then(async () => {
  await Promise.all(videos.map((video) => video.play()));
  useReferencePlayerStore.setState({
    isPlaying: true,
    playbackSpeed: 1,
    requestedTimestamp: 0,
    playingDateRef: { current: 1 },
  });
  useHlsStore.setState({
    hlsRefs: players.map((player) => ({ current: player })),
  });
  Object.assign(window, { playbackReady: true });
});
