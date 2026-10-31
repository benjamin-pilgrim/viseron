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
Object.assign(window, {
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
        const before = video.currentTime;
        setTimeout(() => {
          Object.assign(window, {
            delayedSeekAdvance: video.currentTime - before,
          });
          listener.call(video, event);
        }, 1300);
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
