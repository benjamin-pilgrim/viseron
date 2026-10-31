import Hls from "hls.js";
import React from "react";
import { createRoot } from "react-dom/client";

import SyncManager from "components/events/SyncManager";
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
