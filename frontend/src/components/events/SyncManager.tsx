import { Dayjs } from "dayjs";
import Hls, { ErrorData, Events } from "hls.js";
import { useCallback, useEffect, useRef } from "react";
import { useShallow } from "zustand/react/shallow";

import {
  HlsErrorCodes,
  LIVE_EDGE_DELAY,
  findClosestFragment,
  findFragmentByTimestamp,
  getSeekTarget,
  translateErrorCode,
  useHlsStore,
  useReferencePlayerStore,
} from "components/events/utils";
import useControlledInterval from "hooks/UseControlledInterval";
import { getDayjsFromDate } from "lib/helpers/dates";

const SYNC_INTERVAL = 100; // Sync interval in milliseconds
const MAX_DRIFT = 0.5; // Maximum drift before correcting playback
const HARD_SEEK_DRIFT = 2; // Seek only when rate correction would take too long
const RATE_NUDGE_FRACTION = 0.1;
const SEEK_SETTLE_MS = 1000;
const BUFFER_RECOVERY_MS = 2000;
const BUFFER_RETRY_MS = 5000;
const MAX_RECOVERY_GAP_MS = 2000;

interface SyncManagerProps {
  children: React.ReactNode;
}

function SyncManager({ children }: SyncManagerProps) {
  const { hlsRefs, setHlsRefsError } = useHlsStore(
    useShallow((state) => ({
      hlsRefs: state.hlsRefs,
      setHlsRefsError: state.setHlsRefsError,
    })),
  );

  const {
    setReferencePlayer,
    setIsPlaying,
    setIsLive,
    requestedTimestamp,
    playingDateRef,
  } = useReferencePlayerStore(
    useShallow((state) => ({
      setReferencePlayer: state.setReferencePlayer,
      setIsPlaying: state.setIsPlaying,
      setIsLive: state.setIsLive,
      requestedTimestamp: state.requestedTimestamp,
      playingDateRef: state.playingDateRef,
    })),
  );
  const lastSeek = useRef<{ timestamp: number; at: number | null }>({
    timestamp: requestedTimestamp,
    at: null,
  });
  const lastCorrection = useRef(new WeakMap<Hls, number>());
  const bufferingSince = useRef(new WeakMap<Hls, number>());
  const lastBufferCorrection = useRef(new WeakMap<Hls, number>());
  const seekFence = useRef<{
    generation: number;
    timestamp: number;
    eligible: WeakSet<Hls>;
  } | null>(null);
  const seekGeneration = useRef(
    useReferencePlayerStore.getState().seekGeneration,
  );

  const seekSafely = useCallback((player: Hls, referenceDate: Dayjs) => {
    if (!player.levels || player.levels.length === 0 || !player.media) {
      return false;
    }

    const currentLevel = player.levels[player.currentLevel];
    if (!currentLevel || !currentLevel.details) {
      return false;
    }

    const fragments = currentLevel.details.fragments;
    if (!fragments || fragments.length === 0) {
      return false;
    }

    const referenceTimestampMillis = referenceDate.valueOf();
    const targetFragment = findFragmentByTimestamp(
      fragments,
      referenceTimestampMillis,
    );
    if (!targetFragment) {
      return false;
    }
    const seekTarget = getSeekTarget(targetFragment, referenceTimestampMillis);

    const seekable = player.media.seekable;
    if (seekable.length === 0) {
      return false;
    }

    for (let i = 0; i < seekable.length; i++) {
      if (seekTarget >= seekable.start(i) && seekTarget <= seekable.end(i)) {
        player.media.currentTime = seekTarget;
        return true;
      }
    }

    return false;
  }, []);

  const syncPlayers = useCallback(() => {
    const now = performance.now();
    const state = useReferencePlayerStore.getState();
    if (lastSeek.current.at === null) {
      lastSeek.current.at =
        state.requestedTimestamp === state.playingDateRef.current
          ? now
          : -Infinity;
    }
    if (
      lastSeek.current.timestamp !== state.requestedTimestamp ||
      seekGeneration.current !== state.seekGeneration
    ) {
      lastSeek.current = { timestamp: state.requestedTimestamp, at: now };
      seekGeneration.current = state.seekGeneration;
      seekFence.current =
        state.requestedTimestamp > 0
          ? {
              generation: state.seekGeneration,
              timestamp: state.requestedTimestamp,
              eligible: new WeakSet(),
            }
          : null;
    }
    // Do not let an older player position override a fresh timeline seek.
    // Unlike an awaited delay, each tick reads the latest play and seek state.
    if (now - (lastSeek.current.at ?? -Infinity) < SEEK_SETTLE_MS) return;

    const players = hlsRefs.filter(
      (player): player is React.MutableRefObject<Hls> =>
        player.current !== null && player.current.media !== null,
    );

    const playersWithTime = players.filter(
      (player) => player.current.playingDate !== null,
    );
    if (seekFence.current) {
      playersWithTime.forEach(({ current }) => {
        if (
          current.playingDate &&
          Math.abs(
            current.playingDate.getTime() / 1000 - state.playingDateRef.current,
          ) <= HARD_SEEK_DRIFT
        ) {
          seekFence.current?.eligible.add(current);
        }
      });
    }

    // Sync mute state
    players.forEach((player) => {
      if (player.current.media) {
        player.current.media.muted = state.isMuted;
      }
    });

    if (!state.isPlaying) return;

    // Find the player with the latest playing date, ignoring paused players
    const referencePlayer =
      playersWithTime.reduce<React.MutableRefObject<Hls> | null>(
        (prev, current) => {
          if (
            seekFence.current &&
            !seekFence.current.eligible.has(current.current)
          )
            return prev;
          if (prev === null) {
            return !current.current.media?.paused &&
              (current.current.media?.readyState ?? 0) >=
                HTMLMediaElement.HAVE_FUTURE_DATA
              ? current
              : null;
          }
          return !current.current.media?.paused &&
            (current.current.media?.readyState ?? 0) >=
              HTMLMediaElement.HAVE_FUTURE_DATA &&
            current.current.playingDate! > prev.current.playingDate!
            ? current
            : prev;
        },
        null,
      );

    // Sync all players to the reference player
    if (referencePlayer) {
      setReferencePlayer(referencePlayer.current);
      setIsLive(referencePlayer.current.latency < LIVE_EDGE_DELAY * 1.5);
      setIsPlaying(true);
      playingDateRef.current = referencePlayer.current.playingDate
        ? getDayjsFromDate(referencePlayer.current.playingDate).unix()
        : requestedTimestamp;
      players.forEach((player) => {
        const media = player.current.media;
        if (!media) return;
        media.playbackRate = state.playbackSpeed;
        if (player === referencePlayer) {
          bufferingSince.current.delete(player.current);
          return;
        }
        // Give transient buffering time to resolve before seeking, and avoid
        // repeatedly disrupting a camera while its network is unavailable.
        const buffering = media.readyState < HTMLMediaElement.HAVE_FUTURE_DATA;
        if (buffering) {
          const since = bufferingSince.current.get(player.current) ?? now;
          bufferingSince.current.set(player.current, since);
          if (now - since < BUFFER_RECOVERY_MS) return;
        } else {
          bufferingSince.current.delete(player.current);
        }
        const referenceDate = getDayjsFromDate(
          referencePlayer.current.playingDate!,
        );
        const playerDate = player.current.playingDate;
        const drift = playerDate
          ? (referenceDate.valueOf() - getDayjsFromDate(playerDate).valueOf()) /
            1000
          : Infinity;
        if (
          drift > MAX_DRIFT &&
          drift < HARD_SEEK_DRIFT &&
          !media.paused &&
          !buffering
        ) {
          media.playbackRate = state.playbackSpeed * (1 + RATE_NUDGE_FRACTION);
          return;
        }
        if (drift <= MAX_DRIFT && !media.paused && !buffering) return;
        const correctionTimes = buffering
          ? lastBufferCorrection
          : lastCorrection;
        const last = correctionTimes.current.get(player.current) ?? -Infinity;
        if (now - last < (buffering ? BUFFER_RETRY_MS : SEEK_SETTLE_MS)) return;
        correctionTimes.current.set(player.current, now);
        // A paused but aligned player only needs play, not a discontinuous seek.
        const aligned =
          playerDate && Math.abs(drift) <= MAX_DRIFT && !buffering;
        if (aligned || seekSafely(player.current, referenceDate)) {
          media
            .play()
            .then(() => {
              const latest = useReferencePlayerStore.getState();
              if (!latest.isPlaying) media.pause();
              else if (
                latest.requestedTimestamp === state.requestedTimestamp &&
                latest.seekGeneration === state.seekGeneration
              ) {
                setHlsRefsError(player, null);
              }
            })
            .catch(() => {});
        } else if (!buffering && playerDate) {
          setHlsRefsError(
            player,
            translateErrorCode(HlsErrorCodes.TIMESPAN_MISSING),
          );
        }
      });
    } else {
      setReferencePlayer(null);
    }

    // If there are no players with time, start one without flooding play promises.
    if (playersWithTime.length === 0 && players.length > 0) {
      const first = players[0];
      const media = first.current.media;
      const last = lastCorrection.current.get(first.current) ?? -Infinity;
      if (media && now - last >= SEEK_SETTLE_MS) {
        lastCorrection.current.set(first.current, now);
        media
          .play()
          .then(() => {
            const latest = useReferencePlayerStore.getState();
            if (!latest.isPlaying) media.pause();
            else if (
              latest.requestedTimestamp === state.requestedTimestamp &&
              latest.seekGeneration === state.seekGeneration
            ) {
              setHlsRefsError(first, null);
            }
          })
          .catch(() => {});
      }
    }

    // Recover one camera if none can provide a running, ready reference.
    if (!referencePlayer && playersWithTime.length > 0) {
      const playingDateMillis = playingDateRef.current * 1000;

      let playerToPlayIndex = -1;
      let smallestDifference = Infinity;
      let oldestAttempt = Infinity;

      playersWithTime.forEach((player, index) => {
        const fragments =
          player.current.levels[player.current.currentLevel]?.details
            ?.fragments;
        if (!fragments || fragments.length === 0) {
          return;
        }

        const closestFragment =
          findFragmentByTimestamp(fragments, playingDateMillis) ??
          findClosestFragment(fragments, playingDateMillis);
        if (!closestFragment || !closestFragment.programDateTime) {
          return;
        }

        const difference = Math.max(
          0,
          closestFragment.programDateTime - playingDateMillis,
        );
        if (difference > MAX_RECOVERY_GAP_MS) return;
        const media = player.current.media;
        if (!media) return;
        const target = getSeekTarget(closestFragment, playingDateMillis);
        const seekable = media.seekable;
        if (
          !Array.from({ length: seekable.length }, (_, i) => i).some(
            (i) => target >= seekable.start(i) && target <= seekable.end(i),
          )
        )
          return;
        const buffering = media.readyState < HTMLMediaElement.HAVE_FUTURE_DATA;
        if (buffering && !bufferingSince.current.has(player.current))
          bufferingSince.current.set(player.current, now);
        const last =
          (buffering ? lastBufferCorrection : lastCorrection).current.get(
            player.current,
          ) ?? -Infinity;
        if (
          last < oldestAttempt ||
          (last === oldestAttempt && difference < smallestDifference)
        ) {
          oldestAttempt = last;
          smallestDifference = difference;
          playerToPlayIndex = index;
        }
      });

      if (playerToPlayIndex !== -1) {
        const playerToPlay = playersWithTime[playerToPlayIndex];
        const fragments =
          playerToPlay.current.levels[playerToPlay.current.currentLevel]
            ?.details?.fragments;
        if (!fragments || fragments.length === 0) {
          return;
        }
        const closestFragment =
          findFragmentByTimestamp(fragments, playingDateMillis) ??
          findClosestFragment(fragments, playingDateMillis);
        const media = playerToPlay.current.media;
        const buffering =
          !!media && media.readyState < HTMLMediaElement.HAVE_FUTURE_DATA;
        if (buffering) {
          const since = bufferingSince.current.get(playerToPlay.current) ?? now;
          bufferingSince.current.set(playerToPlay.current, since);
          if (now - since < BUFFER_RECOVERY_MS) return;
        } else {
          bufferingSince.current.delete(playerToPlay.current);
        }
        const correctionTimes = buffering
          ? lastBufferCorrection
          : lastCorrection;
        const last =
          correctionTimes.current.get(playerToPlay.current) ?? -Infinity;
        if (
          media &&
          closestFragment?.programDateTime &&
          now - last >= (buffering ? BUFFER_RETRY_MS : SEEK_SETTLE_MS)
        ) {
          correctionTimes.current.set(playerToPlay.current, now);
          const currentDate = playerToPlay.current.playingDate;
          const aligned =
            currentDate &&
            Math.abs(currentDate.getTime() - playingDateMillis) <=
              MAX_DRIFT * 1000;
          if (!aligned || buffering) {
            const target = getSeekTarget(closestFragment, playingDateMillis);
            // A gap resolves to the first later fragment, while a timestamp
            // inside a fragment retains its exact position.
            media.currentTime = target;
          }
          media
            .play()
            .then(() => {
              const latest = useReferencePlayerStore.getState();
              if (!latest.isPlaying) media.pause();
              else if (
                latest.requestedTimestamp === state.requestedTimestamp &&
                latest.seekGeneration === state.seekGeneration
              ) {
                setHlsRefsError(playerToPlay, null);
              }
            })
            .catch(() => {});
        }
      }
    }
  }, [
    hlsRefs,
    playingDateRef,
    requestedTimestamp,
    seekSafely,
    setHlsRefsError,
    setIsLive,
    setIsPlaying,
    setReferencePlayer,
  ]);

  useControlledInterval(syncPlayers, SYNC_INTERVAL, true);

  useEffect(() => {
    const bindings: {
      player: Hls;
      handler: (event: Events.ERROR, data: ErrorData) => void;
    }[] = [];
    hlsRefs.forEach((player) => {
      if (player.current) {
        const instance = player.current;
        const handler = (_event: Events.ERROR, data: ErrorData) => {
          // A delayed HLS error must not undo an explicit user pause.
          if (!useReferencePlayerStore.getState().isPlaying) return;
          // Dont pause if this is the only playing player
          // console.warn("SyncManager: Error event", data);
          if (
            hlsRefs.filter(
              (p) => p.current && p.current.media && !p.current.media.paused,
            ).length === 1
          ) {
            if (player.current!.media) {
              const media = player.current!.media;
              media
                .play()
                .then(() => {
                  if (!useReferencePlayerStore.getState().isPlaying)
                    media.pause();
                })
                .catch(() => {});
            }
            return;
          }

          if (
            data.details === Hls.ErrorDetails.BUFFER_NUDGE_ON_STALL ||
            data.details === Hls.ErrorDetails.BUFFER_STALLED_ERROR ||
            data.details === Hls.ErrorDetails.LEVEL_LOAD_ERROR ||
            data.details === Hls.ErrorDetails.LEVEL_PARSING_ERROR
          ) {
            if (player.current!.media) {
              const media = player.current!.media;
              media
                .play()
                .then(() => {
                  if (!useReferencePlayerStore.getState().isPlaying)
                    media.pause();
                })
                .catch(() => {});
            }
            return;
          }

          if (player.current!.media) {
            player.current!.media.pause();
          }
        };
        instance.on(Hls.Events.ERROR, handler);
        bindings.push({ player: instance, handler });
      }
    });

    return () => {
      bindings.forEach(({ player, handler }) =>
        player.off(Hls.Events.ERROR, handler),
      );
    };
  }, [hlsRefs]);

  return children;
}

export default SyncManager;
