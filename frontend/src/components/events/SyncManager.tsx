import { Dayjs } from "dayjs";
import Hls from "hls.js";
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
    if (lastSeek.current.timestamp !== state.requestedTimestamp) {
      lastSeek.current = { timestamp: state.requestedTimestamp, at: now };
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
        if (player === referencePlayer) return;
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
        const last = lastCorrection.current.get(player.current) ?? -Infinity;
        if (now - last < (buffering ? BUFFER_RETRY_MS : SEEK_SETTLE_MS)) return;
        lastCorrection.current.set(player.current, now);
        // A paused but aligned player only needs play, not a discontinuous seek.
        const aligned =
          playerDate && Math.abs(drift) <= MAX_DRIFT && !buffering;
        if (aligned || seekSafely(player.current, referenceDate)) {
          media
            .play()
            .then(() => {
              const latest = useReferencePlayerStore.getState();
              if (
                latest.isPlaying &&
                latest.requestedTimestamp === state.requestedTimestamp
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

    // If there are no players with time, play the first player
    if (playersWithTime.length === 0) {
      if (
        hlsRefs.length > 0 &&
        hlsRefs[0].current &&
        hlsRefs[0].current.media
      ) {
        hlsRefs[0].current.media
          .play()
          .then(() => {
            setHlsRefsError(hlsRefs[0], null);
          })
          .catch(() => {
            // Ignore play errors
          });
      }
    }

    // Check if all players are paused
    if (
      playersWithTime.every((player) => player.current.media?.paused ?? true)
    ) {
      const playingDateMillis = playingDateRef.current * 1000;

      let playerToPlayIndex = -1;
      let smallestDifference = Infinity;

      playersWithTime.forEach((player, index) => {
        const fragments =
          player.current.levels[player.current.currentLevel]?.details
            ?.fragments;
        if (!fragments || fragments.length === 0) {
          return;
        }

        const closestFragment = findClosestFragment(
          fragments,
          playingDateMillis,
        );
        if (!closestFragment || !closestFragment.programDateTime) {
          return;
        }

        const difference = Math.abs(
          playingDateMillis - closestFragment.programDateTime,
        );
        if (difference < smallestDifference) {
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
        const closestFragment = findClosestFragment(
          fragments,
          playingDateMillis,
        );
        if (
          closestFragment &&
          closestFragment.programDateTime &&
          playerToPlay.current.media
        ) {
          playerToPlay.current.media.currentTime = closestFragment.start;
        }
        if (playerToPlay.current.media) {
          playerToPlay.current.media
            .play()
            .then(() => {
              setHlsRefsError(playerToPlay, null);
            })
            .catch(() => {
              // Ignore play errors
            });
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
    hlsRefs.forEach((player) => {
      if (player.current) {
        player.current.on(Hls.Events.ERROR, (_event, data) => {
          // Dont pause if this is the only playing player
          // console.warn("SyncManager: Error event", data);
          if (
            hlsRefs.filter(
              (p) => p.current && p.current.media && !p.current.media.paused,
            ).length === 1
          ) {
            if (player.current!.media) {
              player.current!.media.play().catch(() => {
                // Ignore play errors
              });
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
              player.current!.media.play().catch(() => {
                // Ignore play errors
              });
            }
            return;
          }

          if (player.current!.media) {
            player.current!.media.pause();
          }
        });
      }
    });

    return () => {
      hlsRefs.forEach((player) => {
        if (player.current) {
          player.current.off(Hls.Events.ERROR);
        }
      });
    };
  }, [hlsRefs]);

  return children;
}

export default SyncManager;
