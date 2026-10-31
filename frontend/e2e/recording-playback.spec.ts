import { expect, test } from "@playwright/test";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";

let mediaDirectory: string;
test.beforeAll(() => {
  mediaDirectory = mkdtempSync(join(tmpdir(), "viseron-browser-playback-"));
  execFileSync("ffmpeg", [
    "-hide_banner",
    "-loglevel",
    "error",
    "-f",
    "lavfi",
    "-i",
    "testsrc2=size=320x180:rate=10",
    "-t",
    "30",
    "-c:v",
    "libx264",
    "-preset",
    "ultrafast",
    "-g",
    "10",
    "-sc_threshold",
    "0",
    "-f",
    "hls",
    "-hls_time",
    "1",
    "-hls_list_size",
    "0",
    "-hls_segment_type",
    "fmp4",
    "-hls_flags",
    "program_date_time",
    "-hls_segment_filename",
    join(mediaDirectory, "segment%03d.m4s"),
    join(mediaDirectory, "index.m3u8"),
  ]);
});
test.afterAll(() => rmSync(mediaDirectory, { recursive: true, force: true }));

test("two actual HLS players converge smoothly and seek large drift", async ({
  page,
}) => {
  await page.route("**/playback-fixture/*", async (route) => {
    const filename = basename(new URL(route.request().url()).pathname);
    await route.fulfill({
      status: 200,
      contentType: filename.endsWith(".m3u8")
        ? "application/vnd.apple.mpegurl"
        : "video/mp4",
      body: readFileSync(join(mediaDirectory, filename)),
    });
  });
  await page.goto("/e2e/recording-playback/index.html");
  await page.waitForFunction(() => {
    const state = window as unknown as {
      playbackReady: boolean;
      playbackPlayers: { playingDate: Date | null }[];
    };
    return (
      state.playbackReady &&
      state.playbackPlayers.every((player) => player.playingDate)
    );
  });
  await page.waitForTimeout(2500);
  const setDrift = (seconds: number) =>
    page.evaluate((delay) => {
      const state = window as unknown as { playbackVideos: HTMLVideoElement[] };
      const [first, second] = state.playbackVideos;
      second.currentTime = first.currentTime - delay;
    }, seconds);
  const readDrift = () =>
    page.evaluate(() => {
      const state = window as unknown as {
        playbackPlayers: { playingDate: Date; media: HTMLVideoElement }[];
      };
      const [first, second] = state.playbackPlayers;
      return {
        drift:
          (first.playingDate.getTime() - second.playingDate.getTime()) / 1000,
        rate: second.media.playbackRate,
      };
    });
  await setDrift(1.2);
  await page.waitForTimeout(500);
  const before = await readDrift();
  await page.waitForTimeout(3500);
  const after = await readDrift();
  expect(before.drift).toBeGreaterThan(0.8);
  expect(before.rate).toBeGreaterThan(1);
  expect(after.drift).toBeLessThan(before.drift - 0.15);
  await setDrift(3);
  await expect
    .poll(async () => Math.abs((await readDrift()).drift))
    .toBeLessThan(0.6);
  // Simulate a media element reporting buffered data exhaustion while the
  // other real HLS player continues. Recovery waits before seeking.
  await page.evaluate(() => {
    const state = window as unknown as { playbackVideos: HTMLVideoElement[] };
    Object.defineProperty(state.playbackVideos[1], "readyState", {
      configurable: true,
      value: 2,
    });
  });
  await setDrift(3);
  await page.waitForTimeout(500);
  expect((await readDrift()).drift).toBeGreaterThan(2);
  await expect
    .poll(async () => Math.abs((await readDrift()).drift), { timeout: 4000 })
    .toBeLessThan(0.6);
  await page.evaluate(() => {
    const state = window as unknown as { playbackVideos: HTMLVideoElement[] };
    Reflect.deleteProperty(state.playbackVideos[1], "readyState");
  });

  // Both media elements can stall together during a shared storage outage.
  await page.evaluate(() => {
    const state = window as unknown as {
      playbackVideos: HTMLVideoElement[];
      recoveryAttempts: number;
    };
    state.recoveryAttempts = 0;
    state.playbackVideos.forEach((video) => {
      const play = video.play.bind(video);
      video.play = () => {
        state.recoveryAttempts += 1;
        return play();
      };
      Object.defineProperty(video, "readyState", {
        configurable: true,
        value: 2,
      });
    });
  });
  await expect
    .poll(
      () =>
        page.evaluate(
          () =>
            (window as unknown as { recoveryAttempts: number })
              .recoveryAttempts,
        ),
      { timeout: 4000 },
    )
    .toBeGreaterThan(0);
  const attempts = await page.evaluate(
    () => (window as unknown as { recoveryAttempts: number }).recoveryAttempts,
  );
  await page.waitForTimeout(1000);
  expect(
    await page.evaluate(
      () =>
        (window as unknown as { recoveryAttempts: number }).recoveryAttempts,
    ),
  ).toBe(attempts);
  await page.evaluate(() => {
    const state = window as unknown as { playbackVideos: HTMLVideoElement[] };
    state.playbackVideos.forEach((video) =>
      Reflect.deleteProperty(video, "readyState"),
    );
  });

  await page.evaluate(() => {
    const state = window as unknown as {
      setPlaybackSpeed: (speed: number) => void;
    };
    state.setPlaybackSpeed(10);
  });
  await setDrift(1.2);
  await page.waitForTimeout(120);
  const fastBefore = await readDrift();
  await page.waitForTimeout(500);
  const fastAfter = await readDrift();
  expect(fastBefore.rate).toBeGreaterThan(10);
  expect(fastAfter.drift).toBeLessThan(fastBefore.drift - 0.1);
});

test("a 10x seek remains eligible after delayed seeked delivery", async ({
  page,
}) => {
  await page.route("**/playback-fixture/*", async (route) => {
    const filename = basename(new URL(route.request().url()).pathname);
    await route.fulfill({
      status: 200,
      contentType: filename.endsWith(".m3u8")
        ? "application/vnd.apple.mpegurl"
        : "video/mp4",
      body: readFileSync(join(mediaDirectory, filename)),
    });
  });
  await page.goto("/e2e/recording-playback/index.html");
  await page.waitForFunction(
    () =>
      (window as any).playbackReady &&
      (window as any).playbackPlayers.every(
        (player: any) => player.playingDate,
      ),
  );
  await page.evaluate(() => {
    const state = window as any;
    state.setPlaybackSpeed(10);
    state.playbackVideos[0].playbackRate = 10;
    const fragment = state.playbackPlayers[0].levels[0].details.fragments[0];
    state.seekForTest(
      Math.floor(fragment.programDateTime / 1000) + 5,
      fragment.start + 5,
    );
  });
  await expect
    .poll(() => page.evaluate(() => (window as any).delayedSeekAdvance ?? -1))
    .toBeGreaterThan(2);
  await expect
    .poll(
      () =>
        page.evaluate(() => {
          const state = window as any;
          return (
            state.referenceForTest() === state.playbackPlayers[0] &&
            state.playbackVideos[0].currentTime > 8
          );
        }),
      { timeout: 10000 },
    )
    .toBe(true);
});
