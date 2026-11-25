// Opening a historical timeline and seeking must produce advancing video frames.
// Use at least 40 seconds of continuous real media and the packaged frontend;
// no HLS or MediaSource mocks.
// CHROMIUM_PATH=/usr/bin/chromium node e2e/timeline-playback.mjs target.json artifacts
// target.json: {"url":"http://localhost:8888","start":1700000000,
//               "cameraIdentifiers":["camera1"]}
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { chromium } from "playwright";

assert(process.argv[2], "Pass target.json and an optional artifact directory");
const target = JSON.parse(await fs.readFile(process.argv[2], "utf8"));
const artifacts =
  process.argv[3] ?? path.dirname(path.resolve(process.argv[2]));
await fs.mkdir(artifacts, { recursive: true });
assert(
  target.cameraIdentifiers?.length,
  "Specify cameraIdentifiers in target.json",
);
const browser = await chromium.launch({
  executablePath: process.env.CHROMIUM_PATH,
  headless: true,
  args: ["--autoplay-policy=no-user-gesture-required"],
});
const results = [];
try {
  for (const camera of target.cameraIdentifiers) {
    const page = await browser.newPage({ timezoneId: "UTC" });
    const result = { camera, passed: false };
    results.push(result);
    try {
      const response = await fetch(
        `${target.url}/api/v1/hls/${camera}/index.m3u8?start_timestamp=${target.start}&end_timestamp=${Math.floor(Date.now() / 1000)}`,
      );
      assert(response.ok, `Playlist returned ${response.status}`);
      const playlist = await response.text();
      const firstTime =
        Date.parse(playlist.match(/#EXT-X-PROGRAM-DATE-TIME:([^\n]+)/)?.[1]) /
        1000;
      assert(Number.isFinite(firstTime), "No recorded footage in target range");
      const duration = [...playlist.matchAll(/#EXTINF:([\d.]+)/g)].reduce(
        (total, match) => total + Number(match[1]),
        0,
      );
      assert(duration >= 40, "Need at least 40 seconds of continuous footage");
      const timestamp = Math.floor(firstTime + 15);
      const date = new Date(timestamp * 1000).toISOString().slice(0, 10);
      await page.goto(
        `${target.url}/#/events?date=${date}&tab=timeline&camera=${camera}&timestamp=${timestamp}`,
      );
      const video = page.locator("video").first();
      await video.waitFor({ timeout: 15000 });
      const sample = () =>
        video.evaluate((media) => ({
          time: media.currentTime,
          ready: media.readyState,
          frames: media.getVideoPlaybackQuality().totalVideoFrames,
          error: media.error?.message,
        }));
      const advances = async () => {
        const before = await sample();
        await page.waitForFunction(
          ({ time, frames }) => {
            const media = document.querySelector("video");
            return (
              media?.readyState >= 2 &&
              media.currentTime >= time + 2 &&
              media.getVideoPlaybackQuality().totalVideoFrames > frames + 20
            );
          },
          before,
          { timeout: 15000 },
        );
        const after = await sample();
        assert(!after.error, after.error);
        return { before, after };
      };
      result.startup = await advances();
      await video.hover();
      const beforeSeek = await sample();
      await page.getByLabel("Forward 10 seconds", { exact: true }).click();
      await page.waitForFunction(
        (time) => document.querySelector("video")?.currentTime >= time + 9,
        beforeSeek.time,
        { timeout: 15000 },
      );
      result.afterSeek = await advances();
      result.passed = true;
      console.log(`PASS timeline startup and seeking: ${camera}`);
    } catch (error) {
      result.error = error.message;
      throw error;
    } finally {
      await page.screenshot({ path: path.join(artifacts, `${camera}.png`) });
      await page.close();
    }
  }
} finally {
  await fs.writeFile(
    path.join(artifacts, "timeline-playback.json"),
    JSON.stringify(results, null, 2),
  );
  await browser.close();
}
