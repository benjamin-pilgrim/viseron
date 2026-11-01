/* Browser smoke for test_run_recording.sh's disposable Viseron instance. */
const { createRequire } = require("node:module");
const path = require("node:path");

const [, , repo, origin] = process.argv;
if (!repo || !origin) throw new Error("Expected checkout and origin arguments");
const packageRoot = process.env.VISERON_TEST_PLAYWRIGHT_ROOT || path.join(repo, "frontend");
const requireFrontend = createRequire(path.join(packageRoot, "package.json"));
const { chromium } = requireFrontend("@playwright/test");

(async () => {
  const browser = await chromium.launch({
    executablePath: process.env.VISERON_TEST_CHROMIUM || undefined,
    headless: true,
    args: ["--no-sandbox", "--autoplay-policy=no-user-gesture-required"],
  });
  try {
    const page = await browser.newPage();
    const mediaResponses = [];
    page.on("response", (response) => {
      if (/\/api\/v1\/hls\/vertical_camera\/index\.m3u8|\/run-init\/vertical_camera\//.test(response.url())) {
        mediaResponses.push({ url: response.url(), status: response.status() });
      }
    });
    await page.goto(`${origin}/#/events?camera=vertical_camera&tab=timeline`);
    const video = page.locator("video");
    await video.waitFor({ state: "attached", timeout: 45000 });
    await page.waitForFunction(() => {
      const video = document.querySelector("video");
      return video && !video.paused && video.readyState >= 2 && video.videoWidth > 0 && video.getVideoPlaybackQuality().totalVideoFrames > 10;
    }, null, { timeout: 60000 });
    const first = await video.evaluate((v) => ({ time: v.currentTime, frames: v.getVideoPlaybackQuality().totalVideoFrames }));
    await page.waitForFunction(({ time, frames }) => {
      const video = document.querySelector("video");
      return video && video.currentTime > time + 2 && video.getVideoPlaybackQuality().totalVideoFrames > frames;
    }, first, { timeout: 15000 });
    const last = await video.evaluate((v) => ({ time: v.currentTime, frames: v.getVideoPlaybackQuality().totalVideoFrames, width: v.videoWidth, height: v.videoHeight }));
    if (!mediaResponses.some((r) => r.status === 200 && r.url.includes("/api/v1/hls/")) ||
        !mediaResponses.some((r) => r.status === 200 && r.url.includes("/run-init/"))) {
      throw new Error(`Expected successful playlist and run-init responses: ${JSON.stringify(mediaResponses)}`);
    }
    console.log(`PASS: browser played ${last.width}x${last.height} video: ${first.time.toFixed(2)} to ${last.time.toFixed(2)} seconds, ${first.frames} to ${last.frames} decoded frames; playlist and run init HTTP 200`);
    if (process.env.VISERON_TEST_PLAYLIST_RESYNC === "1") {
      let previousClient;
      let injected = false;
      let replacementClient;
      await page.route("**/api/v1/hls/vertical_camera/index.m3u8**", async (route) => {
        const client = route.request().headers()["hls-client-id"];
        if (!injected && client) {
          previousClient = client;
          injected = true;
          await route.fulfill({ status: 410, body: "Playlist changed; reload required" });
        } else {
          if (client && previousClient && client !== previousClient) replacementClient = client;
          await route.continue();
        }
      });
      await page.waitForFunction(() => {
        const video = document.querySelector("video");
        return video && !video.paused;
      });
      await page.waitForTimeout(1000);
      const deadline = Date.now() + 45000;
      while (!replacementClient && Date.now() < deadline) await page.waitForTimeout(250);
      if (!replacementClient) throw new Error("Playlist recovery did not create a new HLS client");
      await page.waitForFunction(() => {
        const video = document.querySelector("video");
        return video && !video.paused && video.readyState >= 2 && video.videoWidth > 0;
      }, null, { timeout: 30000 });
      const resumed = await video.evaluate((v) => ({ time: v.currentTime, frames: v.getVideoPlaybackQuality().totalVideoFrames }));
      await page.waitForFunction(({ time, frames }) => {
        const video = document.querySelector("video");
        return video && video.currentTime > time + 2 && video.getVideoPlaybackQuality().totalVideoFrames > frames;
      }, resumed, { timeout: 20000 });
      console.log("PASS: browser recovered from playlist change with a new client and resumed decoding");
    }
  } finally {
    await browser.close();
  }
})().catch((error) => { console.error(error); process.exitCode = 1; });
