import fs from "node:fs/promises";
import path from "node:path";
import { chromium } from "playwright";

// Usage: CHROMIUM_PATH=/usr/bin/chromium node e2e/recorder-playback.mjs browser-target.json [artifact-directory] [--live]
const targetPath = process.argv[2];
const expectLiveOutage = process.argv.includes("--live");
if (!targetPath)
  throw new Error(
    "Pass browser-target.json from the retained fresh-database test",
  );
const root =
  process.argv.slice(3).find((argument) => argument !== "--live") ??
  path.dirname(path.resolve(targetPath));
await fs.mkdir(root, { recursive: true });
const target = JSON.parse(await fs.readFile(targetPath, "utf8"));
let playlist;
const playlistDeadline = Date.now() + 30000;
while (Date.now() < playlistDeadline) {
  const response = await fetch(
    `${target.url}/api/v1/hls/late_audio/index.m3u8?start_timestamp=${target.start}&end_timestamp=${Math.floor(Date.now() / 1000)}`,
  );
  const body = await response.text();
  if (response.ok && body.includes("#EXTINF:")) {
    playlist = body;
    break;
  }
  await new Promise((resolve) => setTimeout(resolve, 500));
}
if (!playlist)
  throw new Error("No recorded fragments available after 30 seconds");
await fs.writeFile(`${root}/playlist.m3u8`, playlist);
let init,
  initial,
  offset = 0,
  boundary;
for (const line of playlist.split("\n")) {
  if (line.startsWith("#EXT-X-MAP:")) {
    init = line.match(/URI="([^"]+)"/)[1];
    initial ??= init;
    if (init !== initial && boundary === undefined) boundary = offset;
  }
  if (line.startsWith("#EXTINF:"))
    offset += Number(line.slice(8).split(",")[0]);
}
if (boundary === undefined && !target.audioDropout)
  throw new Error("No init/track transition in playlist");
const firstTime =
  Date.parse(playlist.match(/#EXT-X-PROGRAM-DATE-TIME:([^\n]+)/)[1]) / 1000;
const date = new Date(firstTime * 1000).toISOString().slice(0, 10);
const browser = await chromium.launch({
  executablePath: process.env.CHROMIUM_PATH,
  headless: true,
  args: ["--autoplay-policy=no-user-gesture-required"],
});
const page = await browser.newPage({
  viewport: { width: 1440, height: 1000 },
  timezoneId: "UTC",
});
const report = {
  image: target.image,
  relayOverride: target.relayOverride ?? false,
  expectLiveOutage,
  boundary,
  audioDropout: target.audioDropout ?? false,
  firstTime,
  console: [],
  pageErrors: [],
  initRequests: [],
  samples: [],
};
page.on("console", (msg) => {
  if (
    msg.type() === "error" ||
    msg.type() === "warning" ||
    msg.text().includes("HLSJS")
  ) {
    const entry = { type: msg.type(), text: msg.text(), at: Date.now() };
    report.console.push(entry);
    Promise.all(
      msg.args().map((a) =>
        a
          .evaluate((x) =>
            typeof x === "object" && x
              ? {
                  type: x.type,
                  details: x.details,
                  fatal: x.fatal,
                  error: x.error?.message,
                  reason: x.reason,
                  fragStart: x.frag?.start,
                  fragSn: x.frag?.sn,
                  chunk: x.chunkMeta
                    ? {
                        id: x.chunkMeta.id,
                        sn: x.chunkMeta.sn,
                        start: x.chunkMeta.transmuxing?.start,
                      }
                    : null,
                  mediaTime: document.querySelector("video")?.currentTime,
                }
              : String(x),
          )
          .catch(() => null),
      ),
    ).then((args) => (entry.args = args));
  }
});
page.on("pageerror", (err) => report.pageErrors.push(String(err)));
page.on("response", (res) => {
  if (res.url().includes("/hls/init/late_audio/"))
    report.initRequests.push({ url: res.url(), status: res.status() });
});
try {
  await page.goto(
    `${target.url}/#/events?date=${date}&tab=timeline&camera=late_audio&timestamp=${Math.floor(firstTime + 1)}`,
    { waitUntil: "domcontentloaded" },
  );
  await page.locator("video").first().waitFor({ timeout: 45000 });
  await page.waitForFunction(
    () =>
      [...document.querySelectorAll("video")].some(
        (v) => v.readyState >= 2 && v.videoWidth > 0,
      ),
    {},
    { timeout: 45000 },
  );
  await page.screenshot({ path: `${root}/before.png` });
  for (let i = 0; i < (target.audioDropout ? 65 : 35); i++) {
    const sample = await page
      .locator("video")
      .first()
      .evaluate((v) => {
        window.__lastFrameAt ??= performance.now();
        v.requestVideoFrameCallback(
          () => (window.__lastFrameAt = performance.now()),
        );
        return {
          frameGap: (performance.now() - window.__lastFrameAt) / 1000,
          time: v.currentTime,
          paused: v.paused,
          ready: v.readyState,
          error: v.error?.message,
          frames: v.getVideoPlaybackQuality().totalVideoFrames,
          audioBytes: v.webkitAudioDecodedByteCount ?? null,
          buffers: [...Array(v.buffered.length)].map((_, i) => [
            v.buffered.start(i),
            v.buffered.end(i),
          ]),
        };
      });
    sample.wallTime = Date.now();
    report.samples.push(sample);
    if (i % 5 === 0) console.log(JSON.stringify({ i, ...sample }));
    await page.waitForTimeout(1000);
  }
  await page.screenshot({ path: `${root}/after.png` });
  report.body = await page.locator("body").innerText();
  const [first] = report.samples;
  const last = report.samples.at(-1);
  report.maxFrameGap = Math.max(...report.samples.map((s) => s.frameGap));
  let furthestTime = first.time,
    advancedAt = first.wallTime;
  report.longestFreeze = 0;
  for (const sample of report.samples) {
    if (sample.time > furthestTime + 0.04) {
      furthestTime = sample.time;
      advancedAt = sample.wallTime;
    } else
      report.longestFreeze = Math.max(
        report.longestFreeze,
        (sample.wallTime - advancedAt) / 1000,
      );
  }
  if (expectLiveOutage) {
    const recordingPath = path.join(
      path.dirname(path.resolve(targetPath)),
      `${target.container}-recording.txt`,
    );
    const deadline = Date.now() + 30000;
    while (Date.now() < deadline) {
      try {
        const recording = JSON.parse(await fs.readFile(recordingPath, "utf8"));
        const start = recording.scenario?.started;
        if (Number.isFinite(start)) {
          report.outage = { start: start + 30, end: start + 50 };
          break;
        }
      } catch {
        // The recorder probe writes its result after audio has returned.
      }
      await page.waitForTimeout(1000);
    }
    report.playedDuringOutage =
      report.outage !== undefined &&
      report.samples.some(
        (sample) =>
          sample.wallTime / 1000 < report.outage.end &&
          firstTime + sample.time > report.outage.start + 1 &&
          sample.ready >= 2 &&
          sample.frameGap < 2,
      );
    report.observedLiveOutage =
      report.outage !== undefined &&
      first.wallTime / 1000 < report.outage.start &&
      last.wallTime / 1000 > report.outage.end &&
      report.playedDuringOutage;
  }
  report.passed =
    (!expectLiveOutage || report.observedLiveOutage) &&
    report.pageErrors.length === 0 &&
    !report.console.some((x) => x.text.includes("HLSJS Error")) &&
    report.longestFreeze < 4 &&
    report.maxFrameGap < 4 &&
    (last.audioBytes === null || last.audioBytes > 0) &&
    (target.audioDropout
      ? first.time < 10 && last.time > 55
      : first.time < boundary && last.time > boundary + 2) &&
    last.frames > first.frames + 20 &&
    !last.error &&
    (target.audioDropout ||
      new Set(
        report.initRequests
          .filter((x) => x.status === 200)
          .map((x) => new URL(x.url).pathname),
      ).size >= 2);
  if (!report.passed)
    throw new Error(
      "Browser playback failed across the audio transition (see browser-report.json)",
    );
  console.log(
    target.audioDropout
      ? "PASS browser playback continued through audio dropout and recovery"
      : "PASS browser playback crossed init/track change",
  );
} finally {
  await fs.writeFile(
    `${root}/browser-report.json`,
    JSON.stringify(report, null, 2),
  );
  await page.screenshot({ path: `${root}/final.png` }).catch(() => {});
  await browser.close();
}
