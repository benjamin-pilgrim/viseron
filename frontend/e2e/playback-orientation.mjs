/* eslint-disable no-await-in-loop -- Each check changes the same browser page and viewport. */
// Check the rendered orientation and fitting of timeline video, independent of
// encoded media. A quadrant poster paints through the same video element.
// CHROMIUM_PATH=/usr/bin/chromium node e2e/playback-orientation.mjs target.json artifacts
// target.json: {"url":"http://localhost:8888","start":1700000000,"cameraIdentifier":"camera1"}
import { chromium } from "@playwright/test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const target = JSON.parse(await fs.readFile(process.argv[2], "utf8"));
const artifacts =
  process.argv[3] ?? path.dirname(path.resolve(process.argv[2]));
await fs.mkdir(artifacts, { recursive: true });
const poster = `data:image/svg+xml,${encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="320" height="180"><path fill="#f00" d="M0 0h160v90H0z"/><path fill="#0f0" d="M160 0h160v90H160z"/><path fill="#00f" d="M0 90h160v90H0z"/><path fill="#ff0" d="M160 90h160v90H160z"/></svg>')}`;
const expected = {
  0: ["red", "green", "blue", "yellow"],
  90: ["blue", "red", "yellow", "green"],
  180: ["yellow", "blue", "green", "red"],
  270: ["green", "yellow", "red", "blue"],
};
const browser = await chromium.launch({
  executablePath: process.env.CHROMIUM_PATH,
  headless: true,
});
try {
  for (const rotation of [0, 90, 180, 270]) {
    const page = await browser.newPage();
    await page.route("**/api/v1/cameras*", async (route) => {
      const response = await route.fetch();
      const cameras = await response.json();
      cameras[target.cameraIdentifier].playback_rotation = rotation;
      await route.fulfill({ response, json: cameras });
    });
    // Leave media unloaded so the known poster, rather than a camera frame,
    // supplies the pixels. Real playback is covered by timeline-playback.mjs.
    await page.route("**/api/v1/hls/**", (route) => route.abort());
    const date = new Date(target.start * 1000).toISOString().slice(0, 10);
    await page.goto(
      `${target.url}/#/events?date=${date}&tab=timeline&camera=${target.cameraIdentifier}&timestamp=${target.start}`,
    );
    const video = page.locator("video").first();
    await video.waitFor();
    await video.evaluate((media, source) => {
      media.removeAttribute("src");
      media.load();
      media.poster = source;
    }, poster);
    for (const viewport of [
      { width: 1200, height: 900 },
      { width: 500, height: 850 },
    ]) {
      await page.setViewportSize(viewport);
      await page.waitForTimeout(250);
      const player = video.locator("..");
      const bounds = await player.boundingBox();
      assert(
        bounds?.width && bounds?.height,
        "Player must have visible dimensions",
      );
      const shot = await player.screenshot();
      await fs.writeFile(
        path.join(artifacts, `${rotation}-${viewport.width}.png`),
        shot,
      );
      const colors = await page.evaluate(
        async ({ png, rotation: angle }) => {
          const image = new Image();
          image.src = `data:image/png;base64,${png}`;
          await image.decode();
          const canvas = document.createElement("canvas");
          canvas.width = image.width;
          canvas.height = image.height;
          const context = canvas.getContext("2d");
          context.drawImage(image, 0, 0);
          const quarter = angle === 90 || angle === 270;
          const nativeWidth = quarter ? 180 : 320;
          const nativeHeight = quarter ? 320 : 180;
          const scale = Math.min(
            image.width / nativeWidth,
            image.height / nativeHeight,
          );
          const width = nativeWidth * scale;
          const height = nativeHeight * scale;
          const left = (image.width - width) / 2;
          const top = (image.height - height) / 2;
          return [
            [0.25, 0.25],
            [0.75, 0.25],
            [0.25, 0.75],
            [0.75, 0.75],
          ].map(([x, y]) => {
            const [r, g, b] = context.getImageData(
              Math.floor(left + x * width),
              Math.floor(top + y * height),
              1,
              1,
            ).data;
            if (r > g + 20 && r > b + 20) return "red";
            if (g > r + 20 && g > b + 20) return "green";
            if (b > r + 20 && b > g + 20) return "blue";
            if (r > b + 20 && g > b + 20) return "yellow";
            return `unexpected(${r},${g},${b})`;
          });
        },
        { png: shot.toString("base64"), rotation },
      );
      assert.deepEqual(
        colors,
        expected[rotation],
        `${rotation}° at ${viewport.width}px: all corners must remain visible and correctly oriented`,
      );
      console.log(`PASS orientation ${rotation}° at ${viewport.width}px`);
    }
    await page.close();
  }
} finally {
  await browser.close();
}
