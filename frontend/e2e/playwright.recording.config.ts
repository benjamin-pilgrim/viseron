import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: ".",
  testMatch: "recording-playback.spec.ts",
  use: {
    baseURL: "http://127.0.0.1:5195",
    headless: true,
    launchOptions: {
      executablePath: process.env.PLAYWRIGHT_CHROMIUM_PATH,
      args: ["--no-sandbox", "--autoplay-policy=no-user-gesture-required"],
    },
  },
  webServer: {
    command: "npm run start -- --host 127.0.0.1 --port 5195",
    cwd: "..",
    url: "http://127.0.0.1:5195",
    reuseExistingServer: true,
  },
});
