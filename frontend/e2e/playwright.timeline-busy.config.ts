import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: ".",
  testMatch: "timeline-busy.spec.ts",
  use: {
    baseURL: "http://127.0.0.1:5196",
    headless: true,
    launchOptions: {
      executablePath: process.env.PLAYWRIGHT_CHROMIUM_PATH,
      args: ["--no-sandbox"],
    },
  },
  webServer: {
    command: "npm run start -- --host 127.0.0.1 --port 5196",
    cwd: "..",
    url: "http://127.0.0.1:5196",
    reuseExistingServer: false,
  },
});
