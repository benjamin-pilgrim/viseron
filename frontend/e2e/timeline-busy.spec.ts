import { expect, test } from "@playwright/test";

test("busy day timeline builds and navigates with bounded rendered rows", async ({
  page,
}) => {
  await page.goto("/e2e/timeline-busy/index.html");
  const timeline = page.getByTestId("timeline");
  await expect(timeline).toBeVisible();
  const buildMs = Number(
    await page.locator("html").getAttribute("data-build-ms"),
  );
  const items = Number(
    await page.locator("html").getAttribute("data-item-count"),
  );
  console.log(
    `4,000 events: ${items} occupied minutes built in ${buildMs.toFixed(1)}ms`,
  );
  expect(items).toBeGreaterThan(1000);
  const viewportRows = timeline.locator(":scope > div > div");
  await expect.poll(() => viewportRows.count()).toBeGreaterThan(10);
  expect(await viewportRows.count()).toBeLessThan(120);
  await timeline.evaluate((element) => {
    element.scrollTop = element.scrollHeight;
  });
  await expect
    .poll(() => timeline.evaluate((element) => element.scrollTop))
    .toBeGreaterThan(1000);
  await expect.poll(() => viewportRows.count()).toBeLessThan(120);
  await expect
    .poll(() => viewportRows.first().evaluate((row) => row.style.transform))
    .not.toBe("translateY(0px)");
  await timeline.evaluate((element) => {
    element.scrollTop = 0;
  });
  await expect
    .poll(() => timeline.evaluate((element) => element.scrollTop))
    .toBe(0);
  expect(buildMs).toBeLessThan(2000);
});
