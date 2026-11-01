import { afterEach, describe, expect, it, vi } from "vitest";

import { calculateEnd, calculateStart } from "components/events/utils";
import {
  dayjsSetDefaultTimezone,
  getDayjsFromDateString,
  getDayjsFromDateTimeString,
} from "lib/helpers/dates";

afterEach(() => {
  vi.useRealTimers();
  dayjsSetDefaultTimezone(Intl.DateTimeFormat().resolvedOptions().timeZone);
});

describe("timeline day boundaries", () => {
  it.each([
    ["2026-03-29", 23],
    ["2026-10-25", 25],
  ])("covers the full local day on %s", (date, hours) => {
    dayjsSetDefaultTimezone("Europe/London");
    const selected = getDayjsFromDateTimeString(`${date}T12:00:00Z`);
    const start = calculateStart(selected);
    const end = calculateEnd(selected);
    const nextDay = getDayjsFromDateString(date)
      .add(1, "day")
      .format("YYYY-MM-DD");
    expect(end).toBe(getDayjsFromDateString(date).unix());
    expect(start).toBe(getDayjsFromDateString(nextDay).unix());
    expect(start - end).toBe(hours * 60 * 60);
  });

  it("uses local midnight for today after the clock changes", () => {
    dayjsSetDefaultTimezone("Europe/London");
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-03-29T12:00:00Z"));
    expect(calculateEnd(null)).toBe(
      getDayjsFromDateString("2026-03-29").unix(),
    );
  });
});
