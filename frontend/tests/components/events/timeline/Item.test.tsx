import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { Row } from "components/events/timeline/Row";
import { DEFAULT_ITEM } from "components/events/utils";
import type { CameraRecordingEvent, CameraSnapshotEvent } from "lib/types";

vi.mock("components/events/SnapshotEvent", () => ({
  SnapshotEvent: ({ events }: { events: CameraSnapshotEvent[] }) => (
    <span>{events.map((event) => event.id).join(",")}</span>
  ),
}));
vi.mock("components/events/timeline/ActivityLine", () => ({
  ActivityLine: ({
    cameraEvent,
  }: {
    cameraEvent: CameraRecordingEvent | null;
  }) => <span>{cameraEvent?.type}</span>,
}));
vi.mock("components/events/timeline/TimeTick", () => ({
  TimeTick: () => null,
}));

describe("timeline event updates", () => {
  it("updates same-count snapshots and timed event type without moving the row", () => {
    const virtualItem = {
      key: 0,
      index: 0,
      start: 0,
      end: 8,
      size: 8,
      lane: 0,
    };
    const snapshot = (id: number) => ({ id }) as CameraSnapshotEvent;
    const timed = (type: "motion" | "recording") =>
      ({
        type,
        start_timestamp: 1,
        end_timestamp: 2,
      }) as CameraRecordingEvent;
    const item = {
      ...DEFAULT_ITEM,
      time: 100,
      activityLineVariant: "round" as const,
      timedEvent: timed("motion"),
      snapshotEvents: [snapshot(1)],
    };
    const { rerender } = render(<Row virtualItem={virtualItem} item={item} />);
    expect(screen.getByText("1")).toBeTruthy();
    expect(screen.getByText("motion")).toBeTruthy();
    rerender(
      <Row
        virtualItem={virtualItem}
        item={{
          ...item,
          snapshotEvents: [snapshot(2)],
        }}
      />,
    );
    expect(screen.getByText("2")).toBeTruthy();
    const updatedSnapshots = [snapshot(2)];
    rerender(
      <Row
        virtualItem={virtualItem}
        item={{ ...item, snapshotEvents: updatedSnapshots }}
      />,
    );
    rerender(
      <Row
        virtualItem={virtualItem}
        item={{
          ...item,
          snapshotEvents: updatedSnapshots,
          timedEvent: timed("recording"),
        }}
      />,
    );
    expect(screen.getByText("recording")).toBeTruthy();
  });
});
