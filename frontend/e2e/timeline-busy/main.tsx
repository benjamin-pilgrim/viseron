import { useCallback, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";

import { VirtualList } from "components/events/timeline/VirtualList";
import { getTimelineItems, useFilterStore } from "components/events/utils";
import type { CameraEvent } from "lib/types";

const start = 1_793_491_200;
const events: CameraEvent[] = Array.from({ length: 4000 }, (_, i) => {
  const timestamp = start - ((i * 43) % 86400);
  return {
    id: i,
    type: "motion",
    camera_identifier: `camera${i % 4}`,
    start_timestamp: timestamp - 20,
    end_timestamp: timestamp,
    start_time: new Date((timestamp - 20) * 1000).toISOString(),
    end_time: new Date(timestamp * 1000).toISOString(),
    snapshot_path: "",
    duration: 20,
  } as CameraEvent;
});

function App() {
  const parent = useRef<HTMLDivElement>(null);
  const [mounted, setMounted] = useState(false);
  const setParent = useCallback((element: HTMLDivElement | null) => {
    parent.current = element;
    setMounted(element !== null);
  }, []);
  const startRef = useRef(start);
  const endRef = useRef(start - 86400);
  const items = useMemo(() => {
    const before = performance.now();
    const result = getTimelineItems(
      startRef,
      events,
      [],
      useFilterStore.getState().filters,
    );
    document.documentElement.dataset.buildMs = String(
      performance.now() - before,
    );
    document.documentElement.dataset.itemCount = String(
      Object.keys(result).length,
    );
    return result;
  }, []);
  return (
    <div
      ref={setParent}
      data-testid="timeline"
      style={{ height: 500, width: 200, overflow: "auto" }}
    >
      {mounted && (
        <VirtualList
          parentRef={parent}
          startRef={startRef}
          endRef={endRef}
          timelineItems={items}
        />
      )}
    </div>
  );
}
createRoot(document.getElementById("root")!).render(<App />);
