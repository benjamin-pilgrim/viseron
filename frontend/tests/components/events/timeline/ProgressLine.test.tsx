import { act, cleanup, render } from "@testing-library/react";
import { useRef } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { ProgressLine } from "components/events/timeline/ProgressLine";

const { player } = vi.hoisted(() => ({
  player: {
    media: null as HTMLVideoElement | null,
    playingDate: new Date("2025-01-01T00:00:05Z"),
  },
}));

vi.mock("components/events/utils", () => ({
  useReferencePlayerStore: () => player,
  getYPosition: (_start: number, _end: number, _time: number, height: number) =>
    height / 2,
}));

function Timeline() {
  const containerRef = useRef<HTMLDivElement>(null);
  const startRef = useRef(0);
  const endRef = useRef(100);
  return (
    <div data-testid="scroll-container">
      <div data-testid="timeline" ref={containerRef}>
        <ProgressLine
          containerRef={containerRef}
          startRef={startRef}
          endRef={endRef}
        />
      </div>
    </div>
  );
}

describe("ProgressLine", () => {
  let height: number;
  let notifyResize: () => void;
  let measure: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    vi.useFakeTimers();
    height = 600;
    notifyResize = () => {};
    player.media = document.createElement("video");
    Object.defineProperty(player.media, "currentTime", { value: 1 });
    vi.stubGlobal(
      "ResizeObserver",
      class {
        constructor(callback: () => void) {
          notifyResize = callback;
        }

        observe = vi.fn();

        disconnect = vi.fn();
      },
    );
    measure = vi
      .spyOn(Element.prototype, "getBoundingClientRect")
      .mockImplementation(() => ({ height }) as DOMRect);
  });

  afterEach(() => {
    cleanup();
    measure.mockRestore();
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it("positions immediately and updates on resize without measuring every playback update", () => {
    const { getByTestId } = render(<Timeline />);
    const timeline = getByTestId("timeline");
    const line = timeline.firstElementChild as HTMLElement;
    act(() => vi.advanceTimersByTime(100));

    act(() => player.media!.dispatchEvent(new Event("timeupdate")));
    expect(line.style.transform).toBe("translateY(300px)");
    expect(line.style.visibility).toBe("visible");
    expect(line.style.transition).toBe("none");
    expect(measure).toHaveBeenCalledTimes(1);

    act(() => vi.advanceTimersByTime(50));
    expect(line.style.transition).toBe("");
    act(() => {
      for (let index = 0; index < 100; index++) {
        player.media!.dispatchEvent(new Event("timeupdate"));
      }
    });
    expect(measure).toHaveBeenCalledTimes(1);

    height = 1000;
    act(() => notifyResize());
    act(() => player.media!.dispatchEvent(new Event("timeupdate")));
    expect(line.style.transform).toBe("translateY(500px)");
    expect(measure).toHaveBeenCalledTimes(2);

    const scroller = getByTestId("scroll-container");
    scroller.scrollTop = 250;
    act(() => scroller.dispatchEvent(new Event("scroll")));
    act(() => player.media!.dispatchEvent(new Event("timeupdate")));
    expect(line.style.transform).toBe("translateY(500px)");
    expect(measure).toHaveBeenCalledTimes(2);
  });
});
