import { CSSProperties, useCallback, useRef, useState } from "react";

import { useResizeObserver } from "hooks/UseResizeObserver";

/** Rotate the displayed video while fitting its complete image inside the player. */
export function usePlaybackRotation(rotation = 0) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [size, setSize] = useState({ width: 0, height: 0 });
  const resized = useCallback<ResizeObserverCallback>((entries) => {
    const { width, height } = entries[0].contentRect;
    setSize({ width, height });
  }, []);
  useResizeObserver(containerRef, resized);

  const quarterTurn = rotation === 90 || rotation === 270;
  const videoStyle: CSSProperties = quarterTurn
    ? {
        position: "absolute",
        left: "50%",
        top: "50%",
        width: size.height,
        height: size.width,
        transform: `translate(-50%, -50%) rotate(${rotation}deg)`,
      }
    : {
        width: "100%",
        height: "100%",
        transform: rotation ? `rotate(${rotation}deg)` : undefined,
      };
  return { containerRef, videoStyle };
}
