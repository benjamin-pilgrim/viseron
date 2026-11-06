/**
 * HLS passthrough timing does not account for MP4 startup edit lists, while MSE
 * does. Bake those edits into each track's decode clock before passing to HLS.
 * Media payloads and effective presentation times remain unchanged.
 */
interface Box {
  start: number;
  body: number;
  end: number;
  type: string;
}
type Offsets = ReadonlyMap<number, bigint>;

function boxes(data: DataView, start = 0, end = data.byteLength): Box[] {
  const result: Box[] = [];
  let position = start;
  while (position < end) {
    if (position + 8 > end) throw new Error("Truncated MP4 box header");
    let size = data.getUint32(position);
    let header = 8;
    if (size === 1) {
      if (position + 16 > end) throw new Error("Truncated MP4 box size");
      const extended = data.getBigUint64(position + 8);
      if (extended > BigInt(Number.MAX_SAFE_INTEGER))
        throw new Error("MP4 box too large");
      size = Number(extended);
      header = 16;
    } else if (size === 0) size = end - position;
    if (size < header || position + size > end)
      throw new Error("Invalid MP4 box size");
    result.push({
      start: position,
      body: position + header,
      end: position + size,
      type: String.fromCharCode(
        data.getUint8(position + 4),
        data.getUint8(position + 5),
        data.getUint8(position + 6),
        data.getUint8(position + 7),
      ),
    });
    position += size;
  }
  return result;
}
function children(data: DataView, parent: Box): Box[] {
  return boxes(data, parent.body, parent.end);
}
function requireBytes(box: Box, count: number) {
  if (box.body + count > box.end)
    throw new Error(`Truncated MP4 ${box.type} box`);
}
function timeField(data: DataView, box: Box): number {
  requireBytes(box, 4);
  const version = data.getUint8(box.body);
  if (version > 1) throw new Error(`Unsupported MP4 ${box.type} version`);
  const offset = version === 1 ? 20 : 12;
  requireBytes(box, offset + 4);
  return data.getUint32(box.body + offset);
}

// Only a startup delay followed by an unbounded, rate-one media edit can be
// represented by a constant tfdt offset. Keep other edit programs untouched.
function editOffset(
  data: DataView,
  edit: Box,
  movieScale: number,
  trackScale: number,
): bigint | undefined {
  requireBytes(edit, 8);
  const version = data.getUint8(edit.body);
  const count = data.getUint32(edit.body + 4);
  if (version > 1 || (count !== 1 && count !== 2)) return undefined;
  const width = version ? 20 : 12;
  requireBytes(edit, 8 + width * count);
  const first = edit.body + 8;
  const second = first + width;
  const duration = (p: number) =>
    version ? data.getBigUint64(p) : BigInt(data.getUint32(p));
  const mediaTime = (p: number) =>
    version ? data.getBigInt64(p + 8) : BigInt(data.getInt32(p + 4));
  const rate = (p: number) => data.getUint32(p + (version ? 16 : 8));
  if (count === 1)
    return duration(first) === 0n &&
      mediaTime(first) === 0n &&
      rate(first) === 0x10000
      ? 0n
      : undefined;
  if (
    mediaTime(first) !== -1n ||
    mediaTime(second) < 0n ||
    duration(second) !== 0n ||
    rate(first) !== 0x10000 ||
    rate(second) !== 0x10000
  )
    return undefined;
  if (!movieScale || !trackScale) throw new Error("Invalid MP4 timescale");
  return (
    (duration(first) * BigInt(trackScale) +
      BigInt(Math.floor(movieScale / 2))) /
      BigInt(movieScale) -
    mediaTime(second)
  );
}

export function normalizeMp4Init(data: ArrayBuffer): {
  data: ArrayBuffer;
  offsets: Offsets;
} {
  const view = new DataView(data);
  const top = boxes(view);
  const moov = top.find((box) => box.type === "moov");
  const offsets = new Map<number, bigint>();
  if (!moov) return { data, offsets };
  const moovChildren = children(view, moov);
  const mvhd = moovChildren.find((box) => box.type === "mvhd");
  if (!mvhd) return { data, offsets };
  const movieScale = timeField(view, mvhd);
  const remove = new Set<number>();
  for (const track of moovChildren.filter((box) => box.type === "trak")) {
    const parts = children(view, track);
    const edts = parts.find((box) => box.type === "edts");
    if (edts) {
      const elst = children(view, edts).find((box) => box.type === "elst");
      const tkhd = parts.find((box) => box.type === "tkhd");
      const mdia = parts.find((box) => box.type === "mdia");
      const mdhd =
        mdia && children(view, mdia).find((box) => box.type === "mdhd");
      if (!elst || !tkhd || !mdhd) return { data, offsets: new Map() };
      const offset = editOffset(view, elst, movieScale, timeField(view, mdhd));
      if (offset === undefined) return { data, offsets: new Map() };
      if (offset !== 0n) {
        offsets.set(timeField(view, tkhd), offset);
        remove.add(edts.start);
      }
    }
  }
  if (!offsets.size) return { data, offsets };
  function rewrite(list: Box[]): Uint8Array<ArrayBuffer> {
    const parts = list
      .filter((box) => !remove.has(box.start))
      .map((box) => {
        if (box.type !== "moov" && box.type !== "trak")
          return new Uint8Array(data, box.start, box.end - box.start);
        const body = rewrite(children(view, box));
        const headerSize = box.body - box.start;
        const result = new Uint8Array(headerSize + body.length);
        result.set(new Uint8Array(data, box.start, headerSize));
        const header = new DataView(result.buffer);
        if (headerSize === 16) header.setBigUint64(8, BigInt(result.length));
        else header.setUint32(0, result.length);
        result.set(body, headerSize);
        return result;
      });
    const result = new Uint8Array(
      parts.reduce((n, part) => n + part.length, 0),
    );
    let position = 0;
    parts.forEach((part) => {
      result.set(part, position);
      position += part.length;
    });
    return result;
  }
  return { data: rewrite(top).buffer, offsets };
}

export function normalizeMp4Fragment(
  data: ArrayBuffer,
  offsets: Offsets,
): ArrayBuffer {
  if (!offsets.size) return data;
  const result = data.slice(0);
  const view = new DataView(result);
  for (const moof of boxes(view).filter((box) => box.type === "moof")) {
    for (const traf of children(view, moof).filter(
      (box) => box.type === "traf",
    )) {
      const parts = children(view, traf);
      const tfhd = parts.find((box) => box.type === "tfhd");
      if (!tfhd) throw new Error("Missing MP4 tfhd box");
      requireBytes(tfhd, 8);
      const offset = offsets.get(view.getUint32(tfhd.body + 4));
      if (offset !== undefined) {
        const tfdt = parts.find((box) => box.type === "tfdt");
        if (!tfdt) throw new Error("Missing MP4 tfdt box");
        requireBytes(tfdt, 4);
        const version = view.getUint8(tfdt.body);
        if (version > 1) throw new Error("Unsupported MP4 tfdt version");
        requireBytes(tfdt, version ? 12 : 8);
        const position = tfdt.body + 4;
        const time =
          (version
            ? view.getBigUint64(position)
            : BigInt(view.getUint32(position))) + offset;
        if (time < 0n || time > (version ? 0xffffffffffffffffn : 0xffffffffn))
          throw new Error("MP4 decode-time overflow");
        if (version) view.setBigUint64(position, time);
        else view.setUint32(position, Number(time));
      }
    }
  }
  return result;
}
