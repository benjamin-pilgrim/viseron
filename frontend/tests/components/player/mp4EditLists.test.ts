import Hls, {
  Fragment,
  FragmentLoaderContext,
  Loader,
  LoaderCallbacks,
  LoaderConfiguration,
  LoaderContext,
} from "hls.js";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  normalizeMp4Fragment,
  normalizeMp4Init,
} from "components/player/hlsplayer/mp4EditLists";
import { createMp4FragmentLoader } from "components/player/hlsplayer/mp4FragmentLoader";
import { createHlsInstance } from "components/player/hlsplayer/utils";

function join(...parts: Uint8Array[]): Uint8Array<ArrayBuffer> {
  const data = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let offset = 0;
  parts.forEach((p) => {
    data.set(p, offset);
    offset += p.length;
  });
  return data;
}
function box(type: string, ...parts: Uint8Array[]): Uint8Array<ArrayBuffer> {
  const data = join(new Uint8Array(8), ...parts);
  new DataView(data.buffer).setUint32(0, data.length);
  data.set(
    [...type].map((c) => c.charCodeAt(0)),
    4,
  );
  return data;
}
function words(...values: number[]): Uint8Array<ArrayBuffer> {
  const data = new Uint8Array(values.length * 4);
  values.forEach((v, i) => new DataView(data.buffer).setUint32(i * 4, v));
  return data;
}
function init(
  videoDelay = 40511,
  audioDelay = 39578,
  version = 0,
): ArrayBuffer {
  function track(id: number, scale: number, delay: number) {
    const edits =
      version === 0
        ? words(0, 2, delay, 0xffffffff, 0x10000, 0, 0, 0x10000)
        : words(
            0x1000000,
            2,
            0,
            delay,
            0xffffffff,
            0xffffffff,
            0x10000,
            0,
            0,
            0,
            0,
            0x10000,
          );
    return box(
      "trak",
      box("tkhd", words(0, 0, 0, id)),
      box("edts", box("elst", edits)),
      box("mdia", box("mdhd", words(0, 0, 0, scale))),
    );
  }
  return box(
    "moov",
    box("mvhd", words(0, 0, 0, 1000)),
    track(1, 90000, videoDelay),
    track(2, 16000, audioDelay),
  ).buffer;
}
function fragment(
  videoTime = 11004622n,
  audioTime = 1972064n,
  version = 1,
): ArrayBuffer {
  function track(id: number, time: bigint) {
    const tfdt = new Uint8Array(version ? 12 : 8);
    const view = new DataView(tfdt.buffer);
    tfdt[0] = version;
    if (version) view.setBigUint64(4, time);
    else view.setUint32(4, Number(time));
    return box("traf", box("tfhd", words(0, id)), box("tfdt", tfdt));
  }
  return join(
    box("moof", track(1, videoTime), track(2, audioTime)),
    box("mdat", words(0x12345678)),
  ).buffer;
}
function times(data: ArrayBuffer): bigint[] {
  const bytes = new Uint8Array(data);
  const view = new DataView(data);
  const result: bigint[] = [];
  for (let i = 4; i < bytes.length - 8; i += 1) {
    if (String.fromCharCode(...bytes.slice(i, i + 4)) === "tfdt")
      result.push(
        bytes[i + 4] ? view.getBigUint64(i + 8) : BigInt(view.getUint32(i + 8)),
      );
  }
  return result;
}
describe("MP4 startup edit lists", () => {
  it.each([0, 1])(
    "preserves effective A/V times with unequal edit delays (elst v%i)",
    (version) => {
      const originalInit = init(40511, 39578, version);
      const original = fragment();
      const snapshot = original.slice(0);
      const normalized = normalizeMp4Init(originalInit);
      expect(normalized.data.byteLength).toBeLessThan(originalInit.byteLength);
      const result = normalizeMp4Fragment(original, normalized.offsets);
      expect(times(result)).toEqual([14650612n, 2605312n]);
      expect(Number(times(result)[0]) / 90000).toBeCloseTo(162.784578, 5);
      expect(Number(times(result)[1]) / 16000).toBeCloseTo(162.832, 5);
      expect(original).toEqual(snapshot);
      expect(new Uint8Array(result).slice(-12)).toEqual(
        new Uint8Array(original).slice(-12),
      );
      expect(normalizeMp4Init(normalized.data).offsets.size).toBe(0);
    },
  );
  it("leaves zero-delay init and media unchanged", () => {
    const source = init(0, 0);
    const normalized = normalizeMp4Init(source);
    expect(normalized.offsets.size).toBe(0);
    expect(normalized.data).toBe(source);
    const media = fragment();
    expect(normalizeMp4Fragment(media, normalized.offsets)).toBe(media);
  });
  it("handles 32-bit decode times and multiple moofs", () => {
    const offsets = normalizeMp4Init(init()).offsets;
    const media = join(
      new Uint8Array(fragment(100n, 200n, 0)),
      new Uint8Array(fragment(300n, 400n)),
    ).buffer;
    expect(times(normalizeMp4Fragment(media, offsets))).toEqual([
      3646090n,
      633448n,
      3646290n,
      633648n,
    ]);
  });
  it("rejects decode-time overflow", () => {
    expect(() =>
      normalizeMp4Fragment(
        fragment(0xffffffffn, 100n, 0),
        normalizeMp4Init(init()).offsets,
      ),
    ).toThrow(/overflow/i);
  });
  it("does not partially rewrite an unsupported edit program", () => {
    const source = init();
    const bytes = new Uint8Array(source);
    const view = new DataView(source);
    for (let i = 4; i < bytes.length - 4; i += 1) {
      if (String.fromCharCode(...bytes.slice(i, i + 4)) === "elst")
        view.setUint32(i + 20, 0x20000);
    }
    const normalized = normalizeMp4Init(source);
    expect(normalized.data).toBe(source);
    expect(normalized.offsets.size).toBe(0);
  });
  it("supports a zero-delay identity edit on the other track", () => {
    const audio = box(
      "trak",
      box("tkhd", words(0, 0, 0, 3)),
      box("edts", box("elst", words(0, 1, 0, 0, 0x10000))),
      box("mdia", box("mdhd", words(0, 0, 0, 16000))),
    );
    const source = new Uint8Array(init());
    const combined = join(source, audio);
    new DataView(combined.buffer).setUint32(0, combined.length);
    expect(normalizeMp4Init(combined.buffer).offsets.size).toBe(2);
  });
  it("rejects truncated boxes", () => {
    expect(() => normalizeMp4Init(init().slice(0, -1))).toThrow(/box/i);
  });
});

describe("HLS fragment loader integration", () => {
  afterEach(() => vi.restoreAllMocks());
  it("normalizes cached init bytes, preserves run association and does not forward partial media", () => {
    const responses = new Map<string, ArrayBuffer>();
    const loaderPrototype: Loader<LoaderContext> =
      Hls.DefaultConfig.loader.prototype;
    const networkLoad = vi
      .spyOn(loaderPrototype, "load")
      .mockImplementation(function loadResponse(
        this: Loader<LoaderContext>,
        context: LoaderContext,
        _config: LoaderConfiguration,
        callbacks: LoaderCallbacks<LoaderContext>,
      ) {
        expect(callbacks.onProgress).toBeUndefined();
        callbacks.onSuccess(
          { url: context.url, data: responses.get(context.url)! },
          this.stats,
          context,
          null,
        );
      });
    const LoaderClass = createMp4FragmentLoader();
    const configuration = {} as LoaderConfiguration;
    function download(frag: Fragment, data: ArrayBuffer) {
      const loader = new LoaderClass(Hls.DefaultConfig);
      const context = {
        url: `https://recordings.invalid/${responses.size}`,
        responseType: "arraybuffer",
        frag,
        part: null,
      } as FragmentLoaderContext;
      responses.set(context.url, data);
      let result: ArrayBuffer | undefined;
      const onError = vi.fn();
      loader.load(context, configuration, {
        onSuccess: (response) => {
          result = response.data as ArrayBuffer;
        },
        onError,
        onTimeout: vi.fn(),
        onProgress: vi.fn(),
      });
      expect(onError).not.toHaveBeenCalled();
      return result!;
    }
    const runA = { sn: "initSegment" } as Fragment;
    runA.data = new Uint8Array(download(runA, init()));
    const runB = { sn: "initSegment" } as Fragment;
    runB.data = new Uint8Array(download(runB, init(1000, 2000)));
    const next = fragment();
    expect(
      times(download({ sn: 3, initSegment: runA } as Fragment, next)),
    ).toEqual([14650612n, 2605312n]);
    expect(
      times(download({ sn: 4, initSegment: runB } as Fragment, next)),
    ).toEqual([11094622n, 2004064n]);
    // Seeking back must reuse A's metadata, without accumulating offsets.
    expect(
      times(download({ sn: 3, initSegment: runA } as Fragment, next)),
    ).toEqual([14650612n, 2605312n]);
    expect(networkLoad).toHaveBeenCalledTimes(5);
  });
  it("installs the loader in the shared live, timeline and recording player factory", () => {
    const hls = createHlsInstance(
      { enabled: false, onboarding_complete: true },
      { current: "test" },
    );
    expect(hls.config.fLoader).toBeDefined();
    expect(hls.config.progressive).toBe(false);
    hls.destroy();
  });
});
