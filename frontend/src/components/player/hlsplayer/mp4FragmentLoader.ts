import Hls, {
  FragmentLoaderContext,
  LoaderCallbacks,
  LoaderConfiguration,
  LoaderContext,
} from "hls.js";

import { normalizeMp4Fragment, normalizeMp4Init } from "./mp4EditLists";

/** Keep startup-edit metadata with HLS's cached init bytes, across tier moves,
 * playlist refreshes, seeks and run changes. It is released with those bytes. */
export function createMp4FragmentLoader(
  DefaultLoader = Hls.DefaultConfig.loader,
) {
  const offsetsByInit = new WeakMap<object, ReadonlyMap<number, bigint>>();
  return class Mp4FragmentLoader extends DefaultLoader {
    declare context: FragmentLoaderContext | null;

    override load(
      context: LoaderContext,
      config: LoaderConfiguration,
      callbacks: LoaderCallbacks<LoaderContext>,
    ) {
      // A complete moof is needed before editing tfdt. Do not forward partial
      // responses to the transmuxer before normalization has completed.
      super.load(context, config, {
        ...callbacks,
        onProgress: undefined,
        onSuccess: (response, stats, loadedContext, networkDetails) => {
          const { frag } = loadedContext as FragmentLoaderContext;
          let { data } = response;
          try {
            if (data instanceof ArrayBuffer && frag) {
              if (frag.sn === "initSegment") {
                const normalized = normalizeMp4Init(data);
                data = normalized.data;
                offsetsByInit.set(data, normalized.offsets);
              } else if (frag.initSegment?.data) {
                const offsets = offsetsByInit.get(frag.initSegment.data.buffer);
                if (offsets) data = normalizeMp4Fragment(data, offsets);
              }
            }
          } catch (error) {
            callbacks.onError(
              {
                code: 0,
                text: `Invalid MP4 startup timing: ${error instanceof Error ? error.message : String(error)}`,
              },
              loadedContext,
              networkDetails,
              stats,
            );
            return;
          }
          callbacks.onSuccess(
            { ...response, data },
            stats,
            loadedContext,
            networkDetails,
          );
        },
      });
    }
  };
}
