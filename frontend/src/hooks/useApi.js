import { useCallback, useEffect, useRef, useState } from "react";

// Fetches `fn` on mount, whenever `deps` change and (optionally) every `intervalMs`.
// Polling pauses while the browser tab is hidden to avoid needless load.
export function useApi(fn, deps = [], intervalMs = 0) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);
  const fnRef = useRef(fn);
  fnRef.current = fn;

  const load = useCallback(async () => {
    try {
      const d = await fnRef.current();
      setData(d);
      setError(null);
    } catch (e) {
      setError(e);
    } finally {
      setLoading(false);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  useEffect(() => {
    setLoading(true);
    load();
    if (!intervalMs) return undefined;
    const id = setInterval(() => {
      if (!document.hidden) load();
    }, intervalMs);
    return () => clearInterval(id);
  }, [load, intervalMs]);

  return { data, error, loading, reload: load };
}
