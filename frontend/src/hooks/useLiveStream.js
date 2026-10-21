import { useEffect, useRef, useState } from "react";
import { api } from "../services/api.js";

// Server-Sent Events from /api/stream (zone_risk, data_quality, alert, status).
// `onEvent(type, data)` is called for each event. Reports whether the stream is
// connected so the UI can fall back to polling.
export function useLiveStream(onEvent) {
  const [connected, setConnected] = useState(false);
  const [feedStatus, setFeedStatus] = useState(null);
  const [lastEventAt, setLastEventAt] = useState(null);
  const handler = useRef(onEvent);
  handler.current = onEvent;

  useEffect(() => {
    let es;
    let retry;
    const open = () => {
      es = new EventSource(api.streamUrl());
      es.onopen = () => setConnected(true);
      es.onerror = () => {
        setConnected(false);
        es.close();
        retry = setTimeout(open, 10000);
      };
      ["zone_risk", "data_quality", "alert"].forEach((type) =>
        es.addEventListener(type, (e) => {
          setLastEventAt(new Date());
          try {
            handler.current && handler.current(type, JSON.parse(e.data));
          } catch {
            /* ignore malformed event */
          }
        })
      );
      es.addEventListener("status", (e) => {
        try {
          setFeedStatus(JSON.parse(e.data));
        } catch {
          /* ignore */
        }
      });
    };
    open();
    return () => {
      clearTimeout(retry);
      if (es) es.close();
    };
  }, []);

  return { connected, feedStatus, lastEventAt };
}
