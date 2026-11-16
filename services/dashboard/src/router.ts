import { useEffect, useState } from "react";

// Three kinds of page: the public site (/ and /learn/...), and the console (/console).
// Plain history API; the web server falls back to index.html for unknown paths.

export function go(path: string) {
  if (path === window.location.pathname) { window.scrollTo(0, 0); return; }
  history.pushState(null, "", path);
  window.dispatchEvent(new PopStateEvent("popstate"));
  window.scrollTo(0, 0);
}

export function usePath(): string {
  const [path, setPath] = useState(window.location.pathname);
  useEffect(() => {
    const on = () => setPath(window.location.pathname);
    window.addEventListener("popstate", on);
    return () => window.removeEventListener("popstate", on);
  }, []);
  return path;
}

/** Link that navigates inside the app without a page load. */
export function onNav(path: string) {
  return (e: React.MouseEvent) => {
    if (e.metaKey || e.ctrlKey || e.shiftKey || e.button !== 0) return;
    e.preventDefault();
    go(path);
  };
}
