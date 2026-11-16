import React from "react";
import { createRoot } from "react-dom/client";
import App from "./App";
import { ArticlePage, Home } from "./home/Home";
import { usePath } from "./router";
import "./styles.css";

/** Public site at / and /learn/<slug>; everything else is the signed-in console. */
function Root() {
  const path = usePath();
  // A single sign-on return lands with #sso_token or #sso_error; the console reads it.
  const ssoReturn = /sso_(token|error)=/.test(window.location.hash);
  if (!ssoReturn && path === "/") return <Home />;
  const learn = path.match(/^\/learn\/([a-z0-9-]+)\/?$/);
  if (!ssoReturn && learn) return <ArticlePage slug={learn[1]} />;
  return <App />;
}

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <Root />
  </React.StrictMode>,
);
