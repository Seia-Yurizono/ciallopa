"use strict";
// System: memory, buffers, WebSocket, Cloudflare blocks. Placeholder until implemented.
Views.system = (() => {
  const {h} = Core;
  return {
    title: () => I18n.t("nav.system"),
    mount(root) {
      root.appendChild(h("div", "placeholder",
        h("div", "card card-body", UI.skeleton.stats(4)),
        h("div", "card card-body", UI.skeleton.block(160)),
        h("p", "placeholder-note", I18n.t("placeholder.note", {name: I18n.t("nav.system")}))));
    },
    unmount() {},
  };
})();
