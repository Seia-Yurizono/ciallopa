"use strict";
// Requests: request log, problem groups, filters and details. Placeholder until implemented.
Views.requests = (() => {
  const {h} = Core;
  return {
    title: () => I18n.t("nav.requests"),
    scope: ["time", "account", "model", "key", "instance"],
    mount(root) {
      root.appendChild(h("div", "placeholder",
        h("div", "card card-body", UI.skeleton.block(80)),
        h("div", "card card-flush", UI.skeleton.rows(10, 6)),
        h("p", "placeholder-note", I18n.t("placeholder.note", {name: I18n.t("nav.requests")}))));
    },
    unmount() {},
  };
})();
