"use strict";
// Overview: health and usage at a glance. Placeholder until the view is implemented; the layout
// below mirrors the planned first screen (hero metrics, trend, two columns).
Views.overview = (() => {
  const {h} = Core;
  return {
    title: () => I18n.t("nav.overview"),
    scope: ["time", "account", "model", "key", "instance"],
    mount(root) {
      root.appendChild(h("div", "placeholder",
        h("div", "card tint card-body", UI.skeleton.stats(6)),
        h("div", "card card-body", UI.skeleton.block(220)),
        h("div", "grid-2",
          h("div", "card card-body", UI.skeleton.lines(5)),
          h("div", "card card-body", UI.skeleton.lines(5))),
        h("p", "placeholder-note", I18n.t("placeholder.note", {name: I18n.t("nav.overview")}))));
    },
    unmount() {},
  };
})();
