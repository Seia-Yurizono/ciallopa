"use strict";
// Settings: configuration, API Keys, prices, change log. Placeholder until implemented.
Views.settings = (() => {
  const {h} = Core;
  return {
    title: () => I18n.t("nav.settings"),
    mount(root) {
      root.appendChild(h("div", "placeholder",
        h("div", "grid-2",
          h("div", "card card-body", UI.skeleton.lines(6)),
          h("div", "card card-body", UI.skeleton.lines(8))),
        h("p", "placeholder-note", I18n.t("placeholder.note", {name: I18n.t("nav.settings")}))));
    },
    unmount() {},
  };
})();
