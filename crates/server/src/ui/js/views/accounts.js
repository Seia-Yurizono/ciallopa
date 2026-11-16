"use strict";
// Accounts: upstream accounts, quota, scheduling, sign-in / import. Placeholder until implemented.
Views.accounts = (() => {
  const {h} = Core;
  return {
    title: () => I18n.t("nav.accounts"),
    scope: ["time"],
    mount(root) {
      root.appendChild(h("div", "placeholder",
        h("div", "card card-body", UI.skeleton.lines(2)),
        h("div", "card card-flush", UI.skeleton.rows(6, 6)),
        h("p", "placeholder-note", I18n.t("placeholder.note", {name: I18n.t("nav.accounts")}))));
    },
    unmount() {},
  };
})();
