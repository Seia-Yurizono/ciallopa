"use strict";
// Settings view text. View strings are [中文, English] pairs (split into the two dictionaries below,
// so both languages always have the same keys). SCHEMA_EN gives every config key an English title,
// a one-line summary and, where the server help is long, a longer note behind ⓘ. Chinese uses the
// server's desc / help (desc without its unit suffix; long help collapses to its first clause).
const SettingsText = (() => {
  const plural = (one, many) => v => (Number(v.n) === 1 ? one : many).replace("{n}", v.n);
  const PAIRS = {
    "settings.title": ["设置", "Settings"],
    "settings.search": ["搜索全部设置", "Search all settings"],
    "settings.searchLabel": ["搜索全部设置（名称、说明或键名）", "Search all settings (name, description or key)"],
    "settings.onlyModified": ["只看已修改", "Modified only"],
    "settings.onlyModifiedTip": ["列出与默认值不同或未保存的设置项", "List settings that differ from the default or are unsaved"],
    "settings.sections": ["设置分区", "Settings sections"],
    "settings.backToList": ["设置", "Settings"],
    "settings.unsavedDot": ["有未保存的修改", "Has unsaved changes"],
    "settings.editable": ["可编辑", "Editable"],
    "settings.readonly": ["只读", "Read-only"],
    "settings.editableTip": ["配置文件中 [ui] allow_config_write = true", "[ui] allow_config_write = true in the config file"],
    "settings.readonlyTip": ["在配置文件中设置 [ui] allow_config_write = true 才能在此修改（该开关只能在配置文件中修改）",
      "Set [ui] allow_config_write = true in the config file to edit here (that switch is file-only)"],
    "settings.noFile": ["（未使用配置文件）", "(no config file)"],
    "settings.version": ["版本 {v}", "Version {v}"],
    "settings.lastApplied": ["上次应用 {t}", "Last applied {t}"],
    "settings.lastAppliedFailed": ["上次应用 {t}（失败）", "Last applied {t} (failed)"],
    "settings.reload": ["重新加载配置文件", "Reload config file"],
    "settings.reloadShort": ["重新加载", "Reload"],
    "settings.reloadTip": ["重新读取配置文件并应用（文件变化会自动应用，此按钮用于手动触发）",
      "Read the config file again and apply it (file changes apply automatically; this triggers it by hand)"],
    "settings.reloading": ["加载中…", "Reloading…"],
    "settings.reloaded": [v => "已重新加载：" + (v.n ? v.n + " 项改动已生效" : "没有需要应用的改动"),
      v => "Reloaded: " + (v.n ? v.n + (v.n === 1 ? " change applied" : " changes applied") : "nothing to apply")],

    // banners
    "settings.fileErrorTitle": ["配置文件有错误，未应用", "The config file has an error and was not applied"],
    "settings.fileErrorBody": ["仍在使用上一次的配置：{e}", "Still running the previous configuration: {e}"],
    "settings.readonlyTitle": ["配置只读", "Configuration is read-only"],
    "settings.readonlyBody": ["此页面不能保存配置。文件的修改仍会自动应用，也可重新加载；账号页的刷新额度、解除冷却等运行操作仍可用。",
      "This page cannot save the configuration. File edits still apply automatically and can be reloaded; runtime actions " +
      "on the Accounts page (refresh quota, clear cooldown) still work."],
    "settings.pendingTitle": [v => v.n + " 项需重启后生效", plural("{n} setting applies after a restart", "{n} settings apply after a restart")],
    "settings.pendingItem": ["运行中 {old} → 文件 {new}", "running {old} → file {new}"],
    "settings.draftKeptTitle": ["配置已在别处更新，未保存的修改已保留", "The configuration changed elsewhere; your unsaved changes are kept"],
    "settings.draftKeptBody": ["检查并保存时会显示最新值与草稿的差异；放弃后载入最新配置。",
      "Check & save shows the latest values against your draft; discard loads the latest configuration."],
    "settings.conflictTitle": ["配置已被修改", "The configuration was changed"],
    "settings.conflictDraft": ["未保存的修改已保留。重新检查可以查看最新配置与草稿的差异后再确认；也可放弃草稿并载入最新。",
      "Your unsaved changes are kept. Check again to compare the latest configuration with your draft, or discard it and load the latest."],
    "settings.conflictClean": ["请刷新后重试。", "Refresh and try again."],
    "settings.recheck": ["重新检查", "Check again"],
    "settings.savedStaleTitle": ["已保存，但重新读取最新配置失败", "Saved, but reading the latest configuration failed"],
    "settings.savedStaleBody": ["{why}。草稿已保留，请刷新后重新检查。", "{why}. Your draft is kept; refresh and check again."],
    "settings.opFailed": ["操作失败", "Action failed"],
    "settings.dismiss": ["关闭提示", "Dismiss"],

    // rows
    "settings.restart": ["需重启", "Restart"],
    "settings.restartTip": ["写入文件后，重启 ciallopa 才生效", "Written to the file; applies after ciallopa restarts"],
    "settings.irreversible": ["不可逆", "Irreversible"],
    "settings.irreversibleTip": ["重置卡使用后无法撤销", "A used reset card cannot be restored"],
    "settings.modified": ["已修改", "Modified"],
    "settings.modifiedTip": ["与默认值不同", "Differs from the default"],
    "settings.unsavedTag": ["未保存", "Unsaved"],
    "settings.default": ["默认 {v}", "Default {v}"],
    "settings.restore": ["恢复", "Reset"],
    "settings.restoreTip": ["改回默认值（保存后生效）", "Back to the default (applies when saved)"],
    "settings.on": ["开启", "On"],
    "settings.off": ["关闭", "Off"],
    "settings.isOn": ["已开启", "On"],
    "settings.isOff": ["已关闭", "Off"],
    "settings.empty": ["（空）", "(empty)"],
    "settings.builtin": ["（默认）", "(default)"],
    "settings.fileOnly": ["只能在配置文件中修改", "Can only be changed in the config file"],
    "settings.fileOnlyShort": ["仅配置文件", "File only"],
    "settings.running": ["运行中：{v}", "Running: {v}"],
    "settings.rustLog": ["已被环境变量 RUST_LOG 覆盖", "Overridden by the RUST_LOG environment variable"],
    "settings.depOff": ["开启「{p}」后生效，当前值保留", "Applies when “{p}” is on; the value is kept"],
    "settings.depOverload": ["overloaded 冷却已关闭，下面的值保留", "overloaded cooldown is off; the values below are kept"],
    "settings.advanced": ["高级", "Advanced"],
    "settings.advCount": [v => v.n + " 项", plural("{n} setting", "{n} settings")],
    "settings.advUnsaved": [v => v.n + " 项未保存", v => v.n + " unsaved"],
    "settings.advMod": [v => v.n + " 项不是默认值", v => v.n + " not default"],
    "settings.advAllDefault": ["都是默认值", "all default"],
    "settings.restartLegend": ["标为「需重启」的项写入文件后，重启 ciallopa 才生效；其余保存后立即生效",
      "Settings marked Restart are written to the file and apply after a restart; the rest apply when saved"],
    "settings.immediate": ["开关立即生效，无需保存", "Switches apply at once, no save needed"],
    "settings.docs": ["了解账号选择规则", "How an account is chosen"],
    "settings.more": ["展开说明", "Show details"],

    // number / range validation
    "settings.errInt": ["需要大于或等于 0 的整数", "Needs a whole number ≥ 0"],
    "settings.errNum": ["需要大于或等于 0 的数字", "Needs a number ≥ 0"],
    "settings.errPosInt": ["需要大于 0 的整数；关闭请使用左侧开关", "Needs a whole number > 0; use the switch to turn it off"],
    "settings.errPosNum": ["需要大于 0 的数字；关闭请使用左侧开关", "Needs a number > 0; use the switch to turn it off"],
    "settings.errRange": ["最短和最长均需为 1–60 的整数；关闭请使用左侧开关",
      "Shortest and longest need whole numbers 1–60; use the switch to turn it off"],
    "settings.errMinMax": ["最短不能大于最长", "Shortest cannot exceed longest"],
    "settings.errBetween": ["需要在 {a} 到 {b} 之间", "Must be between {a} and {b}"],
    "settings.rangeMin": ["最短", "shortest"],
    "settings.rangeMax": ["最长", "longest"],
    "settings.every": ["每 {a} 分钟", "every {a} min"],
    "settings.everyRange": ["每 {a}–{b} 分钟", "every {a}–{b} min"],

    // zero = off labels
    "settings.zo.off": ["关闭", "Off"],
    "settings.zo.noWait": ["不等待", "No wait"],
    "settings.zo.unlimited": ["不限", "Unlimited"],
    "settings.zo.noCool": ["不冷却", "No cooldown"],
    "settings.zo.noRetry": ["不重试", "No retry"],
    "settings.zo.switchOnly": ["只换账号不冷却", "Switch only"],
    "settings.zo.forever": ["永久保留", "Keep forever"],
    "settings.zo.manual": ["只手动刷新", "Manual only"],

    // units
    "settings.unit.s": ["秒", "s"],
    "settings.unit.min": ["分钟", "min"],
    "settings.unit.ms": ["毫秒", "ms"],
    "settings.unit.MiB": ["MiB", "MiB"],
    "settings.unit.times": ["次", "times"],
    "settings.unit.pct": ["%", "%"],
    "settings.unit.days": ["天", "days"],
    "settings.unit.count": ["个", ""],

    // groups, sections, subsections
    "settings.g.forward": ["转发", "Forwarding"],
    "settings.g.data": ["数据与账号", "Data & accounts"],
    "settings.g.system": ["系统", "System"],
    "settings.p.routing": ["账号调度", "Routing"],
    "settings.p.routing.sub": ["新会话如何选择账号，以及同一会话何时固定使用同一账号",
      "How new sessions pick an account and when a session sticks to one"],
    "settings.p.network": ["网络", "Network"],
    "settings.p.network.sub": ["出口代理、IPv6 和到上游的连接", "Egress proxy, IPv6 and upstream connections"],
    "settings.p.forward": ["限流与容错", "Limits & retries"],
    "settings.p.forward.sub": ["资源上限、超时和重试，以及上游出错时的账号冷却", "Resource limits, timeouts, retries and cooldowns on upstream errors"],
    "settings.p.images": ["图片接口", "Images API"],
    "settings.p.images.sub": ["/v1/images 接口和它使用的模型", "The /v1/images routes and the model they use"],
    "settings.p.usage": ["用量", "Usage"],
    "settings.p.usage.sub": ["请求明细的记录与保留方式，以及额度估算", "How request details are recorded and kept, and quota estimates"],
    "settings.p.prices": ["模型价格", "Prices"],
    "settings.p.prices.sub": ["按 API 标价估算费用（不是实际扣费）；只在点「检查更新」时联网",
      "Estimated cost from API list prices (not billing); goes online only on Check for updates"],
    "settings.p.accounts": ["账号与重置卡", "Accounts & reset cards"],
    "settings.p.accounts.sub": ["对所有账号生效的策略", "Policies for every account"],
    "settings.p.general": ["常规", "General"],
    "settings.p.general.sub": ["实例、日志和配置文件", "Instance, logging and the config file"],
    "settings.p.keys": ["API Key", "API Keys"],
    "settings.p.keys.sub": ["只显示标签和末 4 位；新 Key 只显示一次", "Only the label and last 4 characters are shown; a new key is shown once"],
    "settings.p.audit": ["变更记录", "Change log"],
    "settings.p.audit.sub": ["最近 20 条，包括账号页和配置文件的改动", "Latest 20 entries, including Accounts page and file changes"],
    "settings.s.select": ["账号选择", "Account selection"],
    "settings.s.affinity": ["会话亲和", "Session affinity"],
    "settings.s.proxy": ["出口代理", "Egress proxy"],
    "settings.s.conn": ["连接", "Connection"],
    "settings.s.limits": ["资源上限", "Resource limits"],
    "settings.s.retry": ["超时与重试", "Timeouts & retries"],
    "settings.s.shutdown": ["停止与重启", "Shutdown"],
    "settings.s.cool": ["出错时冷却", "Cooldown on errors"],
    "settings.s.coolNote": ["冷却中的账号不接新请求；最后一个可用账号不冷却", "A cooling account takes no new requests; the last available one never cools"],
    "settings.s.usageLog": ["用量日志", "Usage log"],
    "settings.s.priceRow": ["模型价格", "Prices"],
    "settings.s.priceSource": ["价格来源", "Price source"],
    "settings.s.accounts": ["账号", "Accounts"],
    "settings.s.refresh": ["后台刷新", "Background refresh"],
    "settings.s.reset": ["重置卡", "Reset cards"],
    "settings.s.runtime": ["运行信息", "Runtime"],
    "settings.s.runtimeNote": ["只能在配置文件中修改", "File only"],
    "settings.s.tz": ["时区与内部请求", "Time zone & internal requests"],

    // cooldown table
    "settings.cool.5xx": ["5xx 服务器错误", "5xx server error"],
    "settings.cool.overload": ["overloaded 过载", "overloaded"],
    "settings.cool.402d": ["402 工作区停用", "402 workspace deactivated"],
    "settings.cool.402": ["其他 402", "Other 402"],
    "settings.cool.404": ["404 模型", "404 model"],
    "settings.cool.t3": ["同一账号连续 3 次", "3 in a row on one account"],
    "settings.cool.t1": ["1 次", "Once"],
    "settings.cool.streak": ["连续出错次数", "errors in a row"],
    "settings.cool.start": ["起始", "start"],
    "settings.cool.max": ["每次翻倍，最多", "doubles up to"],
    "settings.cool.trigger": ["触发：{t}", "Trigger: {t}"],

    // info rows
    "settings.info.prices": ["模型价格", "Model prices"],
    "settings.info.pricesHelp": ["按 API 标价估算费用；可以检查更新、给未定价的模型设置价格或忽略",
      "Estimated cost from API list prices; check for updates, price or ignore unpriced models"],
    "settings.info.pricesGo": ["管理价格", "Manage prices"],
    "settings.info.usageOff": ["用量记录已关闭，不计算费用", "Usage logging is off; no cost is computed"],
    "settings.info.models": [v => v.n + " 个模型", plural("{n} model", "{n} models")],
    "settings.info.noTable": ["尚无价格表", "No price table yet"],
    "settings.info.unpriced": [v => v.n + " 个未定价", v => v.n + " unpriced"],
    "settings.info.acct": ["单个账号的设置", "Per-account settings"],
    "settings.info.acctHelp": ["启用、权重、优先级、单独代理、刷新额度和使用重置卡都在账号页，各账号分别设置",
      "Enable, weight, priority, own proxy, quota refresh and reset cards are set per account on the Accounts page"],
    "settings.info.acctN": [v => v.n + " 个账号", plural("{n} account", "{n} accounts")],
    "settings.info.acctGo": ["前往账号页", "Open Accounts"],
    "settings.info.file": ["配置文件", "Config file"],
    "settings.info.fileRw": ["可在此修改，保存时写回该文件", "Editable here; saving writes back to this file"],
    "settings.info.fileRo": ["只读：在配置文件中设置 [ui] allow_config_write = true 才能在此修改",
      "Read-only: set [ui] allow_config_write = true in the file to edit here"],
    "settings.info.tz": ["时区", "Time zone"],
    "settings.info.tzHelp": ["页面时间按本浏览器的时区显示；图表按范围结束时的 UTC 偏移分桶",
      "Times show in this browser's time zone; charts bucket by the UTC offset at the end of the range"],
    "settings.info.fp": ["内部请求 UA", "Internal request UA"],
    "settings.info.fpHelp": ["ciallopa 自己发往上游的请求（刷新令牌、额度、重置卡）使用的指纹，从客户端请求中学习",
      "Fingerprint of ciallopa's own upstream requests (token refresh, quota, reset cards), learned from client requests"],
    "settings.info.fpLearned": ["学习于 {t}（{ago}）", "learned {t} ({ago})"],
    "settings.info.fpKey": ["Key {k}", "key {k}"],
    "settings.info.fpDefault": ["内置默认值", "built-in default"],

    // egress / proxy
    "settings.egress": ["当前出口：{v}", "Current egress: {v}"],
    "settings.direct": ["直连", "direct"],
    "settings.viaProxy": ["代理 {p}", "proxy {p}"],
    "settings.proxyNoUrl": ["已启用，但尚未填写出口代理地址：当前直连", "On, but no proxy URL is set: connecting directly"],
    "settings.proxyNote": ["socks5:// 按 socks5h:// 处理，域名由代理解析。https:// 代理只用于 HTTP 请求，WebSocket 不支持，请用 http:// 或 socks5h://。",
      "socks5:// is treated as socks5h:// (the proxy resolves names). An https:// proxy only carries HTTP, not WebSocket; " +
      "use http:// or socks5h://."],
    "settings.ipv6Note": ["使用代理时只影响连接代理主机本身；目标域名由代理解析。",
      "With a proxy this only affects reaching the proxy host; the proxy resolves target names."],
    "settings.ptest": ["测试连接", "Test connection"],
    "settings.ptesting": ["测试中…", "Testing…"],
    "settings.ptestDraft": ["测试未保存的地址", "Tests the unsaved address"],
    "settings.ptestOk": ["{via} · 出口 IP {ip}{loc}{colo} · {ms}", "{via} · egress IP {ip}{loc}{colo} · {ms}"],
    "settings.ptestFail": ["{via} 失败{status}：{err}", "{via} failed{status}: {err}"],
    "settings.ptestVia": ["经代理", "Via proxy"],
    "settings.ptestDirect": ["直连", "Direct"],
    "settings.imm.proxy_enabled": ["启用出口代理", "Use egress proxy"],
    "settings.imm.proxy_enabled.on": ["已启用出口代理：新请求立即走代理，进行中的 WebSocket 会话在本轮结束后重连",
      "Egress proxy on: new requests use it now; open WebSocket sessions reconnect after their current turn"],
    "settings.imm.proxy_enabled.off": ["已关闭出口代理：新请求立即直连，进行中的 WebSocket 会话在本轮结束后重连",
      "Egress proxy off: new requests connect directly; open WebSocket sessions reconnect after their current turn"],
    "settings.imm.ipv6": ["允许 IPv6 出口", "Allow IPv6 egress"],
    "settings.imm.ipv6.on": ["已允许 IPv6：新连接 IPv6/IPv4 竞速，进行中的 WebSocket 会话在本轮结束后重连",
      "IPv6 allowed: new connections race IPv6/IPv4; open WebSocket sessions reconnect after their current turn"],
    "settings.imm.ipv6.off": ["已改为只用 IPv4：新连接只走 IPv4，进行中的 WebSocket 会话在本轮结束后重连",
      "IPv4 only: new connections use IPv4; open WebSocket sessions reconnect after their current turn"],
    "settings.imm.blocked": ["请先保存或放弃其他未保存的修改", "Save or discard your other unsaved changes first"],
    "settings.imm.undone": ["已改回：{name} {state}", "Reverted: {name} {state}"],
    "settings.imm.tag": ["立即生效", "Applies at once"],

    // save flow
    "settings.unsavedN": [v => v.n + " 项未保存", plural("{n} unsaved change", "{n} unsaved changes")],
    "settings.invalidN": [v => v.n + " 项有误", v => v.n + " invalid"],
    "settings.discard": ["放弃", "Discard"],
    "settings.checkSave": ["检查并保存", "Check & save"],
    "settings.checking": ["检查中…", "Checking…"],
    "settings.editsChanged": ["编辑已变化，请重新检查后保存", "Your edits changed; check again before saving"],
    "settings.editsOrLogin": ["编辑或登录已变化，请重新检查后保存", "Edits or login changed; check again before saving"],
    "settings.errTitle": ["检查未通过", "Check failed"],
    "settings.errLead": ["有错误，无法保存：", "Fix these before saving:"],
    "settings.gotIt": ["知道了", "OK"],
    "settings.saveTitle": [v => "保存 " + v.n + " 项改动？", plural("Save {n} change?", "Save {n} changes?")],
    "settings.saveNone": ["没有与当前配置不同的值", "Nothing differs from the current configuration"],
    "settings.col.key": ["设置项", "Setting"],
    "settings.col.old": ["原值", "Current"],
    "settings.col.new": ["新值", "New"],
    "settings.col.effect": ["生效方式", "Applies"],
    "settings.hot": ["立即", "Now"],
    "settings.hotTip": ["保存或修改文件后无需重启；具体生效时机因设置而异", "No restart needed; exactly when depends on the setting"],
    "settings.saveRestartNote": ["标为「需重启」的项会写入文件，重启 ciallopa 后才生效。",
      "Settings marked Restart are written to the file and apply after a restart."],
    "settings.saveNewer": ["配置已更新。「原值」来自最新配置，请确认要用草稿覆盖这些值。",
      "The configuration changed. Current values come from the latest file; confirm you want your draft to replace them."],
    "settings.saveGo": ["写入配置文件并应用", "Write config & apply"],
    "settings.saving": ["保存中…", "Saving…"],
    "settings.saved": [v => "已保存：" + v.a + " 项已应用" + (v.p ? "，" + v.p + " 项需重启" : "") + (v.k ? "；后续编辑已保留" : ""),
      v => "Saved: " + v.a + " applied" + (v.p ? ", " + v.p + " after restart" : "") + (v.k ? "; later edits kept" : "")],
    "settings.discardTitle": [v => "放弃 " + v.n + " 项未保存的修改？", plural("Discard {n} unsaved change?", "Discard {n} unsaved changes?")],
    "settings.discardBody": ["草稿将丢弃并载入最新配置，已保存的配置不会改变。",
      "The draft is dropped and the latest configuration loaded. Saved settings do not change."],
    "settings.discardGo": ["放弃并载入最新", "Discard & load latest"],
    "settings.discardStale": ["编辑或登录已变化，请重新确认放弃", "Edits or login changed; confirm discarding again"],
    "settings.discarded": ["已放弃未保存的修改，已载入最新配置", "Unsaved changes discarded; latest configuration loaded"],
    "settings.discardKept": ["载入期间有新的编辑，草稿已保留", "New edits arrived while loading; the draft is kept"],
    "settings.leaveTitle": ["离开设置？", "Leave settings?"],
    "settings.leaveBody": [v => v.n + " 项未保存的修改会丢失。", v => v.n + (v.n === 1 ? " unsaved change" : " unsaved changes") + " will be lost."],
    "settings.leaveGo": ["丢弃并离开", "Discard & leave"],
    "settings.stay": ["留在此页", "Stay"],

    // search
    "settings.searchTitle": ["搜索「{q}」 · {n} 项", "“{q}” · {n} results"],
    "settings.modTitle": ["已修改的设置 · {n} 项", "Modified settings · {n}"],
    "settings.noMatch": ["没有匹配的设置项", "No matching settings"],
    "settings.noMatchBody": ["尝试其他关键词，或清除搜索", "Try other words, or clear the search"],
    "settings.noMod": ["所有设置都是默认值", "Every setting is at its default"],
    "settings.clearSearch": ["清除搜索", "Clear search"],
    "settings.pageHits": ["分区", "Sections"],

    // prices
    "settings.pr.check": ["检查更新", "Check for updates"],
    "settings.pr.checking": ["正在下载…", "Downloading…"],
    "settings.pr.checkTip": ["下载最新价格并列出变化，确认后才写入", "Download the latest prices and list changes; nothing is written until you confirm"],
    "settings.pr.noSourceTip": ["usage.prices_source 为空：先在下方「价格来源」设置", "usage.prices_source is empty: set it under Price source below"],
    "settings.pr.updated": ["更新于 {t}", "updated {t}"],
    "settings.pr.source": ["来源", "source"],
    "settings.pr.noSource": ["未设置来源（usage.prices_source）", "no source set (usage.prices_source)"],
    "settings.pr.file": ["价格文件", "Price file"],
    "settings.pr.fileNote": ["更新只改动有变化的模型，带日期的促销价、自定价格和已忽略的模型不会被改。",
      "Updates only touch changed models; dated promo prices, custom prices and ignored models are left alone."],
    "settings.pr.emptyNote": ["尚无价格表，费用均显示为「未定价」。", "No price table yet; every cost shows as unpriced."],
    "settings.pr.unpricedNote": [v => "有 " + v.n + " 个模型未定价（最近 30 天用过），可以设置价格或忽略。",
      v => v.n + (v.n === 1 ? " model is" : " models are") + " unpriced (used in the last 30 days); price or ignore them."],
    "settings.pr.models": ["模型", "Models"],
    "settings.pr.all": ["全部", "All"],
    "settings.pr.un": ["未定价", "Unpriced"],
    "settings.pr.cu": ["自定", "Custom"],
    "settings.pr.ig": ["已忽略", "Ignored"],
    "settings.pr.usedRecently": ["最近 30 天用过", "used in the last 30 days"],
    "settings.pr.customLine": ["输入 {i} · 缓存 {c} · 输出 {o}", "input {i} · cached {c} · output {o}"],
    "settings.pr.sameAsInput": ["同输入", "= input"],
    "settings.pr.ignoredLine": ["费用按 0 计，不提示", "costed at 0, no warning"],
    "settings.pr.set": ["设置价格", "Set price"],
    "settings.pr.edit": ["修改", "Edit"],
    "settings.pr.unset": ["删除", "Remove"],
    "settings.pr.ignore": ["忽略", "Ignore"],
    "settings.pr.unignore": ["取消忽略", "Unignore"],
    "settings.pr.working": ["处理中…", "Working…"],
    "settings.pr.legend": ["「忽略」：不再提示未定价，费用按 0 计，适用于 auto 等免费或本地别名模型；「设置价格」：按填写的价格估算，检查更新不会覆盖。改动立即生效，历史记录也按新设置计算。",
      "Ignore: stop warning and cost at 0 (for free or local aliases such as auto). Set price: estimate with your price; " +
      "updates never overwrite it. Changes apply at once, history included."],
    "settings.pr.ignored": ["已忽略 {m}，费用按 0 计", "Ignored {m}; costed at 0"],
    "settings.pr.reIgnored": ["已重新忽略 {m}", "Ignored {m} again"],
    "settings.pr.unignored": ["已取消忽略 {m}", "Stopped ignoring {m}"],
    "settings.pr.unsetDone": ["已删除 {m} 的自定价格", "Removed the custom price of {m}"],
    "settings.pr.restored": ["已恢复 {m} 的自定价格", "Restored the custom price of {m}"],
    "settings.pr.dlgSet": ["设置 {m} 的价格", "Price {m}"],
    "settings.pr.dlgEdit": ["修改 {m} 的价格", "Edit the price of {m}"],
    "settings.pr.dlgUnit": ["按 API 标价填写，单位为美元 / 百万 Token", "API list price in USD per million Tokens"],
    "settings.pr.in": ["输入", "Input"],
    "settings.pr.cache": ["缓存输入", "Cached input"],
    "settings.pr.out": ["输出", "Output"],
    "settings.pr.cacheHint": ["空 = 同输入", "empty = same as input"],
    "settings.pr.dlgNote": ["保存后立即用于所有费用统计（含历史记录）；检查更新不会覆盖", "Used for every cost at once, history included; updates never overwrite it"],
    "settings.pr.dlgErr": ["输入和输出必填，价格是 0 到 10000 之间的数字", "Input and output are required; prices are numbers from 0 to 10000"],
    "settings.pr.save": ["保存价格", "Save price"],
    "settings.pr.setDone": ["已设置 {m} 的价格", "Priced {m}"],
    "settings.pr.upToDate": ["价格已是最新（来源里有 {n} 个模型）", "Prices are up to date ({n} models in the source)"],
    "settings.pr.changesTitle": ["新增 {a} 个、变化 {u} 个模型的价格", "{a} new, {u} changed model prices"],
    "settings.pr.colModel": ["模型", "Model"],
    "settings.pr.colOld": ["原价", "Old"],
    "settings.pr.colNew": ["新价（美元 / 百万 Token）", "New (USD / M Tokens)"],
    "settings.pr.added": ["（新增）", "(new)"],
    "settings.pr.skipped": ["有促销价、自定价格或已忽略的模型不改动：", "Promo, custom or ignored models stay as they are: "],
    "settings.pr.backupNote": ["写入 {p} 前会把原文件备份为 .bak；新价格立即用于所有费用统计（含历史记录）。",
      "{p} is backed up as .bak before writing; new prices apply to every cost at once, history included."],
    "settings.pr.apply": ["应用", "Apply"],
    "settings.pr.applied": [v => "已更新 " + v.n + " 项价格" + (v.b ? "，原文件已备份为 " + v.b : ""),
      v => "Updated " + v.n + (v.n === 1 ? " price" : " prices") + (v.b ? "; backup at " + v.b : "")],

    // keys
    "settings.k.new": ["新建 API Key", "New API Key"],
    "settings.k.label": ["标签", "Label"],
    "settings.k.labelPh": ["如 phone", "e.g. phone"],
    "settings.k.labelHint": ["新 Key 由服务端生成，可转发请求和查看用量，没有管理权限。需要管理权限时，在配置文件对应的 [[api_keys]] 条目中设置 admin = true。",
      "The server generates the key. It can forward requests and read usage, without admin rights; set admin = true on its " +
      "[[api_keys]] entry in the config file for that."],
    "settings.k.create": ["生成", "Generate"],
    "settings.k.creating": ["生成中…", "Generating…"],
    "settings.k.createdTitle": ["新的 API Key", "New API Key"],
    "settings.k.once": ["只显示这一次，之后无法再次查看。请立即复制保存。", "Shown only this once and never again. Copy it now."],
    "settings.k.copied": ["已复制新 Key", "New key copied"],
    "settings.k.copyFail": ["复制失败，请手动选择复制", "Copy failed; select and copy it by hand"],
    "settings.k.saved": ["我已保存", "I saved it"],
    "settings.k.colLabel": ["标签", "Label"],
    "settings.k.colKey": ["Key", "Key"],
    "settings.k.colRole": ["权限", "Access"],
    "settings.k.admin": ["管理", "Admin"],
    "settings.k.adminTip": ["admin = true：可使用管理接口", "admin = true: can use the management API"],
    "settings.k.client": ["转发", "Client"],
    "settings.k.clientTip": ["可转发请求和查看用量", "Can forward requests and read usage"],
    "settings.k.none": ["没有 API Key：所有转发请求都会被拒绝", "No API Keys: every forwarded request is refused"],
    "settings.k.del": ["删除", "Delete"],
    "settings.k.delLabel": ["删除 Key {l}", "Delete key {l}"],
    "settings.k.delTitle": ["删除 Key {l}？", "Delete key {l}?"],
    "settings.k.delBody": ["使用它的客户端会立即无法访问，此操作会写回配置文件。要恢复只能生成新 Key 并更新客户端。",
      "Clients using it lose access at once; this writes back to the config file. The only way back is a new key and updated clients."],
    "settings.k.deleted": ["已删除 Key {l}", "Deleted key {l}"],
    "settings.k.count": [v => v.n + " 个 Key", plural("{n} key", "{n} keys")],
    "settings.k.adminNote": ["页面生成的 Key 没有管理权限", "Keys made here have no admin rights"],

    // audit
    "settings.a.time": ["时间", "Time"],
    "settings.a.actor": ["操作者", "Actor"],
    "settings.a.action": ["操作", "Action"],
    "settings.a.changes": ["内容", "Changes"],
    "settings.a.fileSignal": ["文件 / 信号", "file / signal"],
    "settings.a.none": ["暂无变更记录", "No changes recorded yet"],
    "settings.a.last": ["最近 {t}", "latest {t}"],

    // phone list summaries
    "settings.sum.affinity": ["会话亲和{v}", "session affinity {v}"],
    "settings.sum.onShort": ["开", "on"],
    "settings.sum.offShort": ["关", "off"],
    "settings.sum.proxy": ["出口代理", "proxy"],
    "settings.sum.ipv6": ["IPv6 {v}", "IPv6 {v}"],
    "settings.sum.inflight": ["并发上限 {v}", "concurrency {v}"],
    "settings.sum.attempts": ["换账号重试 {v} 次", "{v} account retries"],
    "settings.sum.keep": ["保留 {v}", "keep {v}"],
    "settings.sum.days": ["{n} 天", "{n} days"],
    "settings.sum.forever": ["永久", "forever"],
    "settings.sum.resetRefresh": ["重置卡{v}", "reset cards {v}"],
    "settings.sum.quotaRefresh": ["额度{v}", "quota {v}"],
    "settings.sum.unnamed": ["未命名实例", "unnamed instance"],
    "settings.sum.log": ["日志 {v}", "log {v}"],
  };

  const zhDict = {}, enDict = {};
  for (const k in PAIRS) {
    zhDict[k] = PAIRS[k][0];
    enDict[k] = PAIRS[k][1];
  }
  I18n.add("zh", zhDict);
  I18n.add("en", enDict);

  // English per config key: [title, summary, longer note?]. Units are shown next to the control.
  const SCHEMA_EN = {
    "listen": ["Listen address", "Address and port ciallopa accepts client requests on"],
    "api_keys": ["Client API Keys", "Keys can only be generated or deleted here"],
    "instance_id": ["Instance name", "Written to every usage record; tells instances apart when they share one usage database"],
    "upstream": ["Upstream URL", "ChatGPT backend address; usually left as is"],
    "auth_dir": ["Account files directory", "One JSON file per account; rescanned when the directory changes"],
    "proxy_url": ["Egress proxy URL", "http://, https://, socks5:// or socks5h://; empty = direct",
      "socks5 also lets the proxy resolve names."],
    "proxy_enabled": ["Use egress proxy", "Off keeps the proxy URL but connects directly"],
    "ipv6": ["Allow IPv6 egress", "On races IPv6 against IPv4; off uses IPv4 only",
      "Applies to direct connections and to reaching the proxy host."],
    "max_body_mb": ["Request body limit", "1–4096 MiB; larger requests are refused. Applies to the next request"],
    "max_buffered_body_mb": ["Buffered body budget", "Memory all in-flight HTTP bodies and WebSocket messages may hold; 0 = unlimited",
      "HTTP requests use about twice this in process memory. Without room a request waits up to 1 s, then HTTP gets 503 and " +
      "WebSocket gets a 503 error frame (the connection stays open). When set, it cannot be below the request body limit."],
    "max_inflight_requests": ["Concurrency limit", "HTTP requests plus running WebSocket turns forwarded at once; 0 = unlimited",
      "An HTTP request holds a slot until its response ends; a WebSocket turn from the client message to its final event " +
      "(or cancel / close). Idle connections hold none. At the limit new work gets 503 at once, with no queue. Lowering it " +
      "never interrupts existing work."],
    "ws_compression": ["Client WebSocket compression",
      "Use permessage-deflate when the client asks (Codex does by default); new connections"],
    "ws_max_connections": ["WebSocket connection limit", "Client WebSockets open at once, idle ones included; 0 = unlimited",
      "Before a message counts against the buffer budget a connection may briefly hold several times the message limit, so " +
      "this and the message limit bound WebSocket memory; the default 24 fits a 2 GB host. Over the limit the handshake gets " +
      "503 and Codex falls back to HTTP. Lowering it never closes open connections."],
    "ws_max_message_mb": ["WebSocket message limit", "Largest client message after decompression, 1–4096 MiB (default 16)",
      "A larger message closes the connection with 1009 and Codex retries over HTTP, where the request body limit applies. " +
      "The limit is advertised in x-codex-websocket-max-message-bytes (or the upstream's smaller value); Codex 0.170+ then " +
      "sends larger turns over HTTP. The upstream accepts at most 16 MiB, so values above 16 gain nothing. New connections."],
    "shutdown_drain_secs": ["Shutdown drain", "How long running replies may continue after a stop signal, 0–60 s (default 5)",
      "Idle connections close at once. A reply still running at the deadline gets what upstream already sent, then a " +
      "retryable response.failed; Codex resends the turn on the new process after about 2 s without repeating finished tool " +
      "calls. When raising it, raise Docker stop_grace_period or systemd TimeoutStopSec too (at least this + 15 s)."],
    "routing.strategy": ["Account selection strategy", "How new sessions and requests without a session pick an account",
      "Disabled and cooling accounts are skipped, then the strategy picks within the highest priority group. A bound session " +
      "keeps its account while that account is available."],
    "routing.session_affinity": ["Session affinity", "Keep a session's later requests on the same account (per session and model)",
      "Off: every request picks again. Switching accounts mid-conversation may make the upstream reject encrypted reasoning " +
      "in the history."],
    "routing.session_affinity_ttl": ["Affinity duration", "A binding ends after this long without requests; e.g. 90s, 30m, 24h"],
    "routing.session_affinity_subagents": ["Subagents use the parent's account",
      "Codex subagents use their parent session's account; off: they pick their own"],
    "routing.session_affinity_rebind_on": ["When to rebind", "Whether the session binding moves while the bound account is unavailable",
      "A request always uses another account while the bound one is unavailable; this decides whether the binding moves too."],
    "routing.quota_avoid_threshold": ["Quota avoidance threshold",
      "New sessions skip accounts whose last seen usage reached this ratio (0–1); 0 = off",
      "Any window that has not reset counts. If every account is over it, nothing is avoided; bound sessions are not affected. " +
      "Regardless of this, an account with a full window is used only when no other has quota, and bound sessions move off " +
      "it, because the upstream keeps charging purchased credits after the window is full."],
    "routing.cooldown_wait_secs": ["Max wait when all cool down",
      "If every account cools and one recovers within this time, requests wait; max 120, 0 = fail at once",
      "WebSocket handshakes wait at most 10 s."],
    "upstream_opts.stream_idle_timeout_secs": ["Stream idle timeout", "Drop an upstream stream after this long without data (idle_timeout)",
      "Codex's own stream idle timeout defaults to 300 s; set higher here and Codex disconnects first (client_closed)."],
    "upstream_opts.response_header_timeout_secs": ["Response header timeout", "Max time per HTTP attempt until response headers, 1–86400 s",
      "Counted from sending the request (upload included). A timeout is a network error: switch account, no cooldown. HTTP " +
      "only; streaming afterwards is not limited."],
    "upstream_opts.preparation_timeout_secs": ["Preparation budget",
      "Total time one request may spend on retries and account switches, 1–86400 s",
      "Token fetches, header waits, error reads, cooldown and retry waits count; normal generation does not. When used up the " +
      "request gets 504. A WebSocket handshake and each turn have their own budget."],
    "upstream_opts.max_attempts": ["Account switch retries", "Account switches before the first byte on network errors, 408 or 5xx",
      "Never retried on the same account. Switches caused by 401/402/403/404/429 do not count. " +
      "WebSocket handshakes have a fixed 4 retries."],
    "upstream_opts.transient_cooldown_secs": ["5xx cooldown",
      "Cool an account after 3 consecutive 5xx (upstream Retry-After first, max 60 s); 0 = off",
      "The last available account never cools."],
    "upstream_opts.overload_streak": ["overloaded threshold", "Start cooling after this many overloaded errors in a row; 0 = off"],
    "upstream_opts.overload_cooldown_secs": ["overloaded cooldown start",
      "First cooldown; doubles up to the max and resets after normal output"],
    "upstream_opts.overload_cooldown_max_secs": ["overloaded cooldown max", "Longest cooldown after doubling"],
    "upstream_opts.overload_retry_wait_secs": ["overloaded retry wait",
      "With no other account, wait about this long and retry once on the same one; 0 = no retry"],
    "upstream_opts.deactivated_cooldown_secs": ["402 deactivated cooldown",
      "On 402 deactivated_workspace switch account and cool it this long; 0 = off"],
    "upstream_opts.payment_required_cooldown_secs": ["Other 402 cooldown",
      "Billing or payment 402: switch account and cool it this long; 0 = off"],
    "upstream_opts.not_found_cooldown_secs": ["404 model cooldown",
      "On a 404 without “model unsupported”, cool that model on the account; 0 = switch only",
      "When the upstream says the model is unsupported, the cooldown is fixed at 12 hours."],
    "upstream_opts.connect_timeout_secs": ["Connect timeout", "Reaching the upstream or proxy longer than this is a network error"],
    "upstream_opts.compress_requests": ["Compress request bodies (zstd)",
      "zstd-compress /responses bodies sent upstream, as official Codex does"],
    "upstream_opts.gzip_responses": ["Upstream gzip responses",
      "Ask for gzip on direct Responses HTTP requests and decode locally; off asks for identity",
      "WebSocket, compact, models, images and passthrough routes are unchanged."],
    "upstream_opts.cloudflare_cookies": ["Cloudflare cookies per account",
      "Keep upstream infrastructure cookies per account, replayed only for that account"],
    "images.enabled": ["Images API", "Serve /v1/images/generations and /v1/images/edits"],
    "images.main_model": ["Images main model", "Wraps image requests that do not use gpt-image-*; default gpt-6.1-sol"],
    "images.default_originator": ["Default originator", "Only for non-Codex clients that send no originator"],
    "images.default_user_agent": ["Default User-Agent", "Used when the client User-Agent does not start with codex"],
    "log.level": ["Log level", "RUST_LOG takes precedence when set"],
    "log.redact_emails": ["Hide emails in logs", "Off keeps email addresses in stderr; on by default"],
    "usage.enabled": ["Usage log", "Record account, model, Tokens and status of every request; the overview reads it"],
    "usage.path": ["Usage database", "SQLite file, with -wal and -shm files next to it"],
    "usage.retention_days": ["Keep request details", "Older request details are removed; 0 = keep forever"],
    "usage.queue_size": ["Write queue length", "Records buffered for the writer; new records are dropped when full"],
    "usage.batch_size": ["Rows per batch", "Max records per database transaction"],
    "usage.flush_ms": ["Max write wait", "Write a partial batch after at most this long"],
    "usage.quota_weights.input": ["Quota weight: uncached input",
      "Weight of an uncached input Token when estimating Tokens per 1% of quota"],
    "usage.quota_weights.cached": ["Quota weight: cached input", "Weight of a cached input Token"],
    "usage.quota_weights.output": ["Quota weight: output", "Weight of an output Token (reasoning included)"],
    "usage.quota_weights.min_delta_pct": ["Quota estimate min change", "No estimate while the used ratio changed less than this"],
    "usage.prices": ["Price table file", "Reloaded when its content changes"],
    "usage.prices_source": ["Price data source", "Downloaded from here on Check for updates; empty = never"],
    "ui.enabled": ["Management UI", "Off disables every page under /ui"],
    "ui.allow_reset_consume": ["Allow using reset cards by hand",
      "Off hides the use button and the API refuses; per-account auto-use is not affected"],
    "ui.reset_credits_auto_lead_mins": ["Auto-use lead time", "Accounts with auto-use on use a card this long before it expires; 5–1440"],
    "ui.reset_credits_refresh_mins": ["Reset card refresh",
      "Read-only upstream check of every account's reset cards, at a random interval in this range"],
    "ui.reset_credits_refresh_max_mins": ["Reset card refresh max", "0 = same as the shortest (fixed); otherwise ≥ the shortest, max 60"],
    "ui.quota_refresh_mins": ["Automatic quota refresh",
      "Refresh quota of accounts with auto refresh on, each at a random time in this range"],
    "ui.quota_refresh_max_mins": ["Quota refresh max", "0 = same as the shortest (fixed); otherwise ≥ the shortest, max 60"],
    "ui.allow_config_write": ["Allow editing from the UI", "On lets the management UI change configuration and accounts"],
  };

  // English per enum choice: key → value → [label, help].
  const CHOICES_EN = {
    "routing.strategy": {
      "round-robin": ["Round-robin", "Takes accounts in name order, rotating per model; ignores weights. Evens out usage"],
      "weighted-round-robin": ["Weighted round-robin", "Splits by account weight; weight 0 is skipped. For accounts with different quotas"],
      "fill-first": ["Fill first", "Always the first available account by name, the next only when it cools or is disabled; " +
        "ignores weights. Uses one up and keeps the rest as backup"],
    },
    "routing.session_affinity_rebind_on": {
      "quota": ["Quota or token problems only", "Rebind only on exhausted quota, invalid token, 402 or deactivation; short failures " +
        "(such as repeated 5xx) borrow another account and the session returns afterwards"],
      "any": ["Any unavailability", "Rebind whenever the bound account is unavailable for any reason, then stay there"],
    },
  };

  const en = () => I18n.lang() === "en";
  const UNIT_SUFFIX = /（(秒|分钟|毫秒|MiB|次|%)）$/;
  const UNIT_TOKEN = {"秒": "s", "分钟": "min", "毫秒": "ms", "MiB": "MiB", "次": "times", "%": "pct"};
  const UNIT_KEY = {"usage.retention_days": "days", "max_inflight_requests": "count", "ws_max_connections": "count"};

  // Unit token of a key (s, min, ms, MiB, times, pct, days, count) or "".
  function unitToken(f) {
    if (UNIT_KEY[f.key]) return UNIT_KEY[f.key];
    const m = UNIT_SUFFIX.exec(f.desc || "");
    return m ? UNIT_TOKEN[m[1]] : "";
  }
  const unit = f => {
    const u = unitToken(f);
    return u ? I18n.t("settings.unit." + u) : "";
  };

  const title = f => (en() && SCHEMA_EN[f.key] ? SCHEMA_EN[f.key][0] : String(f.desc || f.key).replace(UNIT_SUFFIX, ""));

  // First clause of a long Chinese help text ("；" or "。"), so the row stays one line.
  function firstClause(s) {
    s = String(s || "");
    if (s.length <= 40) return s;
    const m = /^(.{6,}?)[；。：]/.exec(s);
    return m ? m[1] : s;
  }
  const summary = f => (en() && SCHEMA_EN[f.key] ? SCHEMA_EN[f.key][1] : firstClause(f.help));
  // Full explanation behind ⓘ, or "" when the summary already says everything.
  function details(f) {
    if (en() && SCHEMA_EN[f.key]) return SCHEMA_EN[f.key][2] || "";
    const s = summary(f), full = String(f.help || "");
    return full && full !== s ? full : "";
  }
  function choice(f, c) {
    const e = en() && CHOICES_EN[f.key] && CHOICES_EN[f.key][c.value];
    return e ? {label: e[0], help: e[1]} : {label: c.label, help: c.help};
  }
  // Everything a search may match, in both languages.
  function searchText(f) {
    const e = SCHEMA_EN[f.key] || [];
    const ch = (f.choices || []).map(c => {
      const x = CHOICES_EN[f.key] && CHOICES_EN[f.key][c.value];
      return [c.value, c.label, c.help, x ? x[0] : "", x ? x[1] : ""].join(" ");
    });
    return [f.key, f.desc, f.help, e[0], e[1], e[2], ch.join(" ")].filter(Boolean).join(" ").toLowerCase();
  }

  return {PAIRS, SCHEMA_EN, CHOICES_EN, unitToken, unit, title, summary, details, choice, searchText, firstClause};
})();
