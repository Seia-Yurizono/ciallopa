const $=id=>document.getElementById(id);
const KS="ciallopa_key",TH="ciallopa_theme";
// All dynamic text goes through textContent / createTextNode: never parsed as HTML.
function el(tag,cls,text){const e=document.createElement(tag);if(cls)e.className=cls;if(text!==undefined&&text!==null)e.textContent=String(text);return e}
function h(tag,a,...kids){const e=document.createElement(tag);
  if(typeof a==="string")e.className=a;
  else if(a)for(const k in a){const v=a[k];if(v===null||v===undefined||v===false)continue;
    if(k==="class")e.className=v;else if(k==="text")e.textContent=String(v);else if(k.startsWith("on"))e.addEventListener(k.slice(2),v);else e.setAttribute(k,v===true?"":String(v))}
  add(e,kids);return e}
function add(e,kids){for(const k of kids){if(k===null||k===undefined||k===false)continue;if(Array.isArray(k))add(e,k);else e.appendChild(typeof k==="object"?k:document.createTextNode(String(k)))}return e}
function clear(e){while(e.firstChild)e.removeChild(e.firstChild)}
// ---- formats
const NF=new Intl.NumberFormat("zh-CN");
const p2=x=>String(x).padStart(2,"0");
function exact(n){return n===null||n===undefined?"":NF.format(n)}
function fmtN(n){if(n===null||n===undefined)return"–";n=Number(n);const a=Math.abs(n);if(a>=1e9)return(n/1e9).toFixed(2)+"B";if(a>=1e6)return(n/1e6).toFixed(2)+"M";if(a>=1e3)return(n/1e3).toFixed(1)+"k";return String(Math.round(n))}
/*@unit ui-fmtMs-84*/function fmtMs(n){return n===null||n===undefined?"–":(n>=1000?(n/1e3).toFixed(n>=1e4?1:2)+" s":Math.round(n)+" ms")}/*@end ui-fmtMs-84*/
/*@unit ui-fmtUsd-57*/function fmtUsd(c){if(c===null||c===undefined)return"–";if(c===0)return"$0.00";if(c<0.01)return"<$0.01";return"$"+c.toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:2})}/*@end ui-fmtUsd-57*/
/*@unit ui-exactUsd-58*/function exactUsd(c){return c===null||c===undefined?"":"$"+c.toFixed(6)}/*@end ui-exactUsd-58*/
function tzOff(d){const o=-d.getTimezoneOffset(),a=Math.abs(o);return"UTC"+(o>=0?"+":"-")+p2(Math.floor(a/60))+":"+p2(a%60)}
function isoLocal(ms){const d=new Date(ms);return d.getFullYear()+"-"+p2(d.getMonth()+1)+"-"+p2(d.getDate())+"T"+p2(d.getHours())+":"+p2(d.getMinutes())+":"+p2(d.getSeconds())+tzOff(d).slice(3)}
function fmtTime(ms,noSec){const d=new Date(ms);return(d.getFullYear()!==new Date().getFullYear()?d.getFullYear()+"-":"")+p2(d.getMonth()+1)+"-"+p2(d.getDate())+" "+p2(d.getHours())+":"+p2(d.getMinutes())+(noSec?"":":"+p2(d.getSeconds()))}
function hms(d){return p2(d.getHours())+":"+p2(d.getMinutes())+":"+p2(d.getSeconds())}
// Compact countdown: 3天9时 / 1时06分 / 4分12秒.
function dur(s){if(s<=0)return"已到期";const d=Math.floor(s/86400),hh=Math.floor(s%86400/3600),m=Math.floor(s%3600/60);return d?d+"天"+hh+"时":hh?hh+"时"+p2(m)+"分":m+"分"+p2(Math.floor(s%60))+"秒"}
function ago(ms){const s=Math.max(0,(Date.now()-ms)/1000);if(s<60)return"刚刚";if(s<3600)return Math.floor(s/60)+" 分钟前";if(s<86400)return Math.floor(s/3600)+" 小时前";return Math.floor(s/86400)+" 天前"}
// Elements carrying data-until (unix ms) show a live countdown.
/*@unit countdowns*/
const countdowns=new Set(document.querySelectorAll("[data-until]"));
function trackCountdowns(e){if(e.nodeType!==1)return;if(e.hasAttribute("data-until"))countdowns.add(e);for(const n of e.querySelectorAll("[data-until]"))countdowns.add(n)}
new MutationObserver(changes=>{for(const c of changes){if(c.type==="attributes"){if(c.target.hasAttribute("data-until"))countdowns.add(c.target);else countdowns.delete(c.target)}else for(const e of c.addedNodes)trackCountdowns(e)}}).observe(document.body,{subtree:true,childList:true,attributes:true,attributeFilter:["data-until"]});
function tickCountdowns(){if(document.hidden||!countdowns.size)return;for(const e of countdowns){if(!e.isConnected){countdowns.delete(e);continue}e.textContent=(e.dataset.pre||"")+dur((Number(e.dataset.until)-Date.now())/1000)}}
setInterval(tickCountdowns,1000);
/*@end countdowns*/
// ---- access key (sessionStorage of this tab, shared by all three pages) and API calls
function key(){return sessionStorage.getItem(KS)||""}
function adminMsg(j){return j&&j.error&&j.error.type==="admin_required"?"当前密钥没有管理权限。请使用 admin = true 的密钥登录，或在配置文件的 [[api_keys]] 中为该密钥设置 admin = true。":null}
// o.timeout (ms): abort a request that gets no answer (the error then has kind "timeout").
/*@unit ui-req-17*/async function req(method,path,body,extra,o){
  const auth=key(),epoch=authEpoch,current=()=>key()===auth&&epoch===authEpoch&&(!o||!o.current||o.current()),hd={"Authorization":"Bearer "+auth};if(body!==undefined)hd["Content-Type"]="application/json";Object.assign(hd,extra||{});
  const ac=o&&o.timeout?new AbortController():null,tm=ac?setTimeout(()=>ac.abort(),o.timeout):null;let r;
  try{r=await fetch(path,{method,headers:hd,body:body===undefined?undefined:JSON.stringify(body),cache:"no-store",credentials:"same-origin",redirect:"manual",referrerPolicy:"no-referrer",signal:ac?ac.signal:undefined})}
  catch(e){const x=new Error(ac&&ac.signal.aborted?"请求超时（"+Math.round(o.timeout/1000)+" 秒内没有响应）":"网络错误，无法连接 ciallopa（"+e.message+"）");x.kind=ac&&ac.signal.aborted?"timeout":"network";throw x}
  finally{clearTimeout(tm)}
  gate(r);
  if(r.status===401){if(current()){sessionStorage.removeItem(KS);showApp(false,"密钥无效或已过期")}throw new Error("401")}
  let j=null;try{j=await r.json()}catch(_){}
  if(current()&&r.status===403&&j&&j.error&&j.error.type==="admin_required")NOADMIN=true;
  if(!r.ok){const e=new Error(adminMsg(j)||(j&&j.error&&j.error.message)||(path.split("?")[0]+" → HTTP "+r.status));e.status=r.status;e.body=j;throw e}
  return {j,r}}
/*@unit accounts-section-72*/// Behind Cloudflare Access an expired or missing login answers an API call with a redirect to the
// sign-in page (an opaque redirect under redirect:"manual") or an HTML page instead of JSON. That is
// not a network error and not a bad key: keep the key and ask for a page reload.
function gate(r){const hd=n=>r.headers&&r.headers.get?(r.headers.get(n)||"").toLowerCase():"",ks=r.headers&&r.headers.keys?[...r.headers.keys()]:[];
  if(r.type==="opaqueredirect"||r.status===0||r.redirected||hd("content-type").includes("text/html")||(r.status===401||r.status===403)&&ks.some(k=>k.toLowerCase().startsWith("cf-access"))){const x=new Error("Cloudflare Access 登录已过期，请刷新页面重新登录");x.kind="access";throw x}}
/*@end accounts-section-72*//*@end ui-req-17*/async function get(p,o){return (await req("GET",p,undefined,undefined,o)).j}
async function post(p,b){return (await req("POST",p,b||{})).j}
// Reason of a failed call for a toast: timeout / network as such, HTTP errors with their status.
function why(e){if(!e)return"未知错误";if(e.kind||!e.status)return e.message||String(e);return"HTTP "+e.status+" · "+e.message}
// A button action: busy state (spinner, optional label) while it runs; a failure becomes an error
// toast "<name>失败：<reason>". Returns false on failure. Calls pass `T` for a 45 s timeout.
const T={timeout:45000};
// Per-account result of an active quota query (刷新额度) → short reason.
// Quota windows by length: up to a day → short column (5 小时), longer → long column (周, or the
// free plan's single 30 天 window). Without window_minutes the slot decides (primary short).
/*@unit accounts-wlab-80*//*@unit ui-wlab-69*/function wlab(m,d){if(!m)return d;if(m===300)return"5h";if(m===10080)return"周";if(m>=1440)return Math.round(m/1440)+" 天";if(m>=60)return Math.round(m/60)+"h";return m+" 分"}
function wslots(q){const o={s:null,l:null};if(!q)return o;
  for(const [w,d,k] of [[q.primary,"5h","s"],[q.secondary,"周","l"]]){if(!w||w.used_pct===null||w.used_pct===undefined)continue;
    let s=w.window_minutes?(w.window_minutes>1440?"l":"s"):k;if(o[s])s=s==="s"?"l":"s";if(!o[s])o[s]=[wlab(w.window_minutes,d),w,s]}return o}
/*@end accounts-wlab-80*//*@end ui-wlab-69*/function whead(labels,def){const L=[...new Set(labels.map(x=>x==="5h"?"5 小时":x))];return(L.length?L.join(" / "):def)+"剩余"}
// 刷新额度 answered "asked moments ago": neutral, not a failure.
const qskip=r=>!r.ok&&r.error&&r.error.kind==="rate_limited";
function qsummary(j){const sk=j.skipped!==undefined?j.skipped:j.results.filter(qskip).length,cleared=(j.results||[]).filter(r=>r.cooldown_cleared).length,p=[];if(j.ok)p.push("已刷新 "+j.ok+" 个账号的额度");if(cleared)p.push(cleared+" 个已解除额度冷却");if(sk)p.push(sk+" 个 30 秒内刚查询过，已跳过");return p.join("，")||"没有可刷新的账号"}
function qerr(e){e=e||{};const m=e.message||"";return{rate_limited:"刚查询过，"+(e.retry_after_s||30)+" 秒后重试",http:"上游 HTTP "+e.status+(m?" · "+m:""),network:"无法连接上游（"+m.replace(/^send: /,"")+"）",timeout:"上游 5 秒内没有响应",credentials:"获取令牌失败（"+m.replace(/^credentials: /,"")+"）",token_invalid:"令牌失效，需要重新登录",decode:"上游响应中没有额度数据"}[e.kind]||m||"未知错误"}
// The button stays focusable while busy (aria-disabled), so keyboard focus is not dropped.
async function busyRun(b,name,f,label){if(b.disabled||b._busy)return false;const t=b.textContent;b._busy=true;b.classList.add("busy");b.setAttribute("aria-disabled","true");b.setAttribute("aria-busy","true");if(label)b.textContent=label;
  try{await f();return true}catch(e){if(e&&e.message!=="401")toast(name+(/[\w)]$/.test(name)?" ":"")+"失败："+why(e),{kind:"err"});return false}
  finally{b._busy=false;b.classList.remove("busy");b.removeAttribute("aria-disabled");b.removeAttribute("aria-busy");if(label&&b.isConnected)b.textContent=t}}
// set once the server says this key is not an admin: the overview then skips admin-only calls
let onLogout=()=>{},NOADMIN=false,authEpoch=0;
/*@unit ui-showApp-20*/function showApp(on,err){$("app").classList.toggle("hide",!on);if(on){$("main").classList.remove("noadm");const n=$("noadm");if(n)n.remove()}else NOADMIN=false;$("login").classList.toggle("hide",on);$("lerr").textContent=err||"";if(!on){authEpoch++;clearEgress();closeAllModals();onLogout()}}
/*@end ui-showApp-20*/function login(verify,enter){
  $("lf").addEventListener("submit",async e=>{e.preventDefault();sessionStorage.setItem(KS,$("key").value);$("key").value="";
    try{await verify();showApp(true);enter()}catch(err){if(err.message!=="401")$("lerr").textContent=err.message}});
  $("logout").addEventListener("click",()=>{sessionStorage.removeItem(KS);showApp(false)});
  if(key()){showApp(true);enter()}else showApp(false)}
// ---- theme (跟随系统 / 浅色 / 深色), remembered in this browser only
function applyTheme(t){const r=document.documentElement;if(t==="light"||t==="dark")r.dataset.theme=t;else delete r.dataset.theme;
  const n={light:"浅色",dark:"深色"}[t]||"跟随系统",b=$("theme");b.textContent={light:"☀",dark:"☾"}[t]||"◐";b.title="主题："+n+"（点击切换）";b.setAttribute("aria-label","切换主题，当前："+n)}
applyTheme(localStorage.getItem(TH));
$("theme").addEventListener("click",()=>{const c=localStorage.getItem(TH),n=c==="light"?"dark":c==="dark"?"":"light";if(n)localStorage.setItem(TH,n);else localStorage.removeItem(TH);applyTheme(n);toast("主题："+({light:"浅色",dark:"深色"}[n]||"跟随系统"))});
// ---- toasts; o.undo adds 撤销 (10 s). Errors are alerts with a close button (10 s). The timer
// pauses while the toast is hovered, touched or holds keyboard focus, and while the tab is hidden.
function toast(t,o){o=o||{};const err=o.kind==="err",d=h("div",{class:"toast"+(o.kind?" "+o.kind:""),role:err?"alert":null},h("span",null,t));let tm=null,ptr=false,foc=false;
  const bye=()=>{clearTimeout(tm);d.classList.add("out");setTimeout(()=>d.remove(),300)};
  const arm=()=>{clearTimeout(tm);if(ptr||foc)return;tm=setTimeout(()=>{if(document.hidden)arm();else bye()},o.undo||err?10000:4000)};
  if(o.undo){const b=h("button",{type:"button"},"撤销");b.addEventListener("click",async()=>{b.disabled=true;bye();try{await o.undo()}catch(e){toast("撤销失败："+e.message,{kind:"err"})}});d.appendChild(b)}
  if(err){const x=h("button",{type:"button",class:"x","aria-label":"关闭"},"✕");x.addEventListener("click",bye);d.appendChild(x)}
  for(const [ev,v] of [["pointerenter",true],["pointerdown",true],["pointerleave",false],["pointerup",false]])d.addEventListener(ev,()=>{ptr=v;arm()});
  d.addEventListener("focusin",()=>{foc=true;arm()});d.addEventListener("focusout",e=>{if(!d.contains(e.relatedTarget)){foc=false;arm()}});
  $("toasts").appendChild(d);arm();return d}
// An error shown in a page banner that is scrolled out of view is repeated as an error toast.
function inView(e){const r=e.getBoundingClientRect();return r.height>0&&r.bottom>0&&r.top<innerHeight}
// ---- re-rendered lists: the last focused control carrying data-fk gets focus back after a render
// that replaced it (row actions, settings fields). A pointer press elsewhere forgets it.
let FK=null;
document.addEventListener("focusin",e=>{const t=e.target.closest&&e.target.closest("[data-fk]");if(t)FK=t.dataset.fk;else if(!e.target.closest||!e.target.closest("#toasts"))FK=null});
document.addEventListener("pointerdown",e=>{if(!e.target.closest||!e.target.closest("[data-fk],#toasts"))FK=null},true);
function refocus(){if(!FK)return;const a=document.activeElement;if(a&&a!==document.body&&a.isConnected)return;
  const t=document.querySelector('[data-fk="'+CSS.escape(FK)+'"]');if(t&&t.offsetParent!==null)t.focus({preventScroll:true})}
// ---- steady re-render: f() replaces the children of `boxes`. Focus inside them is released
// first (removing a focused node makes the browser lay out the half-empty page and commit a
// clamped scroll), and the focused control, or else the row nearest the top of the view, keeps
// its viewport position. Anchors carry data-fk, data-ak or data-acc, stable across renders.
const ANK=["data-fk","data-ak","data-acc"],ANS=ANK.map(a=>"["+a+"]").join(",");
function ankSel(e){for(const a of ANK)if(e.hasAttribute(a))return"["+a+'="'+CSS.escape(e.getAttribute(a))+'"]';return null}
function ankPick(){const a=document.activeElement,hb=document.querySelector("header").getBoundingClientRect().bottom,vis=e=>{const r=e.getBoundingClientRect();return r.height>0&&r.bottom>hb&&r.top<innerHeight};
  if(a&&a!==document.body&&a.closest){const e=a.closest(ANS);if(e&&vis(e))return e}
  for(const f of [.5,.25,.75])for(let y=hb+8;y<innerHeight;y+=48){const e=document.elementFromPoint(innerWidth*f,y),t=e&&e.closest&&e.closest(ANS);if(t&&vis(t))return t}
  return null}
function steady(boxes,f){const a=document.activeElement;if(a&&a!==document.body&&boxes.some(b=>b&&b.contains(a)))a.blur();
  const e=ankPick(),sel=e&&ankSel(e),top=e?e.getBoundingClientRect().top:0;f();
  const n=sel&&document.querySelector(sel);if(n&&n.getBoundingClientRect().height>0){const d=n.getBoundingClientRect().top-top;if(Math.abs(d)>=1)scrollBy(0,d)}}
// ---- dialogs: focus goes to 取消, Esc / backdrop close, Tab trapped, focus returns to the opener.
// A [data-arm] button ignores clicks for its first 400 ms (no double tap through the opener).
/*@unit ui-MODALS-55*//*@unit ui-MODALS-53*/const MODALS=[];
function openModal(m,focus,opener){if(!m.classList.contains("hide"))return;m.classList.remove("hide");m._opener=opener||document.activeElement;m._at=Date.now();MODALS.push(m);document.documentElement.classList.add("modal-open");(focus||m.querySelector("button")).focus()}
function closeModal(m){if(m.classList.contains("hide"))return;m.classList.add("hide");const i=MODALS.indexOf(m);if(i>=0)MODALS.splice(i,1);const o=m._opener;m._opener=null;if(m._onclose)m._onclose();document.documentElement.classList.toggle("modal-open",MODALS.length>0);if(o&&document.contains(o))o.focus({preventScroll:true})}
function closeAllModals(){for(const m of MODALS.slice().reverse())closeModal(m)}
// Tab order inside a modal: visible, enabled controls, including <summary> toggles; nothing inside a closed <details>.
function tabbables(m){return Array.from(m.querySelectorAll("button,a[href],input,select,textarea,summary,[tabindex]:not([tabindex='-1'])")).filter(x=>!x.disabled&&x.offsetParent!==null&&!x.closest("details:not([open]) > :not(summary)"))}
/*@end ui-MODALS-55*/document.addEventListener("keydown",e=>{const m=MODALS[MODALS.length-1];if(!m)return;
  if(e.key==="Escape"){e.preventDefault();closeModal(m);return}
  if(e.key!=="Tab")return;const f=tabbables(m);if(!f.length)return;
  const i=f.indexOf(document.activeElement);e.preventDefault();f[i<0?(e.shiftKey?f.length-1:0):(i+(e.shiftKey?f.length-1:1))%f.length].focus()});
document.addEventListener("click",e=>{const b=e.target.closest&&e.target.closest("[data-arm]");if(!b)return;const m=b.closest(".modal");if(m&&Date.now()-m._at<400){e.preventDefault();e.stopImmediatePropagation()}},true);
document.addEventListener("click",e=>{const m=MODALS[MODALS.length-1];if(m&&e.target===m)closeModal(m)});
/*@end ui-MODALS-53*/// ---- inline click-again: the first click turns the button into `label` for 3 s (armed after
// 300 ms); a second click runs `act`; timeout, blur or Esc reverts.
function disarm(b){if(!b._armed)return;clearTimeout(b._t);b._armed=0;b.textContent=b._orig;b.classList.remove("armed")}
function clickAgain(b,label,act){if(b._armed){if(Date.now()-b._armed<300)return;disarm(b);return act()}
  b._orig=b.textContent;b._armed=Date.now();b.textContent=label;b.classList.add("armed");b._t=setTimeout(()=>disarm(b),3000);
  b.addEventListener("blur",()=>disarm(b),{once:true})}
document.addEventListener("keydown",e=>{if(e.key==="Escape"&&document.activeElement&&document.activeElement._armed)disarm(document.activeElement)});
/*@unit ui-section-21*/// ---- shell: account count on the 账号 tab, egress tag in the header (admin keys only)
function setTabCount(n){const c=document.querySelector("nav.tabs a[data-p=accounts] .cnt");if(c)c.textContent=n===null||n===undefined?"":String(n)}
let egressSeq=0;
function clearEgress(){egressSeq++;const t=$("egress");t.className="hide";t.textContent="";t.title=""}
async function egressTag(){const seq=++egressSeq,auth=key(),current=()=>seq===egressSeq&&auth===key(),read=p=>get(p,{timeout:30000,current}),t=$("egress");
  try{if(NOADMIN)throw new Error("admin");const c=await read("/admin/v1/runtime");if(!current())return;const eg=Object.assign({},c.egress||{},{runtime:c.runtime});
    if(eg.direct){t.className="tag line";t.textContent="直连";t.title="出口：直连（未使用出口代理）· 点击查看出口设置";return eg}
    const n=c.proxy_errors_1h||0;
    if(!current())return;t.className="tag proxy dot";t.textContent=n?"出口代理 · "+n+" 错误":"出口代理";t.title="出口："+eg.proxy+(n?"；近 1 小时 "+n+" 次出口代理错误":"；近 1 小时没有出口代理错误")+" · 点击查看出口设置";return eg}
  catch(_){if(!current())return;clearEgress();return null}}
/*@end ui-section-21*/// Accounts / settings need an admin key. A usage-only key (e.g. carried over from the overview in
// this tab) gets a plain explanation instead of a half-loaded page.
function needAdmin(e){if(!e||!e.body||!e.body.error||e.body.error.type!=="admin_required")return false;const m=$("main");
  if(!$("noadm"))m.appendChild(h("div",{class:"empty panel",id:"noadm"},h("b",null,"此页面需要管理密钥"),adminMsg(e.body),h("div",null,h("button",{type:"button",class:"pri",onclick:()=>{sessionStorage.removeItem(KS);showApp(false)}},"使用其他密钥登录"))));
  m.classList.add("noadm");$("msg").textContent="";return true}
