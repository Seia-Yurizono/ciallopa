const fs=require('fs'),vm=require('vm'),assert=require('assert');
const src=fs.readFileSync(require('path').join(__dirname,'..','crates/server/src/ui.html'),'utf8');
const grab=(start,end)=>{const a=src.indexOf(start);const b=src.indexOf(end,a);assert(a>=0&&b>a,start);return src.slice(a,b)};
const code=grab('const OMK=','async function lookupRecord')+'\n'+grab('function fmtMs(','\n');
class El{constructor(t){this.tag=t;this.kids=[];this.attrs={};this.style={};this.classList={add(){},remove(){}}}append(...k){for(const x of k)if(x!=null)this.kids.push(typeof x==='string'?{text:x}:x)}appendChild(x){this.append(x)}get text(){return this.kids.map(k=>k.text!==undefined&&!(k instanceof El)?k.text:k.text).join('')}}
Object.defineProperty(El.prototype,'childElementCount',{get(){return this.kids.filter(k=>k instanceof El).length}});
Object.defineProperty(El.prototype,'text',{get(){return this.kids.map(k=>k instanceof El?k.text:k.text).join('')}});
const box=new El('div');
const ctx={streamRecord:r=>r.transport==='sse'||r.transport==='ws',usageGet:null,fmtN:n=>String(n),$:id=>box,clear:e=>{e.kids=[]},h:(t,a,...k)=>{const e=new El(t);if(typeof a==='string')e.attrs.class=a;else if(a)Object.assign(e.attrs,a);e.append(...k.map(x=>typeof x==='number'?String(x):x));return e}};
vm.createContext(ctx);vm.runInContext(code,ctx);
const all=e=>[e,...e.kids.filter(k=>k instanceof El).flatMap(all)];
// SSE backpressure: axis must reach 15.3 s (last yield), not the 9 s duration.
ctx.obsTimeline({up_first_text_ms:1100,up_terminal_ms:8900,up_last_event_ms:8900,sse_first_queue_ms:1100,sse_last_queue_ms:8900,sse_first_yield_ms:1100,sse_last_yield_ms:15300,sse_queue_blocked_us:6200000,sse_queue_blocked_max_us:1800000,up_first_reasoning_ms:null,up_first_tool_ms:null,ws_deflate_down:null,ws_deflate_up:null,down_incomplete:false},{duration_ms:9000,transport:"sse"});
let t=box.text;assert(t.includes('15.3 s'),t);assert(t.includes('6.20 s'),t);assert(!t.includes('压缩协商'));
const dn=all(box).find(e=>e.attrs.class==='os dn');assert.equal(dn.style.left,'7.19%');assert.equal(dn.style.width,'max(2px,92.81%)');
const dur=all(box).find(e=>e.attrs.class==='odur');assert.equal(dur.style.left,'58.82%');
const chart=all(box).find(e=>e.attrs.role==='img');assert(chart.attrs['aria-label'].includes('推理 无'),chart.attrs['aria-label']);
// WS: null deflate is 未知, unrecognized is 无法识别, ext text stays text.
ctx.obsTimeline({up_first_text_ms:650,up_terminal_ms:2880,ws_first_send_ms:650,ws_last_send_ms:2885,ws_sink_blocked_us:0,ws_deflate_down:null,ws_deflate_up:false,ws_ext_up:"unrecognized"},{duration_ms:2900,transport:"ws"});
t=box.text;assert(t.includes('客户端 未知'),t);assert(t.includes('上游 无法识别'),t);assert(t.includes('0 ms'),t);
ctx.obsTimeline({up_first_text_ms:5,ws_first_send_ms:5,ws_last_send_ms:6,ws_deflate_down:true,ws_ext_down:'<img onerror=x>',ws_deflate_up:false},{duration_ms:10,transport:"ws"});
assert(box.text.includes('客户端 已协商 · 上游 未协商'),box.text);
// Old row without first text: legend shows – not 0.
ctx.obsTimeline({up_first_reasoning_ms:1300,up_first_tool_ms:4800,up_terminal_ms:6000},{duration_ms:6100,transport:"sse"});
assert(box.text.includes('文本 –'),box.text);

// Blocked time: measured 0 stays "0 ms"; null / undefined / junk strings are "–" (never 0), and injected text is not echoed.
for(const [v,want] of [[0,'0 ms'],[null,'–'],[undefined,'–'],['<b>x</b>','–'],[-5,'–'],[2500,'3 ms']])assert.equal(ctx.blkText(v),want,String(v));
ctx.obsTimeline({up_first_text_ms:5,ws_first_send_ms:5,ws_last_send_ms:6,ws_sink_blocked_us:0,ws_sink_blocked_max_us:0},{duration_ms:10,transport:"ws"});
assert(box.text.includes('下游阻塞累计 0 ms'),box.text);assert(box.text.includes('仅累计，无时段'),box.text);
ctx.obsTimeline({up_first_text_ms:5,ws_first_send_ms:5,ws_last_send_ms:6},{duration_ms:10,transport:"ws"});
assert(box.text.includes('下游阻塞累计 –'),box.text);assert(box.text.includes('未测'),box.text);assert(!box.text.includes('0 ms · 最长'));
// Chart text equivalent includes the blocked total, not only a title.
assert(all(box).find(e=>e.attrs.role==='img').attrs['aria-label'].includes('下游阻塞累计 –'));
// Only reasoning (no text, no tool, no terminal); axis covers every milestone (>= max), compact legend names both SSE meanings.
ctx.obsTimeline({up_first_reasoning_ms:20000,sse_first_queue_ms:1,sse_last_queue_ms:2,sse_first_yield_ms:1,sse_last_yield_ms:3},{duration_ms:500,transport:"sse"});
assert(box.text.includes('20.0 s'),box.text);assert(box.text.includes('文本 –')&&box.text.includes('工具 –')&&box.text.includes('结束 –'),box.text);
assert(box.text.includes('SSE 入队')&&box.text.includes('SSE 交给 HTTP 层'),box.text);
const mk=all(box).find(e=>e.attrs.class==='om r');assert.equal(mk.style.left,'100.00%');
// Legacy row without observation: a short hint, no empty track pretending to be 0.
(async()=>{box.kids=[];ctx.usageGet=async()=>{const e=new Error('404');e.status=404;throw e};
  await ctx.loadObservation({request_id:'0000000000000001',transport:'sse',duration_ms:5},()=>true);
  assert(box.text.includes('无观测数据'),box.text);assert(!all(box).some(e=>e.attrs.class==='otrack'));
  // Non-stream rows show nothing.
  box.kids=[];await ctx.loadObservation({request_id:'0000000000000002',transport:'http'},()=>true);assert.equal(box.text,'');
  // Exact values sit behind a native <details>/<summary> (Tab/Enter/Space work without script).
  ctx.usageGet=async()=>({up_first_text_ms:1,up_terminal_ms:2,response_id:'<img src=x onerror=1>'});box.kids=[];
  await ctx.loadObservation({request_id:'0000000000000003',transport:'sse',duration_ms:3},()=>true);
  const det=all(box).find(e=>e.tag==='details');assert(det&&det.kids[0].tag==='summary'&&!det.attrs.open);
  assert(box.text.includes('<img src=x onerror=1>'),'injected id rendered as text');
  // Markup: metadata collapsed after the observation block, scale label in the list header, reduced motion.
  const iObs=src.indexOf('id="dotl"'),iMeta=src.indexOf('id="dmetad"');assert(iObs>0&&iMeta>iObs);
  assert(/<details class="dmetad" id="dmetad"><summary>/.test(src));assert(src.includes('id="hdurs"')&&src.includes('首输出</span></th>'));
  assert(src.includes('@media (prefers-reduced-motion:reduce){.otl'));
  console.log('timeline tests ok')})().catch(e=>{console.error(e);process.exit(1)});

// Modal focus trap (shared block in all three pages): visible <summary> must be in the Tab order;
// content of a closed <details>, hidden and disabled controls must not be.
{
  const pages=['ui','accounts','settings'].map(n=>fs.readFileSync(require('path').join(__dirname,'..','crates/server/src',n+'.html'),'utf8'));
  for(const p of pages){
    const m=p.match(/function tabbables\(m\)\{[^\n]*\}/);assert(m,'tabbables() missing from the modal focus trap');
    const c={};vm.createContext(c);vm.runInContext(m[0],c);
    const el=(tag,o={})=>({tag,disabled:false,offsetParent:{},inClosed:false,...o,closest(sel){return sel.startsWith('details:not([open])')&&this.inClosed?{}:null}});
    const prev=el('button'),sum=el('summary'),meta=el('summary'),inner=el('button',{inClosed:true}),hid=el('button',{offsetParent:null}),dis=el('button',{disabled:true});
    const modal={querySelectorAll(sel){assert(/(^|,)summary(,|$)/.test(sel),'selector lacks summary: '+sel);return [prev,sum,meta,inner,hid,dis]}};
    {const got=c.tabbables(modal),want=[prev,sum,meta];assert(got.length===3&&got.every((x,i)=>x===want[i]),"tabbables wrong")}
    assert(/f=tabbables\(m\)/.test(p),'Tab handler does not use tabbables()');
  }
}
console.log('focus trap tests ok');
