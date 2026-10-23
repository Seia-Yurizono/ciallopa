# 网关与 Agent：研究结论

研究以 Codex 为主、OpenCode 为次；通用网关用于比较生命周期、资源和配置设计。
版本与来源固定于 2026-10-23，滚动官方文档按抓取日解释。升级客户端或依赖时重新核对，
不把源码推断、本地假上游结果和真实供应商兼容混为一谈。

## 来源与采用的思想

| 来源 | 值得采用的思想 | 当前选择 |
| --- | --- | --- |
| NGINX | 建连、上游读写和下游写分别计时，重放受发送状态约束 | 不用单个超时覆盖长生成，前置反代不叠加宽泛 POST 重试 |
| HAProxy | 队列、连接、响应等待及重试资格分别解释 | 保持立即拒绝式准入，先观察具体等待阶段 |
| Envoy | 背压水位、并发重试预算和进程过载分别解决不同问题 | 有界观察与取消优先；资源调度由实测需求推动 |
| Pingora | 连接池、双向通道和退出的资源所有权明确 | 保留按账号隔离的连接池和有限缓冲 |
| Caddy | 简洁部署，配置替换与旧 WS 生命周期分开 | 已验证重载和有限延迟关闭；不承诺无损热更新 |
| Traefik | 启动设置与动态路由分开 | 设置页明确保存、生效和待重启的区别 |
| Kong | 管理面故障时保留已知有效配置；插件可能改变缓冲方式 | 保留有效快照，观测失败不阻塞转发 |
| APISIX | 已接受、已应用和已持久化是不同状态 | 单进程只报告真实生效范围，不增加分布式控制面 |
| LiteLLM | retry、fallback、cooldown 及实际服务目标分别记录 | 保留尝试归因，不透明拼接不同模型的输出 |

固定源码、具体指令和引用见[传输研究](research/GATEWAY_TRANSPORT.md)及
[配置与扩展研究](research/GATEWAY_CONTROL.md)。未部署这些产品做吞吐排名。
Agent Router 的[项目更名说明](https://theagentrouter.ai/blog/envoy-ai-gateway-is-now-agent-router/)及
Cloudflare 的[路由协议限制](https://developers.cloudflare.com/ai-gateway/features/dynamic-routing/)
作为辅助：支持更多供应商不等于支持每种原生协议。

## Codex：首要兼容对象

固定 Codex 0.163/0.164 源码，实际客户端探针使用官方 0.164.0。
详情见 [Codex 网络契约](research/CODEX_NETWORK.md)，本地命令见 [TESTING.md](TESTING.md)。

- Responses 终止、工具 call ID、多轮历史、WS `previous_response_id` 和加密上下文都有实际语义。
  模型列表可读或一次文本成功不足以声明完整兼容。
- HTTP 重试、流恢复、WS→SSE 回退和特定断网持续重试是不同层次，没有统一的“最多30次”。
- Codex 的 SSE 完整事件 idle 与网关的 chunk/frame 活动不同；注释心跳不等于客户端事件进展。
- 客户端 WS 握手期限可能先于网关准备预算结束；取消应释放工作和租约，单独增大服务端预算不能解决。
- 账号池的网关密钥模式与保留用户 ChatGPT 身份的企业双凭据透传模式不同，当前仅实现前者。

已具备真实本地客户端的恢复、工具、多轮、取消和重载探针。其模型上游均为假服务；
真实加密有效性、跨账号历史和供应商计费仍不在本地验证结论内。
工具和代理重载的具体恢复边界见 [RECOVERY.md](RECOVERY.md)。

## OpenCode 与 Claude Code

固定 OpenCode 1.19.4，完整分析见 [Agent 网络研究](research/AGENT_NETWORK.md)。

- `@ai-sdk/openai` 使用 Responses，`@ai-sdk/openai-compatible` 使用 Chat Completions；修改 baseURL 不改变协议。
- OpenCode 的内置 OAuth 插件可能重写目标地址；API-key 与 OAuth 配置不能混作一份接入承诺。
- 隔离 API-key 画像已经通过本地工具结果、第二轮与活动流取消；真实 Codex 后端仍未声明完整支持。
- Claude Code 仅核对官方资料，其 Messages 协议当前没有适配器；未执行该客户端或用 SDK 默认值替代 CLI 行为。
- 模型 baseURL 不接管 MCP、工具子进程、更新、OAuth 或所有诊断网络；ciallopa 是模型请求网关。

## 研究如何影响开发

优先解释请求、尝试、配置和流各自何时完成。记录未知值、取消和失败的区别，保留原生字段与事件，
以有限故障实验验证重放资格。已有固定版本的 [CLIProxyAPI 反馈分析](research/CLIPROXY_RECOVERY.md)
分别讨论断线重新生成、转换器重复输出和未定位的 Agent 循环，不按相似症状推断同一根因。

第二真实上游出现时，再验证最小凭据、路径、流、用量和错误边界；性能调整由代表性混合负载推动。
具体实现以[架构](ARCHITECTURE.md)、[设置](SETTINGS.md)和[恢复边界](RECOVERY.md)为准，
研究建议不自动成为待办或已支持能力。
