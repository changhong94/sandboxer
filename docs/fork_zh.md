# 运行中快照：fork 第一版

基于上游 sandboxer `3d546021e3b84b1a0acf7ecca9aa320aec1db79d`。
这里 fork 的操作结果是一份普通快照，原沙箱继续运行；不会自动启动子沙箱。
需要派生实例时，使用已有的 `run --restore`。

## 调用

```sh
# 与 snapshot 相同的本地输出、格式选项
sandbox-ctl fork --sandbox-id parent --run-root /run/sandbox --output /path/to/snapshots --json

# 与 snapshot --upload 相同的 Manifest/chunk 存储路径
sandbox-ctl fork --sandbox-id parent --run-root /run/sandbox --upload --json
```

remote 使用父运行实例启动时 `--manifest-config` 选定的 Store、chunker、加密等配置。
local 的 `--mode local|bundle`、加密和引用解析沿用普通 snapshot。
local/remote 的选择入口也沿用普通 snapshot：`--output` 与 `--upload` 互斥，未引入另一套存储配置。
Go SDK 提供 `runtime.Fork(ctx, ctl.Request{OutDir: directory})`，remote 使用 `Upload: true`。
父沙箱始终恢复运行；这个命令不使用命名 checkpoint/退役 holder 路径。

## 执行和一致性

1. 复用原有捕获互斥、guest quiesce、stdio 排空、CH pause、后端 quiesce 和状态捕获。
2. 在同一个冻结边界中，将磁盘上层及已物化的 RAM 数据复制到匿名 memfd，保留 Hole/Zero/Data 区别，再加写入/扩缩容封印。
3. 恢复后端、CH 和 guest MUX；应用完成 thaw 后继续执行。
4. 普通 FileSink/BundleSink/IngestSink 从独立输入完成原有 S/E 快照保存，返回原有引用。
5. 关闭临时 memfd。保存期间仍串行化该沙箱的捕获操作；没有新后台任务队列。

保存工作与恢复后的应用并行，但调用本身等到保存完成才返回。结果可用既有 restore 路径打开。
捕获前失败按原流程恢复应用；恢复失败沿用原来的终止保护。保存失败报告错误，不将未完成的结果当成功。
最终输出沿用上游保存器的持久性合同，没有新增或降低断电保证。

## 第一版边界

没有池化、预热、专用镜像处理、host fork、UFFD 写保护或定制 VMM 补丁。
没有改变既有 snapshot/pause 的语义、模板格式或存储后端。
为了保持实现简单，冻结期间复制被捕获的数据；所需临时 RAM 约为已物化 guest RAM 加磁盘上层 Data 大小，保存完成后释放。
因此暂停随捕获数据量增长，本版不是零复制/页级 CoW 的性能版本。没有增加 HTTP/E2B API；入口是现有 sandbox-ctl/ctl.sock 和 Go Runtime SDK。

## 计时与验证

`fork_pause_ms`：发出 guest quiesce 前，到成功重新连接 MUX/应用 thaw 完成。它是完整暂停阶段的耗时，包含控制请求时间。
`fork_save_ms`：捕获处理入口，到保存、提交和必要收尾完成，包含暂停。
客户端另外记录命令发出到收到完整结果的耗时，包含命令进程启动与 RPC。
不要把旧 `wallclock_pause_ms`（仅 CH pause RPC）当作应用暂停；fork CLI 不输出这个旧指标。

`scripts/bench-fork.py` 接收独立测试用的 JSON sandbox config、已运行的 Manifest Store 配置和二进制目录。
每种存储方式默认 1 次预热加 50 次正式样本。验证保存期间父进程继续计数、恢复后的内存标识与计数、磁盘内容、独立写入与两种删除顺序。
测试资源使用独立 run-root/base-root；脚本只关闭自己启动的进程。输出目录必须不存在。
