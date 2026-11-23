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
父沙箱始终恢复运行；不发布用户可见的命名 checkpoint，也不退役原 VM。

## 执行和一致性

1. 复用原有捕获互斥、guest quiesce、stdio 排空、CH pause、后端 quiesce 和状态捕获。
2. 在同一个冻结边界中，版本化磁盘创建上游 checkpoint，固定旧版本，随后父沙箱写入由已有磁盘 CoW 隔离；不可变 reader pin 等原沙箱恢复后、保存器实际读取时才打开，避免把读取元数据的等待放进暂停阶段。保存器关闭 pin，操作结束删除临时 checkpoint。未启用版本化磁盘时保留独立复制回退。RAM Data 复制到匿名 memfd，优先使用内核直接复制，最多两个独立区间复制任务，全部结束后加写入/扩缩容封印。Hole/Zero/Data 区别保持不变。
3. 恢复后端、CH 和 guest MUX；应用完成 thaw 后继续执行。
4. 普通 FileSink/BundleSink/IngestSink 从独立输入完成原有 S/E 快照保存，返回原有引用。
5. 关闭临时 memfd、磁盘 pin，并清理临时 checkpoint；失败路径也回收这些资源。保存期间仍串行化该沙箱的捕获操作；没有新后台任务队列。

保存工作与恢复后的应用并行，但调用本身等到保存完成才返回。结果可用既有 restore 路径打开。
捕获前失败按原流程恢复应用；恢复失败沿用原来的终止保护。保存失败报告错误，不将未完成的结果当成功。
最终输出沿用上游保存器的持久性合同，没有新增或降低断电保证。

## 第一版边界

没有池化、预热、专用镜像处理、host fork、UFFD 写保护或定制 VMM 补丁。
没有改变既有 snapshot/pause 的语义、模板格式或存储后端。
上游暂停加速的 RAM holder 依赖原 VM 停止写入，不能直接交给继续运行的父 VM 使用。本实现仅复用已有磁盘版本和 pin，不引入新的 RAM 写保护机制。冻结期间仍复制被捕获的 RAM Data；版本化磁盘不再占用同等大小的临时 RAM 副本，未版本化磁盘才复制上层 Data。临时资源在保存完成后释放。
因此暂停随捕获数据量增长，本版不是零复制/页级 CoW 的性能版本。没有增加 HTTP/E2B API；入口是现有 sandbox-ctl/ctl.sock 和 Go Runtime SDK。

## 计时与验证

`fork_pause_ms`：发出 guest quiesce 前，到成功重新连接 MUX/应用 thaw 完成。它是完整暂停阶段的耗时，包含控制请求时间。
`fork_save_ms`：捕获处理入口，到保存、提交和必要收尾完成，包含暂停。
客户端另外记录命令发出到收到完整结果的耗时，包含命令进程启动与 RPC。
不要把旧 `wallclock_pause_ms`（仅 CH pause RPC）当作应用暂停；fork CLI 不输出这个旧指标。

`scripts/bench-fork.py` 接收独立测试用的 JSON sandbox config、已运行的 Manifest Store 配置和二进制目录。
每种存储方式默认 1 次预热加 50 次正式样本。验证保存期间父进程继续计数、恢复后的内存标识与计数、磁盘内容、独立写入与两种删除顺序。
测试资源使用独立 run-root/base-root；脚本只关闭自己启动的进程。输出目录必须不存在。

## 真实 guest 功能集成测试

`scripts/test-fork-guest.py` 将并发写入、父子隔离、恢复后再次捕获整理为可重复运行的测试。
不依赖本机目录、已有测试结果或历史快照。需要 Linux/KVM、运行 sandbox-ctl 所需的权限（通常为 root）、
可启动的 Python 镜像、内核及 guest runtime；不使用 mock 或 shell 输出替代 guest 进程。

准备参数：

- `--bin`：包含本分支编译的 `sandbox-ctl`、对应的 `cloud-hypervisor`。自动启动测试 Store 时还需要 `store-ctl`。
- `--config`：已有的冷启动 JSON 配置，包含 `resources`、`boot.kernel`、`boot.runtime`、Python 镜像的 `boot.root.base` 和可写磁盘设置。引用应为完整可解析的路径/URI。测试会替换 `launch`，运行自身的 Python 工作负载；不会修改输入配置和基础镜像。
- `--python`：镜像内 Python 路径，默认 `/usr/local/bin/python3`。
- `--output`：必须尚不存在的输出目录。保留配置、guest 日志、快照、逐项 `results.json` 及 `summary.json`。

```sh
sudo python3 scripts/test-fork-guest.py \
  --bin /path/to/bin \
  --config /path/to/python-cold.json \
  --output /path/to/new-test-results
```

默认在 local 和 remote 各执行两个删除顺序，并覆盖以下场景：

1. **父子隔离**：父进程持续输出内存中的随机身份和递增计数；保存时父进程已继续执行。恢复后检查身份和计数继承、文件内容及父子分别写入同一路径。分别先删除父、先删除子，检查剩余实例仍能执行命令并读到自己的内容。
2. **保存期间并发写入**：预先写入并同步 32 MiB 的 A 内容。在父沙箱 thaw 后，经原进程的 stdin 触发覆写和 fsync 为 B，要求收到完成确认时快照保存尚未返回。恢复出的子沙箱必须仍读到 A，父沙箱读到 B。若写入未与保存发生重叠，测试明确失败，不将其算作通过；不通过新 exec 绕过捕获安全屏障。
3. **恢复后再次快照**：先创建自己的初始快照，再恢复、修改、重新捕获并恢复下一代。默认还跨 local/remote 路径，检查内存身份继承、文件隔离和独立销毁。

默认一次完整执行共 14 项功能检查，无性能 warmup。`--iterations N` 重复整套检查。
可用 `--scenario isolation|concurrent|chain` 单独选择场景，或用 `--modes local` / `--modes remote` 限定存储方式。
每项记录暂停及请求到保存完成的实际耗时，仅供诊断；并发写入场景增加了工作负载，不与启动或轻负载 benchmark 混用。

未传 `--manifest-config` 时，脚本在输出目录中创建独立的 fs Store，以 loopback 随机端口服务，使用 CDC 和 AES chunk/manifest；密钥配置文件权限为 0600。
这是真实 Manifest/chunk 路径，但不是跨机器或对象存储性能测试。脚本退出时停止自建 Store，保留其数据供排查。
也可传 `--manifest-config /path/to/test-manifest.json` 使用已启动的测试 Store；此时脚本不会启动、停止或更改该服务，但 remote 测试会写入快照数据。

运行目录使用 `--run-root` 下的随机子目录，默认父目录 `/run/kuasar-fork-tests`。正常结束和异常退出都会尝试关闭自己创建的实例及 Store，
不通过进程名批量杀进程，也不重启现有服务。失败返回非零退出码，`summary.json` 保留错误和清理状态；强制终止不算清理验证成功。
该套测试不验证网卡身份隔离或宿主断电恢复。大量重复计时请使用前述 `scripts/bench-fork.py`。

### 失败与清理语义

fork 使用的磁盘 checkpoint 带有持久化的临时标记，标记和 checkpoint
在同一事务中提交。正常结束时关闭 reader pin 并删除临时 checkpoint；
即使创建返回 I/O 错误，也尝试清理，因为事务可能已提交。
若进程退出或清理失败，重新打开该磁盘时只撤销有临时标记的 checkpoint，
交给已有 GC 回收；普通用户 checkpoint 不受影响，仍存活的 reader pin
继续保护其数据。没有增加后台任务队列或重试服务。

只有快照持久化、发布及原有校验均成功后，临时资源清理失败才作为
`cleanup_warning` 返回，此时快照引用仍有效；CLI 同时在 stderr 打印警告。
持久化失败或取消仍返回错误，不作为成功快照报告。fork 的 JSON 字段保持
原样，但与普通 snapshot 共用响应类型和快照引用的校验。

对应故障回归测试为 `pkg/snapshot/fork_cleanup_test.go` 和
`pkg/diffstore/internal/versioned/temporary_checkpoint_test.go`，覆盖提交后 I/O
错误、保存中取消、存储写入失败、清理失败、进程未执行 Close 就退出，以及
重新打开磁盘后 reader pin 仍保持 CoW 隔离。CLI 的空引用和错误响应类型由
`cmd/sandbox-ctl/fork_test.go` 检查。
