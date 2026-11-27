# Native fork migration (experimental)

This branch is based on `kuasar-sandbox/sandboxer`, not the older
`kuasar-io/kuasar` Rust VMM. The initial implementation adds
`pkg/runtime.Fork` and `sandbox-ctl fork` to the native Go lifecycle.

## Snapshot-backed backend and guarantees

`native-snapshot` captures a running source with the existing guest execution,
memory-report, device and writable-disk barriers. It forces `resume_after=true`,
on checkpoint-capable disks resumes the parent after capturing its CPU/device
state and installing before-image protection, then persists the immutable S/E
views and restores an independent child through the existing UFFD and disk CoW paths. The API returns
after the child's guest ACK and MUX readiness. A failed child restore leaves the
committed snapshot intact and reports its references; it never deletes the
parent. Local and Manifest/chunk capture use the normal native implementations.

This is **not** the legacy inherited-private-RAM fast fork. Child creation still waits for snapshot serialization and durable publication.
The parent no longer needs to remain paused during those operations when the
live-capture capabilities are available. Native RAM is a shared
memfd used by external vhost-user backends, so placing an OS fork around this
runtime would not create independent guest-RAM CoW semantics. This `native-snapshot` backend remains snapshot-backed. The separately selected
`native-private` backend described below uses inherited private RAM. The generic pool SDK below prepares native base VMs.
The existing console deployment and its artifacts remain on the legacy backend
until a separate deployment/cutover is requested. Do not deploy this experimental backend as the legacy service
replacement or infer its performance from legacy measurements.

## Direct private-RAM fork (`native-private`)

`pkg/runtime.ForkNative` and `sandbox-ctl fork --backend native-private` now
use the legacy VMM's safe same-host fork machinery, with native Go storage and
agent ownership. The source must have been started with
`run --private-ram-fork --enable-checkpoint`, using the isolated migrated VMM.
This mode does not call `/vm.snapshot`, serialize RAM, generate `memory.img`,
or cold-boot the child. It retains the parent's KVM objects and reconstructs
only the child's KVM/device state from the inherited private address space.

External vhost-user RAM mappings cannot preserve those semantics. This mode
keeps virtqueues and all guest-memory writers inside the VMM, transporting
bounded disk-offset read/write/flush payloads to native Go disk owners over a
private Unix socket (`KBLK0001`). Guest FLUSH invokes the checkpoint-enabled
backend's durable data/metadata flush. The immutable guest runtime bundle is
an additional read-only virtio block device, not writable shared PMEM.

The source's normal execution/MUX drain, guest quiesce, VM pause, disk drain and
persistent checkpoint commit run before fork. Backends resume while vCPUs stay
paused so fork's flush/reconnect cannot deadlock on the backend gate. Native
checkpoint reconstruction creates independent durable child disks. The legacy
VMM then joins its host workers, checks the single-thread barrier, forks private
RAM, and rebuilds the runtimes. A failed handoff follows normal source recovery.

Each CLI child has a separate Go owner process. The source VMM supervisor sends
an unreaped child's pidfd using SCM_RIGHTS; the receiver verifies peer credentials
and descriptor type before acknowledgement. Teardown signals the pidfd, never an
unverified numeric PID. The child also gets its own API, serial, vsock CID/socket,
and log handles. Guest `fork_restore` acknowledgement and MUX reconstruction
precede readiness. The parent resumes and reconnects before `fork_result` success.

```sh
sandbox-ctl run --private-ram-fork --enable-checkpoint \
  --sandbox-id parent --config parent.yaml --run-root /run/native-private \
  --base-root /var/lib/native-private --ch-binary /path/to/native/cloud-hypervisor

sandbox-ctl fork --backend native-private --child-cid 41701 \
  --parent-id parent --sandbox-id child --config child.yaml \
  --run-root /run/native-private --base-root /var/lib/native-private \
  --ch-binary /path/to/native/cloud-hypervisor
```

`native-private` supports fixed-capacity guests with native checkpoint disks,
independent named/provider TAPs, independent cgroup ownership, and image-free
variable-capacity pool slots. Set `sandbox.private_ram_fork: true` in the isolated
conductor configuration to select it for normal HTTP/E2B running-parent forks.
The reserve pool is shared across compatible image shapes; it does not start a
language interpreter or allocate a separate pool per image. Claiming a slot
updates committed disk geometry through `vm.resize-disk` before guest mount.
Dynamic balloon guests preserve both the frozen target and actual balloon size.
Each child independently reserves capacity minus the smaller of those two values,
then resumes the normal memory controller after guest ACK and MUX readiness.
The frozen state is read only while the source lifecycle barrier is held; an
unpaused source, inconsistent capacity, or failed admission rejects the fork.
Image-free prepared slots still use the upstream fixed-budget admission policy.
The old website and its persisted state are not changed by this deployment.

Running private-RAM snapshots use a single-threaded CoW capture helper after the
normal guest/VM/device/disk barriers. The original VM retains its KVM objects and
resumes before the helper finishes persistence. The helper never creates a child
VM or opens mutable disks. Its frozen memory and pinned disk generations are
published through the normal native Snapshot S/Sandbox E local or encrypted,
chunked Manifest sinks. Explicit snapshot operations serialize memory; ordinary
running-parent fork does not generate a RAM image or copy the complete RAM.

Private Snapshot S restoration uses the same validated native artifact graph.
A private host pager serves logical pages from local or Manifest layers to the
VMM's UFFD handler; neither full RAM materialization nor shared writable guest
memory is required. Snapshot state determines device shape, fresh host paths
bind disks and networking, and the normal guest isolation ACK precedes Ready.
Forking a still-demand-paged restore resolves outstanding pages and joins its
fault handler before the host-fork barrier. This preparation is part of the
request, not hidden outside its timing. A restored instance may snapshot and
fork again. Pause/save, reconnect, repeated template creation after source
removal, and independent SQLite FULL writes/deletion are covered by real-guest
tests. A separate isolated nested-host hard-reset test covers local and encrypted
Manifest snapshots of both parent and child, including inherited process
continuation, committed SQLite state and independent deletion. It restores the
last completed snapshot; it does not preserve unsnapshotted RAM or claim
physical-device power-loss coverage.

The CLI's `complete_ready_ms` includes durable disk capture/reconstruction,
VMM fork, child ACK/MUX and parent reattachment; it excludes command startup.
The smoke client's request-to-complete measurement includes command startup,
and request-to-print additionally starts a new Python interpreter and waits for
its random marker. Disk branch preparation and durability are included.
VMM-internal timing alone must not be reported as sandbox latency.

Native-private child disk preparation now retains same-filesystem immutable
image/overlay carriers by hardlink. Each child owns its path and fresh writable
DIFF head; only the pinned writable checkpoint delta is exported. It never links
an active writable DIFF. The held carrier inode, size, modification time,
canonical identity, payload geometry and target codec policy must match.
Retaining an already admitted inode does not re-encode or rehash its whole body;
this is disk inheritance, not a new artifact ingestion/verification boundary.
New payloads still use normal publication. All branch files and directory entries
are synced before source pins are released. Cross-filesystem or incompatible
carriers retain the normal publication fallback. Maximum-depth pure-block graphs
also retain their existing flattening fallback; this change does not claim
constant-cost unbounded disk chains.

`private_native_smoke.py` verifies independent owner-process deletion, inherited
live Python processes, SQLite FULL writes, disk isolation, a second generation
and both teardown orders, including removal of the stopped parent's entire
owned disk directory before reading and forking its surviving child. `CH_FORK_ACCOUNT_PSS=1 CH_FORK_VERIFY_PAGES=1` adds
quiescent RAM PSS and physical-page comparisons; these diagnostics are separate
from ordinary end-to-end timing. Neither test claims host power-loss recovery.

## CLI

Run a source with the usual `sandbox-ctl run`. Start the child in its own
foreground owner process:

```sh
sandbox-ctl fork --parent-id parent --sandbox-id child \
  --config child.yaml --run-root /run/native-private \
  --base-root /var/lib/native-private --output /data/native-capture \
  --ch-binary /path/to/cloud-hypervisor
```

For chunked storage replace `--output` with `--upload` and supply
`--manifest-config /path/to/manifest.yaml` to both owners. Child configuration
provides runtime, capacity and host network identity, but cannot substitute the
captured disk graph or writable files. Use distinct sandbox IDs and fresh child
run/base paths. The caller owns admission, capture retention and the storage
client lifetime. Cancellation after dispatch can leave a committed capture;
inspect the source and output before retrying.

The stderr `fork_result` JSON reports `backend`, committed refs, `capture_ms`
(request through capture publication and parent reattachment) and
`restore_ready_ms` (child construction through ACK/MUX). Their sum excludes CLI
argument/configuration processing. Neither is a standalone parent pause metric.

## Network boundary

The migration supports no NIC or one virtio-net NIC, using a named TAP or the
native connector TapFD provider. A child needs its own exclusive TAP lease and
MAC. The host automatically gates the TAP, reasserts the gate after CH opens it,
and releases it only after fork ACK and MUX readiness. Failed restores never
release the gate. Namespace-specific control sockets verify interface identity
before every transition. The provider remains responsible for exclusive leasing.

CH patch 0015 treats a transmit EIO as a dropped frame only when an ioctl confirms
that the TAP is administratively down. Other errors remain fatal. The virtio-net
seccomp policy permits only the specific link-state query and its control socket;
seccomp is not disabled. This prevents intentional gating from killing a restored
net worker. Restore also binds the actual saved net device ID and descriptor count
rather than assuming `_net0`.

Only the child's private CH config/state is rewritten (TAP and MAC). The explicit
`fork_restore` / `fork_restore_ack` guest handshake updates link MAC, addresses
and hostname before workload thaw. An older guest cannot silently acknowledge
this operation as a normal restore. Errors fail before thaw; no Ready is emitted.
The fork handshake runs for guests with or without a NIC. It injects 32 fresh
host-random bytes into the guest kernel entropy pool and forces a CRNG reseed
before thaw. It does not modify application-owned PRNGs in inherited memory.

Inherited external TCP sockets are destroyed through SOCK_DIAG in the child
only, while its TAP is DOWN. The application observes a connection error and
must reconnect; the parent's original socket is unaffected. A post-destruction
dump verifies cleanup before identity release. Unsupported kernels, interrupted
dumps or failed cleanup do not publish Ready. The guest kernel includes
INET_DIAG, INET_TCP_DIAG and INET_DIAG_DESTROY for this operation.

Loopback TCP connections, loopback listeners and guest-private UDP remain
intact. Wildcard listeners and unconnected UDP retain their ports. Explicit local
bindings are allowed only when the restored guest keeps that address; bindings
to a changed address fail before Ready. Connected UDP can retain its peer when
the guest-local binding is preserved: the independent host network lease supplies
a distinct external source identity. Real-guest tests exercise parent, child and
second-generation traffic. The host owns the gate through failure too.

## Verification

See the umbrella repository's `test/native-fork/` harness and pinned source lock.
Tests exercise original local/remote snapshot restore before testing the new fork
path, live process state, writable file isolation, distinct identities and both
destruction orders. No existing service or user sandbox is adopted by the harness.

## Native HTTP and E2B integration

The matching orchestrator adds authenticated `POST /sandboxes/{id}/fork`.
Its body accepts `timeout` and optional `count` in 1..20 (default 1). The parent
is captured once per request, then independent children enter normal admission. The 201 response is the E2B SDK result array:
`[{"sandbox": {"sandboxID": "...", ...}}]`. The ordinary asynchronous Create
owner remains authoritative for child Ready/error and cleanup; the batch endpoint
waits for those owners and reports each child separately. SDK command requests
use the existing Proxy route readiness barrier. The source must be a
running sandbox owned by the authenticated tenant; cluster endpoints do not
silently emulate node-local fork.

The source lifecycle/proxy barrier covers capture, parent resume and publication.
Capture uses the native S/E path; publication happens after parent reattachment.
A tenant helper resolves the complete input graph with the native task-artifact
resolver, then publishes to the configured named file location or Manifest store.
Cold launch/init/file mutations and parent credentials are not replayed. The child
uses normal network allocation, resource admission, runner ownership and E2B envd
initialization with fresh service tokens. Every fresh sandbox restored from a memory template requests fork identity,
even if the client omits metadata. Later resume of that same sandbox does not
repeat it.

Successful publication reclaims its temporary capture directory. Failed
publications retain their committed inputs for diagnosis/recovery. Published
immutable graphs follow native artifact retention; deleting a sandbox must not
remove data still referenced by descendants. `/v2/sandboxes/{id}/connect` aliases
the native connect contract, including explicit memory-restore selection.

The `fork phase` log separates capture plus parent reattachment from publication
after resume. Neither field is labeled as pure guest pause. CLI/API smoke timings
are correctness diagnostics, not a comparison with the previous fast backend.

## Image-independent native pool

`runtime.NewPool` prepares `RuntimeReady` VMs without workload images or tenant
credentials. Set the overlay root's `LateBindBase=true` and `BaseCapacity=0`;
its actual read-only geometry is bound once at `Runtime.Launch`. The writable
upper, RAM, vCPU count and device layout remain pool shape parameters. The guest
kernel's empty-media revalidation and the VMM's capacity-notification channel
must both be available before an image can be assigned. There is no padding to a
Python-specific disk size and no language-specific warming.

`Acquire` transfers ownership and opens a refill vacancy immediately. Reserve
size excludes acquired/running VMs. Closing the pool joins preparations and
closes only unclaimed VMs; acquired runtimes keep running until their owners
close them. A failed preparation has bounded retry backoff, while successful
claims have no cooldown. `Observe` is a diagnostics callback: it must be quick
and must not call pool lifecycle methods from a preparation worker.

The runtime SDK pool and the native conductor HTTP Create path both support
image-independent prepared VMs. Configure a runner pool with `prepared_vm: true`
and retain an ordinary runner pool for memory restores and incompatible requests.
Preparation uses native StartRuntime, without a tenant image, credentials or
language interpreter. Acquired workloads support the migrated running fork.

## Persistent storage boundary

This migration strengthens the upstream logical-capture contract. Local
FileSink/BundleSink data is fsynced before publication; destination directories
and alias ancestors are synced before successful completion. The fork publisher
also syncs resolved local input/output namespaces before emitting its success
record. Manifest storage retains the configured store's commit contract (the
isolated filesystem-store tests use its file/directory sync implementation).

Native runtime checkpoint disks explicitly select `WithDurableFlush`: guest
FLUSH waits for accepted writeback and backing-file sync, and named checkpoint
creation syncs the version registration. Library callers without that option
retain the upstream ephemeral-disk policy. No successful persistence response is
emitted after a sync error. These barriers do not turn RAM into persistent state;
restoring process state still requires a successfully published snapshot.

The smoke tests include SQLite WAL transactions with synchronous=FULL, divergent
parent/child commits, reuse after source deletion and second-generation restore.
These are runtime/restore checks, not a claim of completed host power-cut testing.

### Prepared runner and HTTP admission

`RunPrepared(ctx, vm, opts)` consumes an image-independent `runtime_ready` VM
through the existing `Run` source/default/checkpoint path. It does not rename a
running VM or change its host identity. CPU/memory, cgroup, network lease,
checkpoint policy and writable device geometry must match. Only a root created
with `LateBindBase` accepts a different immutable image geometry. The adapter
owns cleanup on every exit, including failed validation. Readiness observers
installed at assignment receive previously observed control/runtime milestones;
workload Ready is emitted only after the normal launch barrier.

The private `sandbox-ctl prepare-run --spec <RuntimeSpec.json> --launch-fd N
--prepared-fd M` transport boots that base VM, writes `runtime_ready` to M, then
accepts one bounded JSON `{ "args": [...], "env": [...] }` document followed by
EOF on N. These are inherited private pipes, not a public VM-claim socket. The
assignment uses the ordinary `run` parser, source binding and OCI defaults;
`--restore` is intentionally rejected for an already booted VM. No image or
tenant environment is needed during preparation. The controller must preserve
exact run identity, resource admission and network-lease ownership when adopting
this adapter. Normal conductor Create now does so through a durable prepared
network lease and an atomic transfer to the sandbox record, before assignment
becomes visible to the runner. The selected runner ID becomes the sandbox ID.
Preparation is authenticated through the private configuration socket and exact
runner PID. Retirement stops that runner before releasing its network lease.

Prepared pools currently require static resource policy. Default-network E2B
image and exported Sandbox templates are eligible; memory snapshot restoration,
explicit identities/network overrides and workspace hooks use ordinary runners.
An incompatible immutable VM shape closes the unused prepared VM before normal
cold launch, preserving requested resources instead of silently ignoring them.
Kernel/runtime identity failures remain fatal. Writable templates must be local
raw files or mkfs references. Reserve size does not cap running sandbox count;
normal deployment capacity remains separately enforced.

`TestKVMPreparedRunAdapter` and `TestKVMPreparedRunnerTransport` boot a real VM,
select a different image afterward, execute BusyBox and verify owned shutdown.
`TestKVMGenericPoolTwoImages` additionally covers reserve replacement, running
fork, parent/child disk isolation and deletion. These are functional checks,
not startup performance benchmarks.
